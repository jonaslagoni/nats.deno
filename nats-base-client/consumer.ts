import {
  Consumer,
  ConsumerInfo,
  JetStreamReader,
  PullOptions,
} from "./types.ts";
import { JsMsg } from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/types.ts";
import { checkJsError, nanos } from "./jsutil.ts";
import { toJsMsg } from "./jsmsg.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { consumerOpts } from "./mod.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";

export class ConsumerImpl implements Consumer {
  js: ConsumerAPIImpl;
  ci: ConsumerInfo;

  constructor(js: ConsumerAPIImpl, info: ConsumerInfo) {
    this.js = js;
    this.ci = info;
  }

  async next(opts: Partial<{ expires: number }> = {}): Promise<JsMsg> {
    if (typeof this.ci.config.deliver_subject === "string") {
      return Promise.reject(
        new Error("consumer configuration is not a pull consumer"),
      );
    }
    let timeout = this.js.timeout;
    let expires = opts.expires ? opts.expires : 0;
    if (expires > timeout) {
      timeout = expires;
    }

    expires = expires < 0 ? 0 : nanos(expires);
    const pullOpts: PullOptions = {
      batch: 1,
      no_wait: expires === 0,
      expires,
    };

    const msg = await this.js.nc.request(
      `${this.js.prefix}.CONSUMER.MSG.NEXT.${this.ci.stream_name}.${this.ci.name}`,
      this.js.jc.encode(pullOpts),
      { noMux: true, timeout },
    );
    const err = checkJsError(msg);
    if (err) {
      throw (err);
    }
    return toJsMsg(msg);
  }

  info(): Promise<ConsumerInfo> {
    return this.js.info(this.ci.stream_name, this.ci.name);
  }

  async read(
    _opts: Partial<
      {
        inflight_limit: Partial<{
          bytes: number;
          messages: number;
        }>;
        callback: (m: JsMsg) => void;
      }
    > = {},
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    const qi = new QueuedIteratorImpl<JsMsg>();
    try {
      if (typeof this.ci.config.deliver_subject === "string") {
        await this.setupPush(qi);
      } else {
        this.setupPull(qi);
      }
    } catch (err) {
      qi.stop(err);
    }
    return qi;
  }

  setupPull(qi: QueuedIteratorImpl<JsMsg>) {
    const js = this.js.nc.jetstream(this.js.opts);
    let done = false;
    qi.iterClosed.then(() => {
      done = true;
    });
    (async () => {
      while (!done) {
        const iter = js.fetch(this.ci.stream_name, this.ci.name, {
          batch: 10,
          expires: 250,
        });
        for await (const m of iter) {
          qi.push(m);
        }
      }
    })();
  }

  async setupPush(
    qi: QueuedIteratorImpl<JsMsg>,
  ) {
    const js = this.js.nc.jetstream(this.js.opts);
    const co = consumerOpts();
    co.bind(this.ci.stream_name, this.ci.name);
    co.manualAck();

    co.callback((err, m) => {
      if (err) {
        //@ts-ignore: stop
        qi.push(() => {
          qi.stop(err);
        });
      }
      if (m) {
        qi.push(m);
      }
    });
    const sub = await js.subscribe(">", co);
    sub.closed.then(() => {
      //@ts-ignore: stop
      qi.push(() => {
        qi.stop();
      });
    });
    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });
  }
}
