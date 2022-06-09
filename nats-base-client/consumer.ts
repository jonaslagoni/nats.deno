import {
  Consumer,
  ConsumerInfo,
  ExportedConsumer,
  JetStreamReader,
  NatsConnection,
  PullOptions,
} from "./types.ts";
import { JsMsg } from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/types.ts";
import { checkJsError, isHeartbeatMsg, nanos } from "./jsutil.ts";
import { toJsMsg } from "./jsmsg.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";
import { consumerOpts, createInbox, ErrorCode, JSONCodec } from "./mod.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";

const jc = JSONCodec();

export class ExportedConsumerImpl implements ExportedConsumer {
  nc: NatsConnection;
  subject: string;

  constructor(nc: NatsConnection, subject: string) {
    this.nc = nc;
    this.subject = subject;
  }
  async next(opts: Partial<{ expires: number }> = {}): Promise<JsMsg> {
    const to = opts.expires ?? 0;
    const expires = nanos(to);
    const timeout = to > 2000 ? to + 2000 : 2000;

    const pullOpts: PullOptions = {
      batch: 1,
      no_wait: expires === 0,
      expires,
    };

    const r = await this.nc.request(this.subject, jc.encode(pullOpts), {
      noMux: true,
      timeout,
    });

    const err = checkJsError(r);
    if (err) {
      throw err;
    }

    return toJsMsg(r);
  }

  read(
    _opts?: Partial<
      {
        inflight_limit: Partial<{ bytes: number; messages: number }>;
        callback: (m: JsMsg) => void;
      }
    >,
  ): Promise<QueuedIterator<JsMsg> | JetStreamReader> {
    const qi = new QueuedIteratorImpl<JsMsg>();
    const inbox = createInbox();
    const to = 5000;
    // FIXME: there's some issue, the server is not sending
    //   more than one message with greater batches
    const batch = 1;
    const payload = jc.encode({
      expires: nanos(to),
      batch,
    });

    let expected = 0;
    let last = Date.now();

    // this timer will check if last message or error was
    // received within the guard timer - if not, then
    // this means that we have a pull that expired, but we
    // got no notification or messages
    const guard = () => {
      if (Date.now() - last > to) {
        expected = 0;
        fn();
      }
    };

    let timer = 0;

    // clean the timer up when the iterator closes
    const cleanup = () => {
      if(timer > 0) {
        clearTimeout(timer);
      }
    };

    qi.iterClosed
      .then(() => {
        cleanup();
      })
      .catch(() => {
        cleanup();
      });

    // this is the pull fn - we initialize last, make a request
    // and initialize the guard to do a check slightly after
    // the pull is supposed to expire
    const fn = () => {
      if(timer > 0) {
        clearTimeout(timer)
      }
      last = Date.now();
      this.nc.publish(this.subject, payload, { reply: inbox });
      expected = batch;

      timer = setTimeout(guard, to + 1000);
    };

    try {
      const sub = this.nc.subscribe(inbox, {
        callback: (err, msg) => {
          last = Date.now();
          if (err) {
            qi.stop(err);
          }
          err = checkJsError(msg);
          if (err) {
            switch (err.code) {
              case ErrorCode.JetStream404NoMessages:
              case ErrorCode.JetStream408RequestTimeout:
                // poll again
                fn();
                return;
              default:
                qi.stop(err);
                sub.unsubscribe();
                return;
            }
          }
          if (isHeartbeatMsg(msg)) {
            return;
          }

          qi.push(toJsMsg(msg));
          // if we are here, we have a data message
          --expected;
          if (expected <= 0) {
            // we got what we asked, so ask for more
            fn();
          }
        },
      });
    } catch (err) {
      qi.stop(err);
    }
    fn();
    return Promise.resolve(qi);
  }
}

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
