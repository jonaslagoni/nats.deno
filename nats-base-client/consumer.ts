import { Consumer, ConsumerInfo, PullOptions } from "./types.ts";
import { JsMsg } from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/types.ts";
import { checkJsError, nanos } from "./jsutil.ts";
import { toJsMsg } from "./jsmsg.ts";
import { ConsumerAPIImpl } from "./jsmconsumer_api.ts";

export class ConsumerImpl implements Consumer {
  js: ConsumerAPIImpl;
  stream: string;
  name: string;

  constructor(js: ConsumerAPIImpl, config: { stream: string; name: string }) {
    this.js = js;
    this.stream = config.stream;
    this.name = config.name;
  }

  async next(opts: Partial<{ expires: number }> = {}): Promise<JsMsg> {
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
      `${this.js.prefix}.CONSUMER.MSG.NEXT.${this.stream}.${this.name}`,
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
    return this.js.info(this.stream, this.name);
  }
}
