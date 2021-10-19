/*
 * Copyright 2021 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  AckPolicy,
  ConsumerConfig,
  ConsumerInfo,
  DeliverPolicy,
  DiscardPolicy,
  Empty,
  JetStreamClient,
  JetStreamManager,
  JetStreamPublishOptions,
  JsHeaders,
  JsMsg,
  KV,
  KvCodec,
  KvCodecs,
  KvEntry,
  KvOptions,
  KvPutOptions,
  KvRemove,
  KvStatus,
  NatsConnection,
  PurgeOpts,
  PurgeResponse,
  RetentionPolicy,
  StorageType,
  StoredMsg,
  StreamConfig,
} from "./types.ts";
import { JetStreamClientImpl } from "./jsclient.ts";
import { millis, nanos } from "./jsutil.ts";
import { QueuedIterator, QueuedIteratorImpl } from "./queued_iterator.ts";
import { deferred } from "./util.ts";
import { headers } from "./headers.ts";
import { createInbox } from "./protocol.ts";
import { consumerOpts } from "./mod.ts";

export function Base64KeyCodec(): KvCodec<string> {
  return {
    encode(key: string): string {
      return btoa(key);
    },
    decode(bkey: string): string {
      return atob(bkey);
    },
  };
}

export function NoopKvCodecs(): KvCodecs {
  return {
    key: {
      encode(k: string): string {
        return k;
      },
      decode(k: string): string {
        return k;
      },
    },
    value: {
      encode(v: Uint8Array): Uint8Array {
        return v;
      },
      decode(v: Uint8Array): Uint8Array {
        return v;
      },
    },
  };
}

export function defaultBucketOpts(): Partial<KvOptions> {
  return {
    replicas: 1,
    history: 1,
    timeout: 2000,
    maxBucketSize: -1,
    maxValueSize: -1,
    codec: NoopKvCodecs(),
  };
}

type OperationType = "PUT" | "DEL" | "PURGE";

export const kvOperationHdr = "KV-Operation";
const kvPrefix = "KV_";
const kvSubjectPrefix = "$KV";

const validKeyRe = /^[-/=.\w]+$/;
const validSearchKey = /^[-/=.>*\w]+$/;
const validBucketRe = /^[-\w]+$/;

// this exported for tests
export function validateKey(k: string) {
  if (k.startsWith(".") || k.endsWith(".") || !validKeyRe.test(k)) {
    throw new Error(`invalid key: ${k}`);
  }
}

export function validateSearchKey(k: string) {
  if (k.startsWith(".") || k.endsWith(".") || !validSearchKey.test(k)) {
    throw new Error(`invalid key: ${k}`);
  }
}

export function hasWildcards(k: string) {
  if (k.startsWith(".") || k.endsWith(".")) {
    throw new Error(`invalid key: ${k}`);
  }
  const chunks = k.split(".");

  let hasWildcards = false;
  for (let i = 0; i < chunks.length; i++) {
    switch (chunks[i]) {
      case "*":
        hasWildcards = true;
        break;
      case ">":
        if (i !== chunks.length - 1) {
          throw new Error(`invalid key: ${k}`);
        }
        hasWildcards = true;
        break;
      default:
        // continue
    }
  }
  return hasWildcards;
}

// this exported for tests
export function validateBucket(name: string) {
  if (!validBucketRe.test(name)) {
    throw new Error(`invalid bucket name: ${name}`);
  }
}

export class Bucket implements KV, KvRemove {
  jsm: JetStreamManager;
  js: JetStreamClient;
  stream!: string;
  bucket: string;
  codec!: KvCodecs;
  _prefixLen: number;

  constructor(bucket: string, jsm: JetStreamManager, js: JetStreamClient) {
    validateBucket(bucket);
    this.jsm = jsm;
    this.js = js;
    this.bucket = bucket;
    this._prefixLen = 0;
  }

  static async create(
    nc: NatsConnection,
    name: string,
    opts: Partial<KvOptions> = {},
  ): Promise<KV> {
    validateBucket(name);
    const to = opts.timeout || 2000;
    const jsm = await nc.jetstreamManager({ timeout: to });
    const bucket = new Bucket(name, jsm, nc.jetstream({ timeout: to }));
    await bucket.init(opts);
    return bucket;
  }

  async init(opts: Partial<KvOptions> = {}): Promise<void> {
    const bo = Object.assign(defaultBucketOpts(), opts) as KvOptions;
    this.codec = bo.codec;
    const sc = {} as StreamConfig;
    this.stream = sc.name = opts.streamName ?? this.bucketName();
    sc.subjects = [this.subjectForBucket()];
    sc.retention = RetentionPolicy.Limits;
    sc.max_msgs_per_subject = bo.history;
    sc.max_bytes = bo.maxBucketSize;
    sc.max_msg_size = bo.maxValueSize;
    sc.storage = StorageType.File;
    sc.discard = DiscardPolicy.Old;
    sc.num_replicas = bo.replicas;
    if (bo.ttl) {
      sc.max_age = nanos(bo.ttl);
    }
    sc.allow_rollup_hdrs = true;

    try {
      await this.jsm.streams.info(sc.name);
    } catch (err) {
      if (err.message === "stream not found") {
        await this.jsm.streams.add(sc);
      }
    }
  }

  bucketName(): string {
    return this.stream ?? `${kvPrefix}${this.bucket}`;
  }

  subjectForBucket(): string {
    return `${kvSubjectPrefix}.${this.bucket}.>`;
  }

  subjectForKey(k: string): string {
    return `${kvSubjectPrefix}.${this.bucket}.${k}`;
  }

  get prefixLen(): number {
    if (this._prefixLen === 0) {
      this._prefixLen = `${kvSubjectPrefix}.${this.bucket}.`.length;
    }
    return this._prefixLen;
  }

  encodeKey(key: string): string {
    const chunks: string[] = [];
    for (const t of key.split(".")) {
      switch (t) {
        case ">":
        case "*":
          chunks.push(t);
          break;
        default:
          chunks.push(this.codec.key.encode(t));
          break;
      }
    }
    return chunks.join(".");
  }

  decodeKey(ekey: string): string {
    const chunks: string[] = [];
    for (const t of ekey.split(".")) {
      switch (t) {
        case ">":
        case "*":
          chunks.push(t);
          break;
        default:
          chunks.push(this.codec.key.decode(t));
          break;
      }
    }
    return chunks.join(".");
  }

  validateKey = validateKey;

  validateSearchKey = validateSearchKey;

  hasWildcards = hasWildcards;

  close(): Promise<void> {
    return Promise.resolve();
  }

  smToEntry(key: string, sm: StoredMsg): KvEntry {
    return {
      bucket: this.bucket,
      key: key,
      value: sm.data,
      delta: 0,
      created: sm.time,
      revision: sm.seq,
      operation: sm.header.get(kvOperationHdr) as OperationType || "PUT",
    };
  }

  jmToEntry(k: string, jm: JsMsg): KvEntry {
    const key = this.decodeKey(jm.subject.substring(this.prefixLen));
    const e = {
      bucket: this.bucket,
      key: key,
      value: jm.data,
      created: new Date(millis(jm.info.timestampNanos)),
      revision: jm.seq,
      operation: jm.headers?.get(kvOperationHdr) as OperationType || "PUT",
    } as KvEntry;

    if (k !== ">") {
      e.delta = jm.info.pending;
    }
    return e;
  }

  create(k: string, data: Uint8Array): Promise<number> {
    return this.put(k, data, { previousSeq: 0 });
  }

  update(k: string, data: Uint8Array, version: number): Promise<number> {
    if (version <= 0) {
      throw new Error("version must be greater than 0");
    }
    return this.put(k, data, { previousSeq: version });
  }

  async put(
    k: string,
    data: Uint8Array,
    opts: Partial<KvPutOptions> = {},
  ): Promise<number> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);

    const o = {} as JetStreamPublishOptions;
    if (opts.previousSeq !== undefined) {
      const h = headers();
      o.headers = h;
      h.set("Nats-Expected-Last-Subject-Sequence", `${opts.previousSeq}`);
    }
    const pa = await this.js.publish(this.subjectForKey(ek), data, o);
    return pa.seq;
  }

  async get(k: string): Promise<KvEntry | null> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);
    try {
      const sm = await this.jsm.streams.getMessage(this.bucketName(), {
        last_by_subj: this.subjectForKey(ek),
      });
      return this.smToEntry(k, sm);
    } catch (err) {
      if (err.message === "no message found") {
        return null;
      }
      throw err;
    }
  }

  purge(k: string): Promise<void> {
    return this._deleteOrPurge(k, "PURGE");
  }

  delete(k: string): Promise<void> {
    return this._deleteOrPurge(k, "DEL");
  }

  async _deleteOrPurge(k: string, op: "DEL" | "PURGE") {
    if (!this.hasWildcards(k)) {
      return this._doDeleteOrPurge(k, op);
    }
    const keys = await this.keys(k);
    if (keys.length === 0) {
      return;
    }
    const d = deferred<void>();
    const buf: Promise<void>[] = [];
    for (const k of keys) {
      buf.push(this._doDeleteOrPurge(k, op));
    }
    Promise.all(buf)
      .then(() => {
        d.resolve();
      })
      .catch((err) => {
        d.reject(err);
      });

    return d;
  }

  async _doDeleteOrPurge(k: string, op: "DEL" | "PURGE"): Promise<void> {
    const ek = this.encodeKey(k);
    this.validateKey(ek);
    const h = headers();
    h.set(kvOperationHdr, op);
    if (op === "PURGE") {
      h.set(JsHeaders.RollupHdr, JsHeaders.RollupValueSubject);
    }
    await this.js.publish(this.subjectForKey(ek), Empty, { headers: h });
  }

  _buildCC(
    k: string,
    history = false,
    opts: Partial<ConsumerConfig> = {},
  ): Partial<ConsumerConfig> {
    const ek = this.encodeKey(k);
    this.validateSearchKey(k);

    return Object.assign({
      "deliver_policy": history
        ? DeliverPolicy.All
        : DeliverPolicy.LastPerSubject,
      "ack_policy": AckPolicy.None,
      "filter_subject": this.subjectForKey(ek),
      "flow_control": true,
      "idle_heartbeat": nanos(5 * 1000),
    }, opts) as Partial<ConsumerConfig>;
  }

  /**
   * @deprecated
   */
  consumerOn(k: string, history = false): Promise<ConsumerInfo> {
    return this.jsm.consumers.add(
      this.stream,
      this._buildCC(k, history, {
        ack_policy: AckPolicy.Explicit,
        deliver_subject: createInbox(),
      }),
    );
  }

  remove(k: string): Promise<void> {
    return this.purge(k);
  }

  async history(opts: { key?: string } = {}): Promise<QueuedIterator<KvEntry>> {
    const k = opts.key ?? ">";
    const qi = new QueuedIteratorImpl<KvEntry>();
    const done = deferred();
    const cc = this._buildCC(k, true);
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
    copts.orderedConsumer();
    copts.callback((err, jm) => {
      if (err) {
        // sub done
        qi.stop(err);
        return;
      }
      if (jm) {
        const e = this.jmToEntry(k, jm);
        qi.push(e);
        qi.received++;
        if (jm.info.pending === 0) {
          done.resolve();
        }
      }
    });

    const sub = await this.js.subscribe(subj, copts);
    done.then(() => {
      sub.unsubscribe();
    });
    done.catch((_err) => {
      sub.unsubscribe();
    });
    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });
    sub.closed.then(() => {
      qi.stop();
    }).catch((err) => {
      qi.stop(err);
    });

    this.jsm.streams.getMessage(this.stream, {
      "last_by_subj": subj,
    }).catch(() => {
      // we don't have a value for this
      done.resolve();
    });

    return qi;
  }

  async watch(opts: { key?: string } = {}): Promise<QueuedIterator<KvEntry>> {
    const k = opts.key ?? ">";
    const qi = new QueuedIteratorImpl<KvEntry>();
    const cc = this._buildCC(k, false);
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
    copts.orderedConsumer();
    copts.callback((err, jm) => {
      if (err) {
        // sub done
        qi.stop(err);
        return;
      }
      if (jm) {
        const e = this.jmToEntry(k, jm);
        qi.push(e);
        qi.received++;
      }
    });

    const sub = await this.js.subscribe(subj, copts);
    qi.iterClosed.then(() => {
      sub.unsubscribe();
    });
    sub.closed.then(() => {
      qi.stop();
    }).catch((err) => {
      qi.stop(err);
    });

    return qi;
  }

  async keys(k = ">"): Promise<string[]> {
    const d = deferred<string[]>();
    const keys: string[] = [];
    const cc = this._buildCC(k, false, { headers_only: true });
    const subj = cc.filter_subject!;
    const copts = consumerOpts(cc);
    copts.orderedConsumer();

    const sub = await this.js.subscribe(subj, copts);
    (async () => {
      for await (const jm of sub) {
        const op = jm.headers?.get(kvOperationHdr);
        if (op !== "DEL" && op !== "PURGE") {
          const key = this.decodeKey(jm.subject.substring(this.prefixLen));
          keys.push(key);
        }
        if (jm.info.pending === 0) {
          sub.unsubscribe();
        }
      }
    })()
      .then(() => {
        d.resolve(keys);
      })
      .catch((err) => {
        d.reject(err);
      });

    this.jsm.streams.getMessage(this.stream, {
      "last_by_subj": subj,
    }).catch(() => {
      // we don't have a value for this
      sub.unsubscribe();
    });

    return d;
  }

  purgeBucket(opts?: PurgeOpts): Promise<PurgeResponse> {
    return this.jsm.streams.purge(this.bucketName(), opts);
  }

  destroy(): Promise<boolean> {
    return this.jsm.streams.delete(this.bucketName());
  }

  async status(): Promise<KvStatus> {
    const ji = this.js as JetStreamClientImpl;
    const cluster = ji.nc.info?.cluster ?? "";
    const si = await this.jsm.streams.info(this.bucketName());
    return {
      bucket: this.bucketName(),
      values: si.state.messages,
      history: si.config.max_msgs_per_subject,
      ttl: si.config.max_age,
      bucket_location: cluster,
      backingStore: si.config.storage,
    } as KvStatus;
  }
}
