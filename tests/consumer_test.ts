import {
  cleanup,
  initStream,
  jetstreamServerConf,
  setup,
} from "./jstest_util.ts";
import { assertEquals } from "https://deno.land/std@0.136.0/testing/asserts.ts";
import { AckPolicy, DeliverPolicy } from "../nats-base-client/mod.ts";
import { assert } from "../nats-base-client/denobuffer.ts";
import { assertRejects } from "https://deno.land/std@0.125.0/testing/asserts.ts";

Deno.test("consumer - create", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await assertRejects(
    async () => {
      await jsm.consumers.get(stream, "me");
    },
    Error,
    "consumer not found",
  );

  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, "me");
  assert(consumer);

  await cleanup(ns, nc);
});

Deno.test("consumer - rejects push consumer", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream } = await initStream(nc);

  const jsm = await nc.jetstreamManager();

  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
    deliver_subject: "foo",
  });

  await assertRejects(
    async () => {
      await jsm.consumers.get(stream, "me");
    },
    Error,
    "consumer configuration is not a pull consumer",
  );

  await cleanup(ns, nc);
});

Deno.test("consumer - next", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, "me");

  await assertRejects(
    async () => {
      await consumer!.next();
    },
    Error,
    "no messages",
  );

  const js = nc.jetstream();
  await js.publish(subj);

  const m = await consumer.next();
  assertEquals(m.subject, subj);
  assertEquals(m.seq, 1);

  await cleanup(ns, nc);
});

Deno.test("consumer - info durable", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  await jsm.consumers.add(stream, {
    durable_name: "me",
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, "me");
  const info = await consumer.info();
  assertEquals(info.name, "me");
  assertEquals(info.stream_name, stream);

  await cleanup(ns, nc);
});

Deno.test("consumer - info ephemeral", async () => {
  const { ns, nc } = await setup(jetstreamServerConf({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await nc.jetstreamManager();
  const ci = await jsm.consumers.add(stream, {
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.All,
  });

  const consumer = await jsm.consumers.get(stream, ci.name);
  const info = await consumer.info();
  assertEquals(info.name, ci.name);
  assertEquals(info.stream_name, stream);

  await cleanup(ns, nc);
});
