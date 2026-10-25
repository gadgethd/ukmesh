import type { MqttClient } from 'mqtt';
import type { DurableMqttPacket } from './durableHandler.js';

type Packet = DurableMqttPacket & { qos?: number; messageId?: number };
type WirePacket = { cmd: string; messageId?: number };
type Done = (error?: Error) => void;
type Send = (packet: WirePacket, callback?: Done, ...args: unknown[]) => unknown;
type Job = { packet: Packet; stream: object; bytes: number; ack?: WirePacket; failed?: boolean };

/**
 * MQTT.js 5.15/5.16 serializes its parser on handleMessage's callback, including
 * PINGRESP. Release that callback immediately, but hold the actual QoS 1 PUBACK
 * at the wire boundary until persistence commits. This deliberately isolated
 * _sendPacket hook is covered by real MQTT wire tests when upgrading MQTT.js.
 * Never use a `message` listener as the durability boundary: it auto-acks.
 */
export function installDurableMqttIntake(
  client: MqttClient,
  persist: (packet: Packet) => Promise<void>,
  onFailure: (error: Error) => void,
  options: { maxItems?: number; maxBytes?: number; concurrency?: number; depth?: (count: number) => void } = {},
) {
  const wire = client as unknown as { _sendPacket: Send };
  const originalSend = wire._sendPacket.bind(client);
  const sessions = new WeakMap<object, Map<number, Job>>();
  const pending: Job[] = [];
  const topics = new Set<string>();
  let active = 0;
  let count = 0;
  let bytes = 0;
  let accepting = true;
  const waiters: Array<() => void> = [];
  const update = () => options.depth?.(count);
  const fail = (job: Job, cause: unknown) => {
    job.failed = true;
    // A previous session's late failure must not disconnect a healthy session.
    if (client.stream === job.stream) onFailure(cause instanceof Error ? cause : new Error(String(cause)));
  };
  wire._sendPacket = (packet, callback, ...args) => {
    if (packet.cmd !== 'puback') return originalSend(packet, callback, ...args);
    const job = sessions.get(client.stream)?.get(packet.messageId!);
    if (!job) {
      onFailure(new Error('MQTT_DURABLE_ACK_WITHOUT_CLAIM'));
      callback?.();
      return;
    }
    job.ack = packet;
    callback?.(); // advance protocol parsing; no acknowledgement is written
  };
  const pump = () => {
    while (active < (options.concurrency ?? 8)) {
      const index = pending.findIndex((job) => !topics.has(job.packet.topic));
      if (index < 0) break;
      const job = pending.splice(index, 1)[0]!;
      active += 1;
      topics.add(job.packet.topic);
      void Promise.resolve().then(() => persist(job.packet)).then(() => {
        if (job.packet.qos === 1 && !job.failed && client.connected && client.stream === job.stream) {
          if (!job.ack) throw new Error('MQTT_DURABLE_ACK_MISSING');
          originalSend(job.ack, (error) => { if (error) fail(job, error); });
        }
      }).catch((cause: unknown) => fail(job, cause)).finally(() => {
        if (job.packet.qos === 1) sessions.get(job.stream)?.delete(job.packet.messageId!);
        active -= 1;
        count -= 1;
        bytes -= job.bytes;
        topics.delete(job.packet.topic);
        update();
        pump();
        if (count === 0) for (const resolve of waiters.splice(0)) resolve();
      });
    }
  };
  client.handleMessage = (packet, callback) => {
    const incoming = packet as Packet;
    const stream = client.stream;
    const size = Buffer.byteLength(incoming.payload) + Buffer.byteLength(incoming.topic);
    const job: Job = { packet: incoming, stream, bytes: size };
    if (!accepting || (incoming.qos !== 0 && incoming.qos !== 1)) {
      fail(job, new Error('MQTT_DURABLE_INTAKE_CLOSED_OR_UNSUPPORTED_QOS'));
      callback(new Error('MQTT_DURABLE_INTAKE_CLOSED_OR_UNSUPPORTED_QOS'));
      return;
    }
    let session = sessions.get(stream);
    if (!session) { session = new Map(); sessions.set(stream, session); }
    const duplicate = incoming.qos === 1 ? session.get(incoming.messageId!) : undefined;
    if (duplicate) {
      if (duplicate.packet.topic !== incoming.topic || !Buffer.from(duplicate.packet.payload).equals(Buffer.from(incoming.payload))) {
        fail(job, new Error('MQTT_DUPLICATE_ID_CONFLICT'));
        callback(new Error('MQTT_DUPLICATE_ID_CONFLICT'));
      } else callback(); // attach retransmission's ACK to the same commit
      return;
    }
    if (count >= (options.maxItems ?? 1_000) || bytes + size > (options.maxBytes ?? 32 * 1_048_576)) {
      fail(job, new Error('MQTT_DURABLE_QUEUE_FULL'));
      callback(new Error('MQTT_DURABLE_QUEUE_FULL'));
      return;
    }
    if (incoming.qos === 1) session.set(incoming.messageId!, job);
    pending.push(job);
    count += 1;
    bytes += size;
    update();
    callback();
    pump();
  };
  return {
    stopAdmission: () => { accepting = false; },
    drain: () => count === 0 ? Promise.resolve() : new Promise<void>((resolve) => waiters.push(resolve)),
  };
}
