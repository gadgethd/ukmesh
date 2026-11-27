import { randomUUID } from 'node:crypto';
import { open } from 'node:fs/promises';
import { constants } from 'node:fs';
import { connect, type MqttClient } from 'mqtt';

const MAX_PASSWORD_BYTES = 1024;

export function reconnectMqttTransport(client: Pick<MqttClient, 'stream' | 'reconnect' | 'incomingStore' | 'outgoingStore'>): void {
  if (!client.stream.destroyed) {
    client.stream.destroy();
    return;
  }
  // Authentication rejection can suppress automatic reconnect while the
  // broker keeps the durable session. Wake the client with its current stores.
  client.reconnect({ incomingStore: client.incomingStore, outgoingStore: client.outgoingStore });
}

export async function readMqttPassword(file: string): Promise<string | null> {
  let handle;
  try { handle = await open(file, constants.O_RDONLY | constants.O_NONBLOCK); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw error;
  }
  try {
    const info = await handle.stat();
    if (!info.isFile() || info.size > MAX_PASSWORD_BYTES + 2) throw new Error('INVALID_MQTT_PASSWORD_FILE');
    const buffer = Buffer.alloc(MAX_PASSWORD_BYTES + 3);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const password = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true })
      .decode(buffer.subarray(0, bytesRead)).replace(/\r?\n$/, '');
    if (!password || /[\r\n\0]/.test(password) || Buffer.byteLength(password) > MAX_PASSWORD_BYTES) {
      throw new Error('INVALID_MQTT_PASSWORD_FILE');
    }
    return password;
  } finally { await handle.close(); }
}

/** Use a different, clean client so validation cannot take over the durable session. */
export async function verifyMqttPassword(
  endpoint: string, username: string, password: string, probeTopic: string, signal: AbortSignal,
): Promise<void> {
  const url = new URL(endpoint);
  if (url.username || url.password || url.searchParams.has('clientId')) throw new Error('MQTT_PASSWORD_RELOAD_ENDPOINT_UNSUPPORTED');
  const budget = AbortSignal.any([signal, AbortSignal.timeout(6_000)]);
  budget.throwIfAborted();
  const probe = connect(endpoint, {
    clientId: `ukm-check-${randomUUID().slice(0, 12)}`, clean: true, username, password,
    reconnectPeriod: 0, connectTimeout: 5_000,
  });
  const nonce = randomUUID();
  try {
    await new Promise<void>((resolve, reject) => {
      const failed = () => reject(new Error('MQTT_PASSWORD_VALIDATION_FAILED'));
      budget.addEventListener('abort', failed, { once: true });
      probe.on('error', failed);
      probe.once('close', failed);
      probe.on('message', (topic, payload, packet) => {
        if (topic === probeTopic && packet.qos === 1 && !packet.retain && payload.toString() === nonce) resolve();
      });
      probe.once('connect', () => {
        probe.subscribe(probeTopic, { qos: 1 }, (error, grants) => {
          if (error || grants?.[0]?.qos !== 1) { failed(); return; }
          probe.publish(probeTopic, nonce, { qos: 1, retain: false }, error => { if (error) failed(); });
        });
      });
    });
  } finally { await probe.endAsync(true); }
}

export function pollMqttPassword(input: {
  signal: AbortSignal;
  read: () => Promise<string | null>;
  current: () => Buffer | string | undefined;
  verify: (password: string) => Promise<void>;
  rotate: (password: string) => Promise<void>;
  rejected: (error: unknown) => void;
  intervalMs?: number;
}): () => Promise<void> {
  const interval = input.intervalMs ?? 10_000;
  let timer: NodeJS.Timeout | undefined;
  let running: Promise<void> | undefined;
  let failed = false;
  function schedule(delay: number) {
    if (input.signal.aborted) return;
    timer = setTimeout(() => { running = check(); }, delay);
    timer.unref();
  }
  async function check() {
    try {
      const candidate = await input.read();
      input.signal.throwIfAborted();
      if (candidate !== null && candidate !== input.current()?.toString()) {
        await input.verify(candidate);
        input.signal.throwIfAborted();
        await input.rotate(candidate);
      }
      failed = false;
    } catch (error) {
      if (!input.signal.aborted && !failed) input.rejected(error);
      failed = true;
    } finally { schedule(failed ? Math.max(interval, 30_000) : interval); }
  }
  schedule(interval);
  return async () => { clearTimeout(timer); await running; };
}
