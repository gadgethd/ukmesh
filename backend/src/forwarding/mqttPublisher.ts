import mqtt, { type IClientOptions, type IClientPublishOptions, type MqttClient } from 'mqtt';
import { withDeadline } from '../platform/deadline.js';

export type MessagePublisher = {
  publish: (topic: string, payload: string) => Promise<void>;
  close: () => Promise<void>;
};

type Connection = { url: string; options: IClientOptions; expiresAt?: number };

/** Lazy connection mechanics; credentials, expiry and delivery QoS belong to the caller. */
export function createMqttPublisher(deps: {
  connection: () => Promise<Connection> | Connection;
  connectionLabel: string;
  publishLabel: string;
  publishOptions: IClientPublishOptions;
  connectTimeoutMs: number;
  publishTimeoutMs?: number;
  discardOnPublishFailure?: boolean;
  logError: (error: Error) => void;
  connect?: typeof mqtt.connect;
}): MessagePublisher {
  let client: MqttClient | null = null;
  let connecting: Promise<MqttClient> | null = null;
  let credentialsExpireAt = Infinity;
  let generation = 0;

  async function closeClient(): Promise<void> {
    generation++;
    const current = client;
    client = null;
    connecting = null;
    if (current) await current.endAsync(true).catch(() => undefined);
  }

  function connect(): Promise<MqttClient> {
    if (client?.connected && Date.now() / 1_000 < credentialsExpireAt - 60) return Promise.resolve(client);
    if (connecting) return connecting;
    const startedGeneration = generation;
    const attempt = (async () => {
      if (client) {
        const expired = client;
        client = null;
        await expired.endAsync(true).catch(() => undefined);
      }
      const connection = await deps.connection();
      if (generation !== startedGeneration) throw new Error(`${deps.connectionLabel} connection closed before connect`);
      credentialsExpireAt = connection.expiresAt ?? Infinity;
      const created = (deps.connect ?? mqtt.connect)(connection.url, connection.options);
      client = created;
      created.on('error', deps.logError);
      return new Promise<MqttClient>((resolve, reject) => {
        let settled = false;
        let timer: NodeJS.Timeout;
        const finish = (error?: Error) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          created.removeListener('connect', onConnect);
          created.removeListener('close', onClose);
          if (error) {
            if (client === created) client = null;
            created.end(true);
            reject(error);
          } else resolve(created);
        };
        const onConnect = () => finish();
        const onClose = () => finish(new Error(`${deps.connectionLabel} connection closed before connect`));
        timer = setTimeout(() => finish(new Error(`${deps.connectionLabel} connection timed out`)), deps.connectTimeoutMs);
        created.once('connect', onConnect);
        created.once('close', onClose);
      });
    })().finally(() => {
      if (connecting === attempt) connecting = null;
    });
    connecting = attempt;
    return attempt;
  }

  return {
    async publish(topic, payload) {
      const current = await connect();
      try {
        const published = current.publishAsync(topic, payload, deps.publishOptions);
        await (deps.publishTimeoutMs === undefined ? published
          : withDeadline(published, deps.publishTimeoutMs, deps.publishLabel));
      } catch (error) {
        // Unknown QoS-1 delivery state requires a fresh connection and credentials.
        if (deps.discardOnPublishFailure) await closeClient();
        throw error;
      }
      if (!current.connected) {
        await closeClient();
        throw new Error(`${deps.connectionLabel} connection closed after publish`);
      }
    },
    close: closeClient,
  };
}
