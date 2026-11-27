import type { MqttRuntimeStatus } from '../mqtt/client.js';

export type IngestHealth = {
  ready: boolean;
  state: MqttRuntimeStatus['state'] | 'disabled';
  deliveryStalled?: boolean;
};

export type ReadIngestHealth = () => Promise<IngestHealth>;

const states = new Set<IngestHealth['state']>(['starting', 'connected', 'disconnected', 'reconnecting', 'error', 'disabled']);

export function mqttIngestEnabled(): boolean {
  return !['0', 'false', 'no'].includes(String(process.env['MQTT_INGEST_ENABLED'] ?? 'true').trim().toLowerCase());
}

export function readRemoteIngestHealth(url: string, fetchStatus = fetch): ReadIngestHealth {
  return async () => {
    try {
      const response = await fetchStatus(url, { signal: AbortSignal.timeout(1_000), redirect: 'error' });
      const health = await response.json() as { checks?: { mqtt?: { state?: IngestHealth['state']; deliveryStalled?: boolean } } };
      const mqtt = health.checks?.mqtt;
      if (mqtt?.state && states.has(mqtt.state)) {
        return { ready: response.status === 200 && (mqtt.state === 'connected' || mqtt.state === 'disabled'),
          state: mqtt.state, deliveryStalled: mqtt.deliveryStalled === true };
      }
    } catch { /* The API remains available while the ingest process restarts. */ }
    return { ready: false, state: 'disconnected' };
  };
}
