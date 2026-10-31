import { boundedIntegerSetting } from '../platform/config/boundedNumber.js';

/** Persistent shared sessions need a proven broker restart/resize recovery path. */
export function assertSupportedIngestConnections(connections: number): void {
  if (connections !== 1) {
    throw new Error(`MQTT_INGEST_CONNECTIONS_UNSUPPORTED:${connections}; use 1 until shared-session recovery is qualified`);
  }
}

/** Bound all admitted work, including jobs waiting behind another topic job. */
export function loadMqttIntakeConfig(env: NodeJS.ProcessEnv) {
  return {
    maxItems: boundedIntegerSetting('MQTT_INGEST_QUEUE_MAX', env['MQTT_INGEST_QUEUE_MAX'], 1_000, 1, 100_000),
  };
}
