import { query } from '../db/runtime.js';
import type { PathObservation } from '../path-beta/observation.js';
import { createMqttRuntime } from '../mqtt/client.js';
import { createRamWriterRuntime } from '../ram/writerRuntime.js';
import { createIngestEvents } from './events.js';
import { createCommittedFollowUps } from './committedFollowUps.js';

export function createIngestRuntime({ observePath }: { observePath?: (observation: PathObservation) => void } = {}) {
  const events = createIngestEvents();
  const committedFollowUps = createCommittedFollowUps({ query, observePath, emitNodeUpsert: events.emitNodeUpsert });
  const mqtt = createMqttRuntime({ events, committedFollowUps });
  const writer = createRamWriterRuntime({
    markMqttPersistenceHealthy: mqtt.markMqttPersistenceHealthy,
    markMqttPersistenceFailed: mqtt.markMqttPersistenceFailed,
    schedulePostPersistFollowUps: committedFollowUps.schedule,
  });
  return { events, committedFollowUps, mqtt, writer };
}
export type IngestRuntime = ReturnType<typeof createIngestRuntime>;
