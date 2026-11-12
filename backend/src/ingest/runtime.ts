import { query } from '../db/runtime.js';
import { createResolvePool } from '../path-beta/resolvePool.js';
import { createSlowResolutionScheduler } from '../path-beta/slowMode.js';
import { createSlowResolutionRecorder } from '../pathing/slowResolutionRepository.js';
import { createMqttRuntime } from '../mqtt/client.js';
import { createRamWriterRuntime } from '../ram/writerRuntime.js';
import { createIngestEvents } from './events.js';
import { createCommittedFollowUps } from './committedFollowUps.js';

export function createIngestRuntime() {
  const resolvePool = createResolvePool();
  const events = createIngestEvents();
  const record = createSlowResolutionRecorder(query, () => slowMode.slowModeWindowMs());
  const slowMode = createSlowResolutionScheduler({ pool: resolvePool, record });
  const committedFollowUps = createCommittedFollowUps({ query, resolvePool,
    scheduleSlowResolution: slowMode.scheduleSlowResolution, emitNodeUpsert: events.emitNodeUpsert });
  const mqtt = createMqttRuntime({ events, committedFollowUps });
  const writer = createRamWriterRuntime({
    markMqttPersistenceHealthy: mqtt.markMqttPersistenceHealthy,
    markMqttPersistenceFailed: mqtt.markMqttPersistenceFailed,
    schedulePostPersistFollowUps: committedFollowUps.schedule,
  });
  return { resolvePool, events, slowMode, committedFollowUps, mqtt, writer };
}
export type IngestRuntime = ReturnType<typeof createIngestRuntime>;
