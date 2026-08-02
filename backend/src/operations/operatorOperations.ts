import { createHash } from 'node:crypto';
import { Redis } from 'ioredis';
import type { QueryResultRow } from 'pg';
import { LINK_V3_KEYS, linkQueueLimits } from '../queue/linkQueueV3.js';
import { getRedisConnectionOptions, getRedisUrl } from '../platform/config/redis.js';
import { listOperatorPlannedNodes } from '../repositories/plannedNodes.js';

export type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type OperatorActor = {
  id: string;
  mode: 'session' | 'automation';
};

export type ObserverAction =
  | 'approve'
  | 'reject'
  | 'provision'
  | 'notification-sent'
  | 'notification-failed';

export type QueueName = 'viewshed' | 'link-v3';
export type QueueAction = 'requeue' | 'purge' | 'repair';
export type PlannedPublicationAction = 'publish' | 'unpublish';

export const VIEWSHED_V2_KEYS = {
  ready: 'meshcore:viewshed:v2:ready',
  payloads: 'meshcore:viewshed:v2:payloads',
  states: 'meshcore:viewshed:v2:states',
  attempts: 'meshcore:viewshed:v2:attempts',
  bytes: 'meshcore:viewshed:v2:bytes',
  pending: 'meshcore:viewshed_pending',
  tokens: 'meshcore:viewshed:v2:tokens',
  leases: 'meshcore:viewshed:v2:leases',
  dead: 'meshcore:viewshed:v2:dead',
  deadReasons: 'meshcore:viewshed:v2:dead_reasons',
  counters: 'meshcore:viewshed:v2:counters',
  events: 'meshcore:viewshed:v2:events',
  enqueued: 'meshcore:viewshed:v2:enqueued',
  dirty: 'meshcore:viewshed:v2:dirty',
  workerHeartbeat: 'meshcore:viewshed:worker_heartbeat',
} as const;

const VIEWSHED_QUEUE_MAX = boundedPositiveInt(
  process.env['VIEWSHED_QUEUE_MAX'],
  1_000,
  10_000,
);
const VIEWSHED_QUEUE_MAX_BYTES = boundedPositiveInt(
  process.env['VIEWSHED_QUEUE_MAX_BYTES'],
  16 * 1024 * 1024,
  256 * 1024 * 1024,
);

let operatorRedis: Redis | null = null;

function boundedPositiveInt(value: string | undefined, fallback: number, max: number): number {
  const parsed = Number(value ?? fallback);
  return Math.min(max, Math.max(1, Number.isFinite(parsed) ? Math.floor(parsed) : fallback));
}

function redis(): Redis {
  if (!operatorRedis) {
    operatorRedis = new Redis(getRedisUrl(), getRedisConnectionOptions());
    operatorRedis.on('error', (error: Error) => {
      console.error('[operator-operations] Redis error', error.message);
    });
  }
  return operatorRedis;
}

export async function closeOperatorOperations(): Promise<void> {
  if (!operatorRedis) return;
  await operatorRedis.quit();
  operatorRedis = null;
}

export function operatorActor(mode: OperatorActor['mode'], sessionId?: string): OperatorActor {
  if (mode === 'automation') return { id: 'automation-token', mode };
  const digest = createHash('sha256').update(String(sessionId ?? '')).digest('hex').slice(0, 16);
  return { id: `browser-session:${digest}`, mode };
}

export function validateIdempotencyKey(value: unknown): string {
  const parsed = typeof value === 'string' ? value.trim() : '';
  if (
    parsed.length < 16
    || parsed.length > 128
    || !/^[A-Za-z0-9._:-]+$/.test(parsed)
  ) {
    throw new Error('INVALID_IDEMPOTENCY_KEY');
  }
  return parsed;
}

export function validateDecisionReason(value: unknown): string {
  const parsed = typeof value === 'string' ? value.trim() : '';
  if (parsed.length < 4 || parsed.length > 500 || /[\u0000-\u001f\u007f]/.test(parsed)) {
    throw new Error('INVALID_DECISION_REASON');
  }
  return parsed;
}

export function validatePlannedPublication(value: unknown): {
  publicName: string;
  publicLat: number;
  publicLon: number;
  publicHeightM: number | null;
  region: string | null;
  expiresAt: string;
} {
  const body = value as Record<string, unknown> | null | undefined;
  const publicName = typeof body?.['publicName'] === 'string' ? body['publicName'].trim() : '';
  const publicLat = Number(body?.['publicLat']);
  const publicLon = Number(body?.['publicLon']);
  const rawHeight = body?.['publicHeightM'];
  const publicHeightM = rawHeight === undefined || rawHeight === null || rawHeight === ''
    ? null
    : Number(rawHeight);
  const rawRegion = typeof body?.['region'] === 'string' ? body['region'].trim().toUpperCase() : '';
  const expiresAtMs = Date.parse(String(body?.['expiresAt'] ?? ''));
  const now = Date.now();
  if (
    publicName.length < 1
    || publicName.length > 100
    || /[\u0000-\u001f\u007f]/.test(publicName)
    || !Number.isFinite(publicLat)
    || publicLat < -90
    || publicLat > 90
    || !Number.isFinite(publicLon)
    || publicLon < -180
    || publicLon > 180
    || (publicHeightM !== null
      && (!Number.isFinite(publicHeightM) || publicHeightM < 0 || publicHeightM > 500))
    || (rawRegion !== '' && !/^[A-Z0-9]{2,8}$/.test(rawRegion))
    || !Number.isFinite(expiresAtMs)
    || expiresAtMs <= now + 60_000
    || expiresAtMs > now + 366 * 24 * 60 * 60_000
  ) {
    throw new Error('INVALID_PLANNED_PUBLICATION');
  }
  return {
    publicName,
    publicLat,
    publicLon,
    publicHeightM,
    region: rawRegion || null,
    expiresAt: new Date(expiresAtMs).toISOString(),
  };
}

export async function listObserverRegistrations(
  query: QueryFn,
  status: string,
  limit: number,
): Promise<Record<string, unknown>> {
  const normalizedStatus = ['pending', 'approved', 'rejected', 'expired', 'provisioned', 'all']
    .includes(status)
    ? status
    : 'pending';
  const boundedLimit = Math.min(100, Math.max(1, Math.floor(limit) || 50));
  const result = await query(
    `SELECT request.id::text,
            request.public_key,
            request.iata,
            request.display_name,
            request.contact,
            request.status,
            request.expires_at,
            request.reviewed_at,
            request.reviewed_by,
            request.decision_reason,
            request.duplicate_of::text,
            request.provisioned_at,
            request.notification_status,
            request.notification_error,
            request.created_at,
            request.updated_at,
            (
              SELECT duplicate.id::text
                FROM observer_registration_requests duplicate
               WHERE duplicate.id <> request.id
                 AND (
                   duplicate.public_key = request.public_key
                   OR lower(duplicate.contact) = lower(request.contact)
                 )
               ORDER BY duplicate.created_at DESC
               LIMIT 1
            ) AS possible_duplicate_id
       FROM observer_registration_requests request
      WHERE ($1 = 'all' OR request.status = $1)
      ORDER BY
        CASE request.status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,
        request.created_at ASC,
        request.id ASC
      LIMIT $2`,
    [normalizedStatus, boundedLimit],
  );
  return {
    generatedAt: new Date().toISOString(),
    status: normalizedStatus,
    registrations: result.rows,
  };
}

export async function actOnObserverRegistration(
  query: QueryFn,
  input: {
    requestId: string;
    action: ObserverAction;
    reason: string;
    duplicateOf?: string;
    idempotencyKey: string;
    actor: OperatorActor;
  },
): Promise<Record<string, unknown>> {
  if (!/^[1-9][0-9]{0,19}$/.test(input.requestId)) throw new Error('INVALID_REQUEST_ID');
  if (!['approve', 'reject', 'provision', 'notification-sent', 'notification-failed'].includes(input.action)) {
    throw new Error('INVALID_OBSERVER_ACTION');
  }
  if (input.duplicateOf && !/^[1-9][0-9]{0,19}$/.test(input.duplicateOf)) {
    throw new Error('INVALID_DUPLICATE_ID');
  }

  const result = await query<{
    audit_id: string;
    action: string;
    target_id: string;
    status: string;
    result: Record<string, unknown> | null;
    was_created: boolean;
  }>(
    `WITH existing AS (
       SELECT id
         FROM operator_audit_events
        WHERE idempotency_key = $4
     ),
     changed AS (
       UPDATE observer_registration_requests request
          SET status = CASE
                WHEN $2 = 'approve' THEN 'approved'
                WHEN $2 = 'reject' THEN 'rejected'
                WHEN $2 = 'provision' THEN 'provisioned'
                ELSE request.status
              END,
              reviewed_at = CASE
                WHEN $2 IN ('approve', 'reject') THEN NOW()
                ELSE request.reviewed_at
              END,
              reviewed_by = CASE
                WHEN $2 IN ('approve', 'reject', 'provision') THEN $1
                ELSE request.reviewed_by
              END,
              decision_reason = CASE
                WHEN $2 IN ('approve', 'reject', 'provision') THEN $5
                ELSE request.decision_reason
              END,
              duplicate_of = CASE WHEN $2 = 'reject' THEN $6::bigint ELSE request.duplicate_of END,
              provisioned_at = CASE WHEN $2 = 'provision' THEN NOW() ELSE request.provisioned_at END,
              notification_status = CASE
                WHEN $2 IN ('approve', 'reject', 'provision') THEN 'pending'
                WHEN $2 = 'notification-sent' THEN 'sent'
                WHEN $2 = 'notification-failed' THEN 'failed'
                ELSE request.notification_status
              END,
              notification_error = CASE
                WHEN $2 = 'notification-failed' THEN $5
                WHEN $2 IN ('approve', 'reject', 'provision', 'notification-sent') THEN NULL
                ELSE request.notification_error
              END,
              updated_at = NOW()
        WHERE request.id = $3::bigint
          AND NOT EXISTS (SELECT 1 FROM existing)
          AND (
            ($2 IN ('approve', 'reject') AND request.status = 'pending')
            OR ($2 = 'provision' AND request.status = 'approved')
            OR ($2 IN ('notification-sent', 'notification-failed')
                AND request.status IN ('approved', 'rejected', 'provisioned')
                AND request.notification_status IN ('pending', 'failed'))
          )
        RETURNING request.*
     ),
     provision AS (
       INSERT INTO observers (public_key, name, location, is_active)
       SELECT public_key, COALESCE(NULLIF(display_name, ''), iata), iata, TRUE
         FROM changed
        WHERE $2 = 'provision'
       ON CONFLICT (public_key) DO UPDATE SET
         name = EXCLUDED.name,
         location = EXCLUDED.location,
         is_active = TRUE
       RETURNING public_key
     ),
     audit_upsert AS (
       INSERT INTO operator_audit_events (
         actor, action, target_type, target_id, idempotency_key, request,
         result, status, completed_at
       ) VALUES (
         $1, $2, 'observer-registration', $3, $4,
         jsonb_build_object('reason', $5::text, 'duplicateOf', $6::text),
         CASE
           WHEN EXISTS (SELECT 1 FROM changed)
           THEN jsonb_build_object(
             'status', (SELECT status FROM changed),
             'notificationStatus', (SELECT notification_status FROM changed),
             'provisioned', EXISTS (SELECT 1 FROM provision)
           )
           ELSE jsonb_build_object('error', 'invalid_state_or_missing')
         END,
         CASE WHEN EXISTS (SELECT 1 FROM changed) THEN 'succeeded' ELSE 'failed' END,
         NOW()
       )
       ON CONFLICT (idempotency_key) DO UPDATE
         SET idempotency_key = EXCLUDED.idempotency_key
       RETURNING id, action, target_id, status, result, (xmax = 0) AS was_created
     )
     SELECT id::text AS audit_id, action, target_id, status, result, was_created
       FROM audit_upsert`,
    [
      input.actor.id,
      input.action,
      input.requestId,
      input.idempotencyKey,
      input.reason,
      input.duplicateOf ?? null,
    ],
  );
  const row = result.rows[0];
  if (!row) throw new Error('AUDIT_WRITE_FAILED');
  if (row.action !== input.action || row.target_id !== input.requestId) {
    throw new Error('IDEMPOTENCY_KEY_REUSED');
  }
  if (row.status === 'failed') throw new Error('INVALID_OBSERVER_STATE');
  return {
    auditId: row.audit_id,
    idempotentReplay: !row.was_created,
    status: row.status,
    result: row.result,
  };
}

export async function actOnPlannedNodePublication(
  query: QueryFn,
  input: {
    plannedNodeId: string;
    action: PlannedPublicationAction;
    reason: string;
    publication?: ReturnType<typeof validatePlannedPublication>;
    idempotencyKey: string;
    actor: OperatorActor;
  },
): Promise<Record<string, unknown>> {
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
      .test(input.plannedNodeId)
  ) {
    throw new Error('INVALID_PLANNED_NODE_ID');
  }
  if (!['publish', 'unpublish'].includes(input.action)) {
    throw new Error('INVALID_PLANNED_PUBLICATION_ACTION');
  }
  if (input.action === 'publish' && !input.publication) {
    throw new Error('INVALID_PLANNED_PUBLICATION');
  }
  const publication = input.publication ?? {
    publicName: '',
    publicLat: 0,
    publicLon: 0,
    publicHeightM: null,
    region: null,
    expiresAt: new Date(0).toISOString(),
  };
  const result = await query<{
    audit_id: string;
    action: string;
    target_id: string;
    status: string;
    result: Record<string, unknown> | null;
    was_created: boolean;
  }>(
    `WITH existing AS (
       SELECT id FROM operator_audit_events WHERE idempotency_key = $4
     ),
     source AS (
       SELECT id FROM planned_nodes WHERE id = $3::uuid
     ),
     published AS (
       INSERT INTO planned_node_publications (
         planned_node_id, public_name, public_lat, public_lon, public_height_m,
         region, expires_at, published_by
       )
       SELECT id, $6, $7, $8, $9, $10, $11, $1
         FROM source
        WHERE $2 = 'publish' AND NOT EXISTS (SELECT 1 FROM existing)
       ON CONFLICT (planned_node_id) DO UPDATE SET
         public_name = EXCLUDED.public_name,
         public_lat = EXCLUDED.public_lat,
         public_lon = EXCLUDED.public_lon,
         public_height_m = EXCLUDED.public_height_m,
         region = EXCLUDED.region,
         published_at = NOW(),
         expires_at = EXCLUDED.expires_at,
         published_by = EXCLUDED.published_by,
         updated_at = NOW()
       RETURNING planned_node_id
     ),
     unpublished AS (
       DELETE FROM planned_node_publications publication_row
        WHERE publication_row.planned_node_id = $3::uuid
          AND $2 = 'unpublish'
          AND NOT EXISTS (SELECT 1 FROM existing)
        RETURNING publication_row.planned_node_id
     ),
     audit_upsert AS (
       INSERT INTO operator_audit_events (
         actor, action, target_type, target_id, idempotency_key, request,
         result, status, completed_at
       ) VALUES (
         $1, 'planned-node-' || $2, 'planned-node', $3, $4,
         jsonb_build_object(
           'reason', $5::text,
           'publicName', NULLIF($6::text, ''),
           'region', $10::text,
           'expiresAt', CASE WHEN $2 = 'publish' THEN $11::text ELSE NULL END
         ),
         CASE
           WHEN EXISTS (SELECT 1 FROM published)
             OR EXISTS (SELECT 1 FROM unpublished)
           THEN jsonb_build_object('published', $2 = 'publish')
           ELSE jsonb_build_object('error', 'missing_or_unchanged')
         END,
         CASE
           WHEN EXISTS (SELECT 1 FROM published)
             OR EXISTS (SELECT 1 FROM unpublished)
           THEN 'succeeded'
           ELSE 'failed'
         END,
         NOW()
       )
       ON CONFLICT (idempotency_key) DO UPDATE
         SET idempotency_key = EXCLUDED.idempotency_key
       RETURNING id, action, target_id, status, result, (xmax = 0) AS was_created
     )
     SELECT id::text AS audit_id, action, target_id, status, result, was_created
       FROM audit_upsert`,
    [
      input.actor.id,
      input.action,
      input.plannedNodeId,
      input.idempotencyKey,
      input.reason,
      publication.publicName,
      publication.publicLat,
      publication.publicLon,
      publication.publicHeightM,
      publication.region,
      publication.expiresAt,
    ],
  );
  const row = result.rows[0];
  if (!row) throw new Error('AUDIT_WRITE_FAILED');
  if (row.action !== `planned-node-${input.action}` || row.target_id !== input.plannedNodeId) {
    throw new Error('IDEMPOTENCY_KEY_REUSED');
  }
  if (row.status === 'failed') throw new Error('PLANNED_PUBLICATION_UNCHANGED');
  return {
    auditId: row.audit_id,
    idempotentReplay: !row.was_created,
    status: row.status,
    result: row.result,
  };
}

type QueueDescriptor = {
  name: QueueName;
  ready: string;
  deferred?: string;
  payloads: string;
  states: string;
  attempts: string;
  bytes: string;
  leases: string;
  tokens: string;
  dead: string;
  deadReasons: string;
  enqueued: string;
  counters: string;
  events: string;
  heartbeat: string;
  maxJobs: number;
  maxBytes: number;
  dedupe?: string;
  dedupeByJob?: string;
  cleanupSet?: string;
};

const QUEUES: Record<QueueName, QueueDescriptor> = {
  viewshed: {
    name: 'viewshed',
    ready: VIEWSHED_V2_KEYS.ready,
    payloads: VIEWSHED_V2_KEYS.payloads,
    states: VIEWSHED_V2_KEYS.states,
    attempts: VIEWSHED_V2_KEYS.attempts,
    bytes: VIEWSHED_V2_KEYS.bytes,
    leases: VIEWSHED_V2_KEYS.leases,
    tokens: VIEWSHED_V2_KEYS.tokens,
    dead: VIEWSHED_V2_KEYS.dead,
    deadReasons: VIEWSHED_V2_KEYS.deadReasons,
    enqueued: VIEWSHED_V2_KEYS.enqueued,
    counters: VIEWSHED_V2_KEYS.counters,
    events: VIEWSHED_V2_KEYS.events,
    heartbeat: VIEWSHED_V2_KEYS.workerHeartbeat,
    maxJobs: VIEWSHED_QUEUE_MAX,
    maxBytes: VIEWSHED_QUEUE_MAX_BYTES,
    cleanupSet: VIEWSHED_V2_KEYS.pending,
  },
  'link-v3': {
    name: 'link-v3',
    ready: LINK_V3_KEYS.ready,
    deferred: LINK_V3_KEYS.deferred,
    payloads: LINK_V3_KEYS.payloads,
    states: LINK_V3_KEYS.states,
    attempts: LINK_V3_KEYS.attempts,
    bytes: LINK_V3_KEYS.bytes,
    leases: LINK_V3_KEYS.leases,
    tokens: LINK_V3_KEYS.tokens,
    dead: LINK_V3_KEYS.dead,
    deadReasons: LINK_V3_KEYS.deadReasons,
    enqueued: LINK_V3_KEYS.enqueued,
    counters: LINK_V3_KEYS.counters,
    events: LINK_V3_KEYS.events,
    heartbeat: LINK_V3_KEYS.workerHeartbeat,
    maxJobs: linkQueueLimits.maxJobs,
    maxBytes: linkQueueLimits.maxBytes,
    dedupe: LINK_V3_KEYS.dedupe,
    dedupeByJob: LINK_V3_KEYS.dedupeByJob,
  },
};

async function queueSnapshot(descriptor: QueueDescriptor): Promise<Record<string, unknown>> {
  const client = redis();
  const pipeline = client.pipeline()
    .hmget(descriptor.counters, 'count', 'bytes', 'dead_count', 'dead_bytes')
    .llen(descriptor.ready)
    .zcard(descriptor.leases)
    .zrange(descriptor.enqueued, 0, 0, 'WITHSCORES')
    .zrevrange(descriptor.dead, 0, 24, 'WITHSCORES')
    .exists(descriptor.heartbeat);
  if (descriptor.deferred) pipeline.llen(descriptor.deferred);
  const values = await pipeline.exec();
  if (!values) throw new Error('REDIS_PIPELINE_FAILED');
  for (const [error] of values) {
    if (error) throw error;
  }
  const counters = values[0]?.[1] as Array<string | null>;
  const oldest = values[3]?.[1] as string[];
  const deadRows = values[4]?.[1] as string[];
  const deadIds = deadRows.filter((_value, index) => index % 2 === 0);
  const deadDetails = await Promise.all(deadIds.map(async (jobId, index) => {
    const [attempts, reason] = await Promise.all([
      client.hget(descriptor.attempts, jobId),
      client.hget(descriptor.deadReasons, jobId),
    ]);
    return {
      jobId,
      deadAt: new Date(Number(deadRows[index * 2 + 1] ?? 0)).toISOString(),
      attempts: Number(attempts ?? 0),
      reason: reason ?? 'attempt_limit_exceeded',
    };
  }));
  const oldestAtMs = Number(oldest[1] ?? 0);
  return {
    name: descriptor.name,
    capacity: { jobs: descriptor.maxJobs, bytes: descriptor.maxBytes },
    active: { count: Number(counters[0] ?? 0), bytes: Number(counters[1] ?? 0) },
    ready: Number(values[1]?.[1] ?? 0),
    deferred: descriptor.deferred ? Number(values[6]?.[1] ?? 0) : 0,
    leases: Number(values[2]?.[1] ?? 0),
    workerHealthy: Number(values[5]?.[1] ?? 0) === 1,
    oldestAgeSeconds: oldestAtMs > 0 ? Math.max(0, Math.floor((Date.now() - oldestAtMs) / 1_000)) : 0,
    dead: {
      count: Number(counters[2] ?? 0),
      bytes: Number(counters[3] ?? 0),
      jobs: deadDetails,
    },
  };
}

export async function loadOperationsDashboard(query: QueryFn): Promise<Record<string, unknown>> {
  const [analysisRuns, plannedCoverage, plannedNodes, mlLearner, calibration, viewshed, link] = await Promise.all([
    query(
      `SELECT run_id, workload, scope, status, checkpoint, total_items,
              lease_owner, lease_expires_at, run_deadline_at, attempt,
              heartbeat_at, started_at, completed_at, terminal_reason
         FROM analysis_runs
        ORDER BY started_at DESC
        LIMIT 50`,
    ),
    query(
      `SELECT job_id, status, heartbeat_at, expires_at, error, created_at, updated_at
         FROM planned_coverage_jobs
        ORDER BY created_at DESC
        LIMIT 50`,
    ),
    listOperatorPlannedNodes(query),
    query(
      `SELECT cursor_observed_at, lease_expires_at, heartbeat_at, run_started_at,
              run_deadline_at, next_run_at, last_trained_at, next_training_at,
              model_version, data_version, last_terminal_reason, updated_at
         FROM ml_learner_state
        WHERE singleton = TRUE`,
    ),
    query(
      `SELECT versions.version, versions.network, versions.generation,
              versions.evaluated_packets, versions.evaluated_hops,
              versions.top1_accuracy, versions.top3_accuracy,
              versions.complete_path_accuracy, versions.mean_path_completion,
              versions.is_active, versions.promoted_at,
              calibration.mean_pred_confidence,
              calibration.confidence_scale,
              calibration.confidence_bias,
              calibration.recommended_threshold,
              calibration.updated_at AS calibration_updated_at
         FROM ml_model_versions versions
         LEFT JOIN path_model_calibration calibration
           ON calibration.network = versions.network
        ORDER BY versions.trained_at DESC
        LIMIT 40`,
    ),
    queueSnapshot(QUEUES.viewshed),
    queueSnapshot(QUEUES['link-v3']),
  ]);
  return {
    generatedAt: new Date().toISOString(),
    queues: [viewshed, link],
    analysisRuns: analysisRuns.rows,
    plannedCoverageJobs: plannedCoverage.rows,
    plannedNodes,
    mlLearner: mlLearner.rows[0] ?? null,
    modelCalibration: calibration.rows,
  };
}

const PURGE_DEAD_SCRIPT = `
if redis.call('HGET', KEYS[2], ARGV[1]) ~= 'dead' then return 0 end
local payload_bytes = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
local dedupe_key = redis.call('HGET', KEYS[7], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
if KEYS[6] ~= '' and dedupe_key and redis.call('HGET', KEYS[6], dedupe_key) == ARGV[1] then
  redis.call('HDEL', KEYS[6], dedupe_key)
end
if KEYS[10] ~= '' then redis.call('SREM', KEYS[10], ARGV[1]) end
redis.call('HDEL', KEYS[11], ARGV[1])
local dead_count = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'dead_count') or '0') - 1)
local dead_bytes = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'dead_bytes') or '0') - payload_bytes)
redis.call('HSET', KEYS[8], 'dead_count', dead_count, 'dead_bytes', dead_bytes)
redis.call('LPUSH', KEYS[9], 'operator_purge')
redis.call('LTRIM', KEYS[9], 0, 255)
return 1
`;

const REQUEUE_DEAD_SCRIPT = `
if redis.call('HGET', KEYS[2], ARGV[1]) ~= 'dead' then return 0 end
local payload_bytes = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
local count = tonumber(redis.call('HGET', KEYS[6], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[6], 'bytes') or '0')
if count + 1 > tonumber(ARGV[2]) or bytes + payload_bytes > tonumber(ARGV[3]) then return -1 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[9], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], 'queued')
redis.call('HSET', KEYS[3], ARGV[1], '0')
redis.call('LPUSH', KEYS[5], ARGV[1])
redis.call('ZADD', KEYS[7], ARGV[4], ARGV[1])
redis.call('HSET', KEYS[6],
  'count', count + 1,
  'bytes', bytes + payload_bytes,
  'dead_count', math.max(0, tonumber(redis.call('HGET', KEYS[6], 'dead_count') or '0') - 1),
  'dead_bytes', math.max(0, tonumber(redis.call('HGET', KEYS[6], 'dead_bytes') or '0') - payload_bytes))
redis.call('LPUSH', KEYS[8], 'operator_requeue')
redis.call('LTRIM', KEYS[8], 0, 255)
return 1
`;

const REPAIR_COUNTERS_SCRIPT = `
local rows = redis.call('HGETALL', KEYS[1])
local active_count = 0
local active_bytes = 0
local dead_count = 0
local dead_bytes = 0
local invalid_states = 0
for index = 1, #rows, 2 do
  local job_id = rows[index]
  local state = rows[index + 1]
  local payload_bytes = tonumber(redis.call('HGET', KEYS[2], job_id) or '0')
  if state == 'queued' or state == 'in_flight' then
    active_count = active_count + 1
    active_bytes = active_bytes + payload_bytes
    if not redis.call('ZSCORE', KEYS[5], job_id) then
      redis.call('ZADD', KEYS[5], ARGV[1], job_id)
    end
  elseif state == 'dead' then
    dead_count = dead_count + 1
    dead_bytes = dead_bytes + payload_bytes
    redis.call('ZREM', KEYS[5], job_id)
  elseif state ~= 'complete' then
    invalid_states = invalid_states + 1
  else
    redis.call('ZREM', KEYS[5], job_id)
  end
end
local old_count = tonumber(redis.call('HGET', KEYS[3], 'count') or '0')
local old_bytes = tonumber(redis.call('HGET', KEYS[3], 'bytes') or '0')
local old_dead_count = tonumber(redis.call('HGET', KEYS[3], 'dead_count') or '0')
local old_dead_bytes = tonumber(redis.call('HGET', KEYS[3], 'dead_bytes') or '0')
redis.call('HSET', KEYS[3],
  'count', active_count, 'bytes', active_bytes,
  'dead_count', dead_count, 'dead_bytes', dead_bytes)
redis.call('LPUSH', KEYS[4], 'operator_repair')
redis.call('LTRIM', KEYS[4], 0, 255)
return {
  old_count, active_count, old_bytes, active_bytes,
  old_dead_count, dead_count, old_dead_bytes, dead_bytes, invalid_states
}
`;

async function beginAudit(
  query: QueryFn,
  input: {
    actor: OperatorActor;
    action: string;
    targetType: string;
    targetId: string;
    idempotencyKey: string;
    request: Record<string, unknown>;
  },
): Promise<{ id: string; existing: boolean; action: string; targetId: string; status: string; result: unknown }> {
  const audit = await query<{
    id: string;
    action: string;
    target_id: string;
    status: string;
    result: unknown;
    existing: boolean;
  }>(
    `WITH inserted AS (
       INSERT INTO operator_audit_events (
         actor, action, target_type, target_id, idempotency_key, request
       ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING id, action, target_id, status, result
     )
     SELECT id::text, action, target_id, status, result, FALSE AS existing FROM inserted
     UNION ALL
     SELECT id::text, action, target_id, status, result, TRUE
       FROM operator_audit_events
      WHERE idempotency_key = $5 AND NOT EXISTS (SELECT 1 FROM inserted)
     LIMIT 1`,
    [
      input.actor.id,
      input.action,
      input.targetType,
      input.targetId,
      input.idempotencyKey,
      JSON.stringify(input.request),
    ],
  );
  const row = audit.rows[0];
  if (!row) throw new Error('AUDIT_WRITE_FAILED');
  return {
    id: row.id,
    existing: row.existing,
    action: row.action,
    targetId: row.target_id,
    status: row.status,
    result: row.result,
  };
}

async function finishAudit(
  query: QueryFn,
  auditId: string,
  status: 'succeeded' | 'failed',
  result: Record<string, unknown>,
): Promise<void> {
  await query(
    `UPDATE operator_audit_events
        SET status = $2, result = $3::jsonb, completed_at = NOW()
      WHERE id = $1::bigint AND status = 'started'`,
    [auditId, status, JSON.stringify(result)],
  );
}

export async function actOnQueueJob(
  query: QueryFn,
  input: {
    queue: QueueName;
    jobId: string;
    action: QueueAction;
    confirmation?: string;
    idempotencyKey: string;
    actor: OperatorActor;
  },
): Promise<Record<string, unknown>> {
  const descriptor = QUEUES[input.queue];
  if (!descriptor || !['requeue', 'purge', 'repair'].includes(input.action)) {
    throw new Error('INVALID_QUEUE_ACTION');
  }
  if (
    input.jobId.length < 1
    || input.jobId.length > 200
    || /[\u0000-\u001f\u007f]/.test(input.jobId)
  ) {
    throw new Error('INVALID_JOB_ID');
  }
  const expectedConfirmation = input.action === 'repair'
    ? `REPAIR ${input.queue}`
    : input.action === 'purge'
      ? `PURGE ${input.jobId}`
      : undefined;
  if (expectedConfirmation && input.confirmation !== expectedConfirmation) {
    throw new Error('CONFIRMATION_REQUIRED');
  }
  const targetId = input.action === 'repair' ? input.queue : input.jobId;
  const audit = await beginAudit(query, {
    actor: input.actor,
    action: `queue-${input.action}`,
    targetType: input.action === 'repair' ? 'queue' : 'queue-job',
    targetId,
    idempotencyKey: input.idempotencyKey,
    request: { queue: input.queue, jobId: input.jobId },
  });
  if (audit.action !== `queue-${input.action}` || audit.targetId !== targetId) {
    throw new Error('IDEMPOTENCY_KEY_REUSED');
  }
  if (audit.existing) {
    return {
      auditId: audit.id,
      idempotentReplay: true,
      status: audit.status,
      result: audit.result,
    };
  }

  try {
    let actionResult: Record<string, unknown>;
    if (input.action === 'purge') {
      const applied = Number(await redis().eval(
        PURGE_DEAD_SCRIPT,
        11,
        descriptor.dead,
        descriptor.states,
        descriptor.payloads,
        descriptor.bytes,
        descriptor.attempts,
        descriptor.dedupe ?? '',
        descriptor.dedupeByJob ?? '',
        descriptor.counters,
        descriptor.events,
        descriptor.cleanupSet ?? '',
        descriptor.deadReasons,
        input.jobId,
      ));
      if (applied !== 1) throw new Error('JOB_NOT_DEAD');
      actionResult = { applied: true, jobId: input.jobId };
    } else if (input.action === 'requeue') {
      const applied = Number(await redis().eval(
        REQUEUE_DEAD_SCRIPT,
        9,
        descriptor.dead,
        descriptor.states,
        descriptor.attempts,
        descriptor.bytes,
        descriptor.ready,
        descriptor.counters,
        descriptor.enqueued,
        descriptor.events,
        descriptor.deadReasons,
        input.jobId,
        descriptor.maxJobs,
        descriptor.maxBytes,
        Date.now(),
      ));
      if (applied === -1) throw new Error('QUEUE_CAPACITY_EXCEEDED');
      if (applied !== 1) throw new Error('JOB_NOT_DEAD');
      actionResult = { applied: true, jobId: input.jobId };
    } else {
      const values = await redis().eval(
        REPAIR_COUNTERS_SCRIPT,
        5,
        descriptor.states,
        descriptor.bytes,
        descriptor.counters,
        descriptor.events,
        descriptor.enqueued,
        Date.now(),
      ) as Array<number | string>;
      actionResult = {
        applied: true,
        before: {
          count: Number(values[0]),
          bytes: Number(values[2]),
          deadCount: Number(values[4]),
          deadBytes: Number(values[6]),
        },
        after: {
          count: Number(values[1]),
          bytes: Number(values[3]),
          deadCount: Number(values[5]),
          deadBytes: Number(values[7]),
          invalidStates: Number(values[8]),
        },
      };
    }
    await finishAudit(query, audit.id, 'succeeded', actionResult);
    return { auditId: audit.id, idempotentReplay: false, status: 'succeeded', result: actionResult };
  } catch (error) {
    const code = (error as Error).message.slice(0, 100);
    await finishAudit(query, audit.id, 'failed', { error: code });
    throw error;
  }
}

export async function listRecentOperatorAudit(
  query: QueryFn,
  limit = 100,
): Promise<QueryResultRow[]> {
  const result = await query(
    `SELECT id::text, actor, action, target_type, target_id, request,
            result, status, created_at, completed_at
       FROM operator_audit_events
      ORDER BY created_at DESC
      LIMIT $1`,
    [Math.min(100, Math.max(1, Math.floor(limit) || 50))],
  );
  return result.rows;
}
