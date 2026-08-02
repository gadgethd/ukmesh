"""Crash-safe Redis protocol for MeshCore link jobs."""

import hashlib
import json
import os
import secrets
import threading
import time
import uuid

READY = 'meshcore:link:v3:ready'
DEFERRED = 'meshcore:link:v3:deferred'
PAYLOADS = 'meshcore:link:v3:payloads'
STATES = 'meshcore:link:v3:states'
ATTEMPTS = 'meshcore:link:v3:attempts'
BYTES = 'meshcore:link:v3:bytes'
DEDUPE = 'meshcore:link:v3:dedupe'
DEDUPE_BY_JOB = 'meshcore:link:v3:dedupe_by_job'
LEASES = 'meshcore:link:v3:leases'
TOKENS = 'meshcore:link:v3:tokens'
DEAD = 'meshcore:link:v3:dead'
DEAD_REASONS = 'meshcore:link:v3:dead_reasons'
ENQUEUED = 'meshcore:link:v3:enqueued'
COMPLETED = 'meshcore:link:v3:completed'
COUNTERS = 'meshcore:link:v3:counters'
REBUILD = 'meshcore:link:v3:rebuild'
WORKER_HEARTBEAT = 'meshcore:link:v3:worker_heartbeat'
EVENTS = 'meshcore:link:v3:events'

MAX_JOBS = max(1, min(100_000, int(os.environ.get('LINK_QUEUE_V3_MAX_JOBS', '5000'))))
MAX_BYTES = max(1, min(1024 * 1024 * 1024, int(os.environ.get('LINK_QUEUE_V3_MAX_BYTES', str(64 * 1024 * 1024)))))
MAX_PAYLOAD_BYTES = max(1, min(1024 * 1024, int(os.environ.get('LINK_QUEUE_V3_MAX_PAYLOAD_BYTES', str(32 * 1024)))))
MAX_ATTEMPTS = max(1, min(20, int(os.environ.get('LINK_QUEUE_V3_MAX_ATTEMPTS', '5'))))
LEASE_MS = max(10_000, min(30 * 60_000, int(os.environ.get('LINK_QUEUE_V3_LEASE_MS', '120000'))))
COMPLETED_RETENTION_MS = max(60_000, int(os.environ.get('LINK_QUEUE_V3_COMPLETED_RETENTION_MS', str(7 * 24 * 60 * 60_000))))
DEAD_MAX_JOBS = max(1, min(10_000, int(os.environ.get('LINK_QUEUE_V3_DEAD_MAX_JOBS', '1000'))))
DEAD_MAX_BYTES = max(1, min(256 * 1024 * 1024, int(os.environ.get('LINK_QUEUE_V3_DEAD_MAX_BYTES', str(32 * 1024 * 1024)))))
DEAD_RETENTION_MS = max(60_000, int(os.environ.get('LINK_QUEUE_V3_DEAD_RETENTION_MS', str(30 * 24 * 60 * 60_000))))

ADMIT_SCRIPT = """
local existing = redis.call('HGET', KEYS[6], ARGV[2])
if existing then
  local existing_state = redis.call('HGET', KEYS[3], existing)
  if existing == ARGV[1] and existing_state == 'complete' then
    return {'duplicate', existing}
  end
  if existing_state == 'queued' or existing_state == 'in_flight' or existing_state == 'dead' then
    return {'coalesced', existing}
  end
end
local payload_bytes = tonumber(ARGV[4])
if payload_bytes > tonumber(ARGV[7]) then return {'oversized', ''} end
local count = tonumber(redis.call('HGET', KEYS[10], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[10], 'bytes') or '0')
if count + 1 > tonumber(ARGV[5]) or bytes + payload_bytes > tonumber(ARGV[6]) then
  return {'full', ''}
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[3], ARGV[1], 'queued')
redis.call('HSET', KEYS[4], ARGV[1], '0')
redis.call('HSET', KEYS[5], ARGV[1], tostring(payload_bytes))
redis.call('HSET', KEYS[6], ARGV[2], ARGV[1])
redis.call('HSET', KEYS[7], ARGV[1], ARGV[2])
redis.call('HINCRBY', KEYS[10], 'count', 1)
redis.call('HINCRBY', KEYS[10], 'bytes', payload_bytes)
redis.call('ZADD', KEYS[12], ARGV[9], ARGV[1])
if ARGV[8] == '' and redis.call('EXISTS', KEYS[9]) == 1 then
  redis.call('LPUSH', KEYS[8], ARGV[1])
else
  redis.call('LPUSH', KEYS[1], ARGV[1])
end
redis.call('LPUSH', KEYS[11], 'admit')
redis.call('LTRIM', KEYS[11], 0, 255)
return {'accepted', ARGV[1]}
"""

CLAIM_SCRIPT = """
if redis.call('EXISTS', KEYS[8]) == 0 then
  local recovered = 0
  while recovered < 1000 do
    local deferred_id = redis.call('RPOP', KEYS[7])
    if not deferred_id then break end
    if redis.call('HGET', KEYS[3], deferred_id) == 'queued' then
      redis.call('LPUSH', KEYS[1], deferred_id)
      recovered = recovered + 1
    end
  end
end
while true do
  local job_id = redis.call('RPOP', KEYS[1])
  if not job_id then return nil end
  if redis.call('HGET', KEYS[3], job_id) == 'queued' then
    redis.call('HSET', KEYS[3], job_id, 'in_flight')
    redis.call('HINCRBY', KEYS[4], job_id, 1)
    redis.call('HSET', KEYS[6], job_id, ARGV[1])
    redis.call('ZADD', KEYS[5], ARGV[2], job_id)
    local payload = redis.call('HGET', KEYS[2], job_id)
    redis.call('LPUSH', KEYS[9], 'claim')
    redis.call('LTRIM', KEYS[9], 0, 255)
    return {job_id, payload or '', redis.call('HGET', KEYS[4], job_id)}
  end
end
"""

ACK_SCRIPT = """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= 'in_flight'
   or redis.call('HGET', KEYS[6], ARGV[1]) ~= ARGV[2] then
  return 0
end
local payload_bytes = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
redis.call('ZREM', KEYS[7], ARGV[1])
redis.call('ZREM', KEYS[12], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HSET', KEYS[3], ARGV[1], 'complete')
redis.call('ZADD', KEYS[10], ARGV[3], ARGV[1])
local count = math.max(0, tonumber(redis.call('HGET', KEYS[9], 'count') or '0') - 1)
local bytes = math.max(0, tonumber(redis.call('HGET', KEYS[9], 'bytes') or '0') - payload_bytes)
redis.call('HSET', KEYS[9], 'count', count, 'bytes', bytes)
redis.call('LPUSH', KEYS[11], 'ack')
redis.call('LTRIM', KEYS[11], 0, 255)
return 1
"""

NACK_SCRIPT = """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= 'in_flight'
   or redis.call('HGET', KEYS[6], ARGV[1]) ~= ARGV[2] then
  return 'invalid'
end
redis.call('ZREM', KEYS[7], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
local attempts = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
if attempts >= tonumber(ARGV[3]) then
  local payload_bytes = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
  local active_count = math.max(0, tonumber(redis.call('HGET', KEYS[10], 'count') or '0') - 1)
  local active_bytes = math.max(0, tonumber(redis.call('HGET', KEYS[10], 'bytes') or '0') - payload_bytes)
  redis.call('HSET', KEYS[10], 'count', active_count, 'bytes', active_bytes)
  redis.call('ZREM', KEYS[13], ARGV[1])
  local dead_count = tonumber(redis.call('HGET', KEYS[10], 'dead_count') or '0')
  local dead_bytes = tonumber(redis.call('HGET', KEYS[10], 'dead_bytes') or '0')
  if dead_count + 1 > tonumber(ARGV[4]) or dead_bytes + payload_bytes > tonumber(ARGV[5]) then
    local dedupe_key = redis.call('HGET', KEYS[12], ARGV[1])
    if dedupe_key and redis.call('HGET', KEYS[11], dedupe_key) == ARGV[1] then
      redis.call('HDEL', KEYS[11], dedupe_key)
    end
    redis.call('HDEL', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[3], ARGV[1])
    redis.call('HDEL', KEYS[4], ARGV[1])
    redis.call('HDEL', KEYS[5], ARGV[1])
    redis.call('HDEL', KEYS[12], ARGV[1])
    redis.call('HDEL', KEYS[14], ARGV[1])
    redis.call('LPUSH', KEYS[9], 'dead_purged')
    redis.call('LTRIM', KEYS[9], 0, 255)
    return 'purged'
  end
  redis.call('HSET', KEYS[3], ARGV[1], 'dead')
  redis.call('ZADD', KEYS[8], ARGV[6], ARGV[1])
  redis.call('HSET', KEYS[14], ARGV[1], ARGV[7])
  redis.call('HSET', KEYS[10], 'dead_count', dead_count + 1, 'dead_bytes', dead_bytes + payload_bytes)
  redis.call('LPUSH', KEYS[9], 'dead')
  redis.call('LTRIM', KEYS[9], 0, 255)
  return 'dead'
end
redis.call('HSET', KEYS[3], ARGV[1], 'queued')
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[9], 'retry')
redis.call('LTRIM', KEYS[9], 0, 255)
return 'retry'
"""

VERIFY_LEASE_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'in_flight' then return 0 end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local expires = redis.call('ZSCORE', KEYS[3], ARGV[1])
if not expires or tonumber(expires) <= tonumber(ARGV[3]) then return 0 end
return 1
"""

PURGE_DEAD_SCRIPT = """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= 'dead' then return 0 end
local payload_bytes = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
local dedupe_key = redis.call('HGET', KEYS[7], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HDEL', KEYS[9], ARGV[1])
if dedupe_key and redis.call('HGET', KEYS[6], dedupe_key) == ARGV[1] then
  redis.call('HDEL', KEYS[6], dedupe_key)
end
local dead_count = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'dead_count') or '0') - 1)
local dead_bytes = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'dead_bytes') or '0') - payload_bytes)
redis.call('HSET', KEYS[8], 'dead_count', dead_count, 'dead_bytes', dead_bytes)
return 1
"""

REQUEUE_DEAD_SCRIPT = """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= 'dead' then return 0 end
local payload_bytes = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
local count = tonumber(redis.call('HGET', KEYS[6], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[6], 'bytes') or '0')
if count + 1 > tonumber(ARGV[2]) or bytes + payload_bytes > tonumber(ARGV[3]) then return -1 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[8], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], 'queued')
redis.call('HSET', KEYS[3], ARGV[1], '0')
redis.call('LPUSH', KEYS[5], ARGV[1])
redis.call('ZADD', KEYS[7], ARGV[4], ARGV[1])
redis.call('HSET', KEYS[6],
  'count', count + 1,
  'bytes', bytes + payload_bytes,
  'dead_count', math.max(0, tonumber(redis.call('HGET', KEYS[6], 'dead_count') or '0') - 1),
  'dead_bytes', math.max(0, tonumber(redis.call('HGET', KEYS[6], 'dead_bytes') or '0') - payload_bytes))
return 1
"""

RENEW_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'in_flight'
   or redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return 1
"""

REAP_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local count = 0
for _, job_id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], job_id)
  if redis.call('HGET', KEYS[2], job_id) == 'in_flight' then
    redis.call('HSET', KEYS[2], job_id, 'queued')
    redis.call('HDEL', KEYS[3], job_id)
    redis.call('LPUSH', KEYS[4], job_id)
    count = count + 1
  end
end
if count > 0 then
  redis.call('LPUSH', KEYS[5], 'reap')
  redis.call('LTRIM', KEYS[5], 0, 255)
end
return count
"""

CLEAN_COMPLETED_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local count = 0
for _, job_id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], job_id)
  if redis.call('HGET', KEYS[2], job_id) == 'complete' then
    local dedupe_key = redis.call('HGET', KEYS[3], job_id)
    if dedupe_key and redis.call('HGET', KEYS[4], dedupe_key) == job_id then
      redis.call('HDEL', KEYS[4], dedupe_key)
    end
    redis.call('HDEL', KEYS[3], job_id)
    redis.call('HDEL', KEYS[2], job_id)
    count = count + 1
  end
end
return count
"""

AUDIT_REPAIR_SCRIPT = """
local rows = redis.call('HGETALL', KEYS[1])
local active_count = 0
local active_bytes = 0
local dead_count = 0
local dead_bytes = 0
local queued_count = 0
local in_flight_count = 0
local retry_attempts = 0
local invalid_states = 0
for index = 1, #rows, 2 do
  local job_id = rows[index]
  local state = rows[index + 1]
  local payload_bytes = tonumber(redis.call('HGET', KEYS[2], job_id) or '0')
  local attempts = tonumber(redis.call('HGET', KEYS[3], job_id) or '0')
  retry_attempts = retry_attempts + math.max(0, attempts - 1)
  if state == 'queued' then
    active_count = active_count + 1
    active_bytes = active_bytes + payload_bytes
    queued_count = queued_count + 1
    if ARGV[1] == '1' and not redis.call('ZSCORE', KEYS[10], job_id) then
      redis.call('ZADD', KEYS[10], ARGV[2], job_id)
    end
  elseif state == 'in_flight' then
    active_count = active_count + 1
    active_bytes = active_bytes + payload_bytes
    in_flight_count = in_flight_count + 1
    if ARGV[1] == '1' and not redis.call('ZSCORE', KEYS[10], job_id) then
      redis.call('ZADD', KEYS[10], ARGV[2], job_id)
    end
  elseif state == 'dead' then
    dead_count = dead_count + 1
    dead_bytes = dead_bytes + payload_bytes
    if ARGV[1] == '1' then redis.call('ZREM', KEYS[10], job_id) end
  elseif state ~= 'complete' then
    invalid_states = invalid_states + 1
  elseif ARGV[1] == '1' then
    redis.call('ZREM', KEYS[10], job_id)
  end
end
local old_count = tonumber(redis.call('HGET', KEYS[4], 'count') or '0')
local old_bytes = tonumber(redis.call('HGET', KEYS[4], 'bytes') or '0')
local old_dead_count = tonumber(redis.call('HGET', KEYS[4], 'dead_count') or '0')
local old_dead_bytes = tonumber(redis.call('HGET', KEYS[4], 'dead_bytes') or '0')
if ARGV[1] == '1' then
  redis.call('HSET', KEYS[4],
    'count', active_count,
    'bytes', active_bytes,
    'dead_count', dead_count,
    'dead_bytes', dead_bytes)
  redis.call('LPUSH', KEYS[9], 'repair')
  redis.call('LTRIM', KEYS[9], 0, 255)
end
local oldest = redis.call('ZRANGE', KEYS[10], 0, 0, 'WITHSCORES')
return {
  old_count, active_count,
  old_bytes, active_bytes,
  old_dead_count, dead_count,
  old_dead_bytes, dead_bytes,
  queued_count, in_flight_count,
  redis.call('LLEN', KEYS[5]),
  redis.call('LLEN', KEYS[6]),
  redis.call('ZCARD', KEYS[7]),
  retry_attempts,
  invalid_states,
  redis.call('HLEN', KEYS[8]),
  redis.call('ZCARD', KEYS[10]),
  oldest[2] or '0'
}
"""


def audit_invariants(client, apply: bool = False) -> dict[str, int | bool]:
    values = [
        int(value) for value in client.eval(
            AUDIT_REPAIR_SCRIPT,
            10,
            STATES,
            BYTES,
            ATTEMPTS,
            COUNTERS,
            READY,
            DEFERRED,
            DEAD,
            TOKENS,
            EVENTS,
            ENQUEUED,
            '1' if apply else '0',
            int(time.time() * 1000),
        )
    ]
    (
        old_count,
        actual_count,
        old_bytes,
        actual_bytes,
        old_dead_count,
        actual_dead_count,
        old_dead_bytes,
        actual_dead_bytes,
        queued,
        in_flight,
        ready_entries,
        deferred_entries,
        dead_entries,
        retries,
        invalid_states,
        token_count,
        enqueued_entries,
        oldest_enqueued_at_ms,
    ) = values
    return {
        'applied': apply,
        'recorded_count': old_count,
        'actual_count': actual_count,
        'count_delta': actual_count - old_count,
        'recorded_bytes': old_bytes,
        'actual_bytes': actual_bytes,
        'bytes_delta': actual_bytes - old_bytes,
        'recorded_dead_count': old_dead_count,
        'actual_dead_count': actual_dead_count,
        'dead_count_delta': actual_dead_count - old_dead_count,
        'recorded_dead_bytes': old_dead_bytes,
        'actual_dead_bytes': actual_dead_bytes,
        'dead_bytes_delta': actual_dead_bytes - old_dead_bytes,
        'queued': queued,
        'in_flight': in_flight,
        'ready_entries': ready_entries,
        'deferred_entries': deferred_entries,
        'dead_entries': dead_entries,
        'retries': retries,
        'invalid_states': invalid_states,
        'lease_tokens': token_count,
        'enqueued_entries': enqueued_entries,
        'oldest_enqueued_at_ms': oldest_enqueued_at_ms,
        'consistent': (
            old_count == actual_count
            and old_bytes == actual_bytes
            and old_dead_count == actual_dead_count
            and old_dead_bytes == actual_dead_bytes
            and invalid_states == 0
            and enqueued_entries == actual_count
        ),
    }


def _payload_bytes(payload: str) -> int:
    return len(payload.encode('utf-8'))


def observation_identity(packet_hash: str, rx_node_id: str) -> tuple[str, str]:
    digest = hashlib.sha256(f'observe\0{packet_hash.lower()}\0{rx_node_id.lower()}'.encode()).hexdigest()
    return f'lo_{digest}', f'observe:{digest}'


def physical_identity(node_a_id: str, node_b_id: str, generation: str | None = None) -> tuple[str, str, str, str]:
    a_id, b_id = sorted((node_a_id, node_b_id))
    return f'lp_{uuid.uuid4()}', f'physical:{generation or "live"}:{a_id}:{b_id}', a_id, b_id


def admit(client, job: dict) -> tuple[str, str | None]:
    payload = json.dumps(job, separators=(',', ':'), sort_keys=True)
    result = client.eval(
        ADMIT_SCRIPT, 12,
        READY, PAYLOADS, STATES, ATTEMPTS, BYTES, DEDUPE, DEDUPE_BY_JOB,
        DEFERRED, REBUILD, COUNTERS, EVENTS, ENQUEUED,
        job['job_id'], job['dedupe_key'], payload, _payload_bytes(payload),
        MAX_JOBS, MAX_BYTES, MAX_PAYLOAD_BYTES, job.get('generation') or '',
        int(time.time() * 1000),
    )
    status = str(result[0])
    job_id = str(result[1]) if result[1] else None
    return status, job_id


def admit_physical(client, node_a_id: str, node_b_id: str, generation: str | None = None) -> tuple[str, str | None]:
    job_id, dedupe_key, a_id, b_id = physical_identity(node_a_id, node_b_id, generation)
    job = {
        'version': 3, 'type': 'physical_pair', 'job_id': job_id,
        'dedupe_key': dedupe_key, 'node_a_id': a_id, 'node_b_id': b_id,
    }
    if generation:
        job['generation'] = generation
    return admit(client, job)


def claim(client) -> tuple[str, str, dict, int] | None:
    token = secrets.token_hex(16)
    result = client.eval(
        CLAIM_SCRIPT, 9, READY, PAYLOADS, STATES, ATTEMPTS, LEASES, TOKENS,
        DEFERRED, REBUILD, EVENTS,
        token, int(time.time() * 1000) + LEASE_MS,
    )
    if not result:
        return None
    return str(result[0]), token, json.loads(result[1]), int(result[2])


def ack(client, job_id: str, token: str) -> bool:
    result = client.eval(
        ACK_SCRIPT, 12,
        READY, PAYLOADS, STATES, ATTEMPTS, BYTES, TOKENS, LEASES, DEAD,
        COUNTERS, COMPLETED, EVENTS, ENQUEUED,
        job_id, token, int(time.time() * 1000) + COMPLETED_RETENTION_MS,
    )
    return int(result) == 1


def nack(client, job_id: str, token: str, reason: str = 'attempt_limit_exceeded') -> str:
    return str(client.eval(
        NACK_SCRIPT, 14,
        READY, PAYLOADS, STATES, ATTEMPTS, BYTES, TOKENS, LEASES, DEAD, EVENTS,
        COUNTERS, DEDUPE, DEDUPE_BY_JOB, ENQUEUED, DEAD_REASONS,
        job_id, token, MAX_ATTEMPTS, DEAD_MAX_JOBS, DEAD_MAX_BYTES, int(time.time() * 1000),
        str(reason)[:100],
    ))


def owns_lease(client, job_id: str, token: str) -> bool:
    return int(client.eval(
        VERIFY_LEASE_SCRIPT, 3, STATES, TOKENS, LEASES,
        job_id, token, int(time.time() * 1000),
    )) == 1


def purge_dead(client, job_id: str) -> bool:
    return int(client.eval(
        PURGE_DEAD_SCRIPT, 9,
        DEAD, STATES, PAYLOADS, BYTES, ATTEMPTS, DEDUPE, DEDUPE_BY_JOB, COUNTERS,
        DEAD_REASONS,
        job_id,
    )) == 1


def requeue_dead(client, job_id: str) -> str:
    result = int(client.eval(
        REQUEUE_DEAD_SCRIPT, 8,
        DEAD, STATES, ATTEMPTS, BYTES, READY, COUNTERS, ENQUEUED,
        DEAD_REASONS,
        job_id, MAX_JOBS, MAX_BYTES, int(time.time() * 1000),
    ))
    return 'requeued' if result == 1 else 'full' if result == -1 else 'not_found'


def cleanup_dead(client, limit: int = 100) -> int:
    expired = client.zrangebyscore(DEAD, '-inf', int(time.time() * 1000) - DEAD_RETENTION_MS, start=0, num=limit)
    return sum(1 for job_id in expired if purge_dead(client, job_id))


def reap(client, limit: int = 100) -> int:
    return int(client.eval(
        REAP_SCRIPT, 5, LEASES, STATES, TOKENS, READY, EVENTS,
        int(time.time() * 1000), limit,
    ))


def cleanup_completed(client, limit: int = 500) -> int:
    return int(client.eval(
        CLEAN_COMPLETED_SCRIPT, 4, COMPLETED, STATES, DEDUPE_BY_JOB, DEDUPE,
        int(time.time() * 1000), limit,
    ))


class LeaseRenewer:
    def __init__(self, redis_factory, job_id: str, token: str):
        self.redis_factory = redis_factory
        self.job_id = job_id
        self.token = token
        self.stop_event = threading.Event()
        self.lease_lost = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f'link-lease-{job_id[:8]}', daemon=True)

    def _run(self):
        client = None
        last_success = time.monotonic()
        backoff = 0.25
        while not self.stop_event.wait(max(0.25, min(backoff, LEASE_MS / 3000))):
            try:
                if client is None:
                    client = self.redis_factory()
                renewed = client.eval(
                    RENEW_SCRIPT, 3, STATES, TOKENS, LEASES,
                    self.job_id, self.token, int(time.time() * 1000) + LEASE_MS,
                )
                if int(renewed) != 1:
                    self.lease_lost.set()
                    break
                last_success = time.monotonic()
                backoff = max(1.0, LEASE_MS / 3000)
            except Exception:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
                client = None
                if (time.monotonic() - last_success) * 1000 >= LEASE_MS:
                    self.lease_lost.set()
                    break
                backoff = min(5.0, backoff * 2)
        if client is not None:
            client.close()

    def assert_owned(self):
        if self.lease_lost.is_set():
            raise RuntimeError(f'LINK_LEASE_LOST:{self.job_id}')

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop_event.set()
        self.thread.join(timeout=5)
        self.assert_owned()


def start_worker_heartbeat(redis_factory, stop_event: threading.Event) -> threading.Thread:
    def run():
        client = None
        while not stop_event.is_set():
            try:
                if client is None:
                    client = redis_factory()
                client.set(WORKER_HEARTBEAT, str(int(time.time())), ex=45)
                stop_event.wait(10)
            except Exception:
                if client is not None:
                    client.close()
                client = None
                stop_event.wait(2)
        if client is not None:
            client.close()

    thread = threading.Thread(target=run, name='link-worker-heartbeat', daemon=True)
    thread.start()
    return thread
