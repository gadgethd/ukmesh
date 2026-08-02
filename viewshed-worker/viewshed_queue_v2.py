"""Crash-safe Redis protocol for ordinary and planned coverage jobs."""

import json
import os
import secrets
import threading
import time

READY = 'meshcore:viewshed:v2:ready'
PAYLOADS = 'meshcore:viewshed:v2:payloads'
STATES = 'meshcore:viewshed:v2:states'
ATTEMPTS = 'meshcore:viewshed:v2:attempts'
BYTES = 'meshcore:viewshed:v2:bytes'
PENDING = 'meshcore:viewshed_pending'
TOKENS = 'meshcore:viewshed:v2:tokens'
LEASES = 'meshcore:viewshed:v2:leases'
DEAD = 'meshcore:viewshed:v2:dead'
DEAD_REASONS = 'meshcore:viewshed:v2:dead_reasons'
COUNTERS = 'meshcore:viewshed:v2:counters'
EVENTS = 'meshcore:viewshed:v2:events'
ENQUEUED = 'meshcore:viewshed:v2:enqueued'
DIRTY = 'meshcore:viewshed:v2:dirty'

MAX_JOBS = max(1, min(10_000, int(os.environ.get('VIEWSHED_QUEUE_MAX', '1000'))))
MAX_BYTES = max(
    64 * 1024,
    min(256 * 1024 * 1024, int(os.environ.get('VIEWSHED_QUEUE_MAX_BYTES', str(16 * 1024 * 1024)))),
)
MAX_PAYLOAD_BYTES = max(
    256,
    min(64 * 1024, int(os.environ.get('VIEWSHED_MAX_PAYLOAD_BYTES', str(4 * 1024)))),
)
MAX_ATTEMPTS = max(1, min(20, int(os.environ.get('VIEWSHED_QUEUE_MAX_ATTEMPTS', '5'))))
LEASE_MS = max(30_000, min(60 * 60_000, int(os.environ.get('VIEWSHED_QUEUE_LEASE_MS', '900000'))))
DEAD_MAX_JOBS = max(1, min(1_000, int(os.environ.get('VIEWSHED_QUEUE_DEAD_MAX_JOBS', '100'))))
DEAD_MAX_BYTES = max(
    1,
    min(64 * 1024 * 1024, int(os.environ.get('VIEWSHED_QUEUE_DEAD_MAX_BYTES', str(16 * 1024 * 1024)))),
)
DEAD_RETENTION_MS = max(
    60_000,
    int(os.environ.get('VIEWSHED_QUEUE_DEAD_RETENTION_MS', str(30 * 24 * 60 * 60_000))),
)

ADMIT_SCRIPT = """
local payload_bytes = tonumber(ARGV[3])
if payload_bytes > tonumber(ARGV[6]) then return {'oversized', ''} end
local state = redis.call('HGET', KEYS[3], ARGV[1])
if state == 'queued' or state == 'in_flight' then
  local old_bytes = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
  local total_bytes = tonumber(redis.call('HGET', KEYS[7], 'bytes') or '0')
  local next_bytes = total_bytes - old_bytes + payload_bytes
  if next_bytes > tonumber(ARGV[5]) then return {'full', ''} end
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
  redis.call('HSET', KEYS[5], ARGV[1], payload_bytes)
  redis.call('HSET', KEYS[7], 'bytes', next_bytes)
  if state == 'in_flight' then redis.call('SADD', KEYS[10], ARGV[1]) end
  return {'coalesced', ARGV[1]}
end
if state == 'dead' then return {'dead', ARGV[1]} end
local count = tonumber(redis.call('HGET', KEYS[7], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[7], 'bytes') or '0')
if count + 1 > tonumber(ARGV[4]) or bytes + payload_bytes > tonumber(ARGV[5]) then
  return {'full', ''}
end
redis.call('SADD', KEYS[6], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], 'queued')
redis.call('HSET', KEYS[4], ARGV[1], '0')
redis.call('HSET', KEYS[5], ARGV[1], payload_bytes)
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[7], 'count', count + 1, 'bytes', bytes + payload_bytes)
redis.call('ZADD', KEYS[9], ARGV[7], ARGV[1])
redis.call('LPUSH', KEYS[8], 'admit')
redis.call('LTRIM', KEYS[8], 0, 255)
return {'accepted', ARGV[1]}
"""

CLAIM_SCRIPT = """
while true do
  local job_id = redis.call('RPOP', KEYS[1])
  if not job_id then return nil end
  if redis.call('HGET', KEYS[3], job_id) == 'queued' then
    redis.call('HSET', KEYS[3], job_id, 'in_flight')
    local attempt = redis.call('HINCRBY', KEYS[4], job_id, 1)
    redis.call('HSET', KEYS[5], job_id, ARGV[1])
    redis.call('ZADD', KEYS[6], ARGV[2], job_id)
    redis.call('LPUSH', KEYS[7], 'claim')
    redis.call('LTRIM', KEYS[7], 0, 255)
    return {job_id, redis.call('HGET', KEYS[2], job_id) or '', attempt}
  end
end
"""

ACK_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'in_flight'
   or redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
if redis.call('SREM', KEYS[13], ARGV[1]) == 1 then
  redis.call('HSET', KEYS[1], ARGV[1], 'queued')
  redis.call('LPUSH', KEYS[12], ARGV[1])
  redis.call('LPUSH', KEYS[10], 'ack_requeue')
  redis.call('LTRIM', KEYS[10], 0, 255)
  return 2
end
local payload_bytes = tonumber(redis.call('HGET', KEYS[6], ARGV[1]) or '0')
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('HDEL', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('SREM', KEYS[7], ARGV[1])
redis.call('ZREM', KEYS[9], ARGV[1])
redis.call('ZREM', KEYS[11], ARGV[1])
local count = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'count') or '0') - 1)
local bytes = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'bytes') or '0') - payload_bytes)
redis.call('HSET', KEYS[8], 'count', count, 'bytes', bytes)
redis.call('LPUSH', KEYS[10], 'ack')
redis.call('LTRIM', KEYS[10], 0, 255)
return 1
"""

NACK_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'in_flight'
   or redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 'invalid'
end
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[13], ARGV[1])
local attempts = tonumber(redis.call('HGET', KEYS[5], ARGV[1]) or '0')
if ARGV[7] == '1' then attempts = tonumber(ARGV[3]) end
if attempts >= tonumber(ARGV[3]) then
  local payload_bytes = tonumber(redis.call('HGET', KEYS[6], ARGV[1]) or '0')
  local active_count = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'count') or '0') - 1)
  local active_bytes = math.max(0, tonumber(redis.call('HGET', KEYS[8], 'bytes') or '0') - payload_bytes)
  local dead_count = tonumber(redis.call('HGET', KEYS[8], 'dead_count') or '0')
  local dead_bytes = tonumber(redis.call('HGET', KEYS[8], 'dead_bytes') or '0')
  redis.call('HSET', KEYS[8], 'count', active_count, 'bytes', active_bytes)
  redis.call('ZREM', KEYS[11], ARGV[1])
  if dead_count + 1 > tonumber(ARGV[4]) or dead_bytes + payload_bytes > tonumber(ARGV[5]) then
    redis.call('HDEL', KEYS[1], ARGV[1])
    redis.call('HDEL', KEYS[12], ARGV[1])
    redis.call('HDEL', KEYS[5], ARGV[1])
    redis.call('HDEL', KEYS[6], ARGV[1])
    redis.call('HDEL', KEYS[14], ARGV[1])
    redis.call('SREM', KEYS[7], ARGV[1])
    redis.call('LPUSH', KEYS[9], 'dead_purged')
    redis.call('LTRIM', KEYS[9], 0, 255)
    return 'purged'
  end
  redis.call('HSET', KEYS[1], ARGV[1], 'dead')
  redis.call('ZADD', KEYS[10], ARGV[6], ARGV[1])
  redis.call('HSET', KEYS[14], ARGV[1], ARGV[8])
  redis.call('HSET', KEYS[8],
    'dead_count', dead_count + 1,
    'dead_bytes', dead_bytes + payload_bytes)
  redis.call('LPUSH', KEYS[9], 'dead')
  redis.call('LTRIM', KEYS[9], 0, 255)
  return 'dead'
end
redis.call('HSET', KEYS[1], ARGV[1], 'queued')
redis.call('LPUSH', KEYS[4], ARGV[1])
redis.call('LPUSH', KEYS[9], 'retry')
redis.call('LTRIM', KEYS[9], 0, 255)
return 'retry'
"""

RENEW_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'in_flight'
   or redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return 1
"""

VERIFY_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'in_flight' then return 0 end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
local expiry = redis.call('ZSCORE', KEYS[3], ARGV[1])
if not expiry or tonumber(expiry) <= tonumber(ARGV[3]) then return 0 end
return 1
"""

REAP_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local recovered = 0
for _, job_id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], job_id)
  if redis.call('HGET', KEYS[2], job_id) == 'in_flight' then
    redis.call('HSET', KEYS[2], job_id, 'queued')
    redis.call('HDEL', KEYS[3], job_id)
    redis.call('SREM', KEYS[6], job_id)
    redis.call('LPUSH', KEYS[4], job_id)
    recovered = recovered + 1
  end
end
if recovered > 0 then
  redis.call('LPUSH', KEYS[5], 'reap')
  redis.call('LTRIM', KEYS[5], 0, 255)
end
return recovered
"""

PURGE_DEAD_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'dead' then return 0 end
local payload_bytes = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('HDEL', KEYS[5], ARGV[1])
redis.call('SREM', KEYS[6], ARGV[1])
redis.call('HDEL', KEYS[8], ARGV[1])
local dead_count = math.max(0, tonumber(redis.call('HGET', KEYS[7], 'dead_count') or '0') - 1)
local dead_bytes = math.max(0, tonumber(redis.call('HGET', KEYS[7], 'dead_bytes') or '0') - payload_bytes)
redis.call('HSET', KEYS[7], 'dead_count', dead_count, 'dead_bytes', dead_bytes)
return 1
"""

REQUEUE_DEAD_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= 'dead' then return 0 end
local payload_bytes = tonumber(redis.call('HGET', KEYS[4], ARGV[1]) or '0')
local count = tonumber(redis.call('HGET', KEYS[6], 'count') or '0')
local bytes = tonumber(redis.call('HGET', KEYS[6], 'bytes') or '0')
if count + 1 > tonumber(ARGV[2]) or bytes + payload_bytes > tonumber(ARGV[3]) then return -1 end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[8], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], 'queued')
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


def admit(client, job: dict) -> tuple[str, str | None]:
    job_id = str(job.get('node_id') or '').strip()
    if not job_id:
        return 'invalid', None
    payload = json.dumps(job, separators=(',', ':'), sort_keys=True)
    payload_bytes = len(payload.encode('utf-8'))
    result = client.eval(
        ADMIT_SCRIPT,
        10,
        READY,
        PAYLOADS,
        STATES,
        ATTEMPTS,
        BYTES,
        PENDING,
        COUNTERS,
        EVENTS,
        ENQUEUED,
        DIRTY,
        job_id,
        payload,
        payload_bytes,
        MAX_JOBS,
        MAX_BYTES,
        MAX_PAYLOAD_BYTES,
        int(time.time() * 1000),
    )
    status = str(result[0])
    accepted_id = str(result[1]) if result[1] else None
    return status, accepted_id


def claim(client):
    token = secrets.token_hex(16)
    result = client.eval(
        CLAIM_SCRIPT,
        7,
        READY,
        PAYLOADS,
        STATES,
        ATTEMPTS,
        TOKENS,
        LEASES,
        EVENTS,
        token,
        int(time.time() * 1000) + LEASE_MS,
    )
    if not result:
        return None
    payload = json.loads(result[1])
    if not isinstance(payload, dict):
        raise ValueError('viewshed queue payload must be an object')
    return str(result[0]), token, payload, int(result[2])


def ack(client, job_id: str, token: str) -> str:
    result = int(client.eval(
        ACK_SCRIPT,
        13,
        STATES,
        TOKENS,
        LEASES,
        PAYLOADS,
        ATTEMPTS,
        BYTES,
        PENDING,
        COUNTERS,
        DEAD,
        EVENTS,
        ENQUEUED,
        READY,
        DIRTY,
        job_id,
        token,
    ))
    return 'complete' if result == 1 else 'requeued' if result == 2 else 'invalid'


def nack(
    client,
    job_id: str,
    token: str,
    *,
    permanent: bool = False,
    reason: str = 'attempt_limit_exceeded',
) -> str:
    return str(client.eval(
        NACK_SCRIPT,
        14,
        STATES,
        TOKENS,
        LEASES,
        READY,
        ATTEMPTS,
        BYTES,
        PENDING,
        COUNTERS,
        EVENTS,
        DEAD,
        ENQUEUED,
        PAYLOADS,
        DIRTY,
        DEAD_REASONS,
        job_id,
        token,
        MAX_ATTEMPTS,
        DEAD_MAX_JOBS,
        DEAD_MAX_BYTES,
        int(time.time() * 1000),
        '1' if permanent else '0',
        str(reason)[:100],
    ))


def owns_lease(client, job_id: str, token: str) -> bool:
    return int(client.eval(
        VERIFY_SCRIPT,
        3,
        STATES,
        TOKENS,
        LEASES,
        job_id,
        token,
        int(time.time() * 1000),
    )) == 1


def reap(client, limit: int = 100) -> int:
    return int(client.eval(
        REAP_SCRIPT,
        6,
        LEASES,
        STATES,
        TOKENS,
        READY,
        EVENTS,
        DIRTY,
        int(time.time() * 1000),
        max(1, min(1_000, limit)),
    ))


def purge_dead(client, job_id: str) -> bool:
    return int(client.eval(
        PURGE_DEAD_SCRIPT,
        8,
        STATES,
        DEAD,
        PAYLOADS,
        BYTES,
        ATTEMPTS,
        PENDING,
        COUNTERS,
        DEAD_REASONS,
        job_id,
    )) == 1


def requeue_dead(client, job_id: str, max_jobs: int, max_bytes: int) -> str:
    result = int(client.eval(
        REQUEUE_DEAD_SCRIPT,
        8,
        STATES,
        DEAD,
        ATTEMPTS,
        BYTES,
        READY,
        COUNTERS,
        ENQUEUED,
        DEAD_REASONS,
        job_id,
        max_jobs,
        max_bytes,
        int(time.time() * 1000),
    ))
    return 'requeued' if result == 1 else 'full' if result == -1 else 'not_found'


def cleanup_dead(client, limit: int = 100) -> int:
    cutoff = int(time.time() * 1000) - DEAD_RETENTION_MS
    expired = client.zrangebyscore(DEAD, '-inf', cutoff, start=0, num=max(1, min(1_000, limit)))
    return sum(1 for job_id in expired if purge_dead(client, str(job_id)))


class LeaseRenewer:
    def __init__(self, redis_factory, job_id: str, token: str):
        self.redis_factory = redis_factory
        self.job_id = job_id
        self.token = token
        self.stop_event = threading.Event()
        self.lease_lost = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f'viewshed-lease-{job_id[:8]}', daemon=True)

    def _run(self):
        client = None
        last_success = time.monotonic()
        backoff = 0.25
        interval = max(0.25, LEASE_MS / 3000)
        while not self.stop_event.wait(min(backoff, interval)):
            try:
                if client is None:
                    client = self.redis_factory()
                renewed = client.eval(
                    RENEW_SCRIPT,
                    3,
                    STATES,
                    TOKENS,
                    LEASES,
                    self.job_id,
                    self.token,
                    int(time.time() * 1000) + LEASE_MS,
                )
                if int(renewed) != 1:
                    self.lease_lost.set()
                    break
                last_success = time.monotonic()
                backoff = interval
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
            raise RuntimeError(f'VIEWSHED_LEASE_LOST:{self.job_id}')

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop_event.set()
        self.thread.join(timeout=5)
        self.assert_owned()
