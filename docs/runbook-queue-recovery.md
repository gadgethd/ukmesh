# Queue recovery runbook

The operator dashboard at `/operations` is the primary recovery interface. It
shows queue capacity, ready/deferred/leased counts, heartbeat, oldest age,
attempts, retained dead jobs and bounded reasons. Every mutation is local-only,
CSRF protected, idempotent, and audited.

## Diagnose

1. Open `/operations` through the documented localhost tunnel.
2. Identify `viewshed` or `link-v3`.
3. Check worker heartbeat, oldest age, active jobs/bytes, leases, attempts, and
   the dead-letter reason.
4. Inspect the worker logs without dumping payloads:

```bash
docker compose logs --since=30m viewshed-worker
docker compose logs --since=30m link-worker
docker compose exec -T backend wget -qO- http://127.0.0.1:9091/metrics \
  | grep -E '^meshcore_(viewshed|link)_queue_|^meshcore_worker_heartbeat'
```

Correct a missing dependency, unavailable terrain source, bad database
connection, or stopped worker before requeueing. An invalid job should normally
be purged, not retried.

## Requeue or purge one dead job

Use the single-job controls in `/operations`. Requeue is capacity checked and
retains attempt history. Purge requires the exact confirmation
`PURGE JOB_ID`. Repeating the same request with its idempotency key returns the
recorded result without applying it twice.

The container CLI is an emergency alternative:

```bash
docker compose run --rm --no-deps viewshed-worker \
  python3 queue_admin.py requeue-dead JOB_ID
docker compose run --rm --no-deps viewshed-worker \
  python3 queue_admin.py purge-dead JOB_ID
docker compose run --rm --no-deps viewshed-worker \
  python3 queue_admin.py requeue-coverage-dead JOB_ID
docker compose run --rm --no-deps viewshed-worker \
  python3 queue_admin.py purge-coverage-dead JOB_ID
```

Record emergency CLI use separately because the UI/API path provides the
stronger database audit trail.

## Repair queue counters

Counter repair is not a purge and does not recreate missing payloads. First run
the read-only audit:

```bash
docker compose run --rm --no-deps viewshed-worker \
  python3 queue_admin.py audit
```

If it reports drift, capture the output and use `/operations`, typing exactly
`REPAIR link-v3`. The operator mutation atomically recalculates retained
link-v3 queue counters and writes an audit event. The CLI fallback is:

```bash
docker compose run --rm --no-deps viewshed-worker \
  python3 queue_admin.py audit --repair
```

Run the read-only audit again. Do not edit Redis keys directly and do not use a
wildcard deletion. Bulk dead-letter purge is deliberately unsupported.

## Lease and analysis recovery

An unexpired lease belongs to its worker. Restarting a healthy worker to steal
it can duplicate compute; publication fencing prevents stale results but does
not make wasted work free. If heartbeat is absent, restart only the affected
worker and allow the bounded lease to expire. Analysis runs expose checkpoint,
attempt, lease owner/expiry, and terminal reason in `/operations`; recovery
must resume from the durable checkpoint.
