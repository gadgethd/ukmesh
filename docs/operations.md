# Operations

## Live host Compose overlay

The long-running production host shares its historical
`meshcore-analytics_default` bridge with separately managed beacon and Discord
services. Use `docker-compose.live.yml` for every production `config`, `up`,
`ps`, `exec`, and controlled replacement command:

```bash
docker compose -f docker-compose.yml -f docker-compose.live.yml \
  --profile dev --profile viewshed --profile tunnel config --quiet
```

The overlay treats that bridge as external and reserves
`172.18.30.10`–`172.18.30.30` for the analytics proxies and tunnel. This keeps
the backend trusted-proxy allowlist exact without attempting to delete or
renumber unrelated endpoints. It also publishes this stack's Prometheus on
`127.0.0.1:9092` because the host's independent isolated-network Prometheus
owns port 9090. Fresh, CI, and isolated restore environments must use only
`docker-compose.yml` and its dedicated `172.30.0.0/24` network.

## Browser operator access

The operator dashboard exchanges `OPERATOR_SITE_TOKEN` for a 30-minute,
in-memory browser session. The session cookie is always `HttpOnly`, `Secure`,
and `SameSite=Strict`; the token is not stored in browser storage. Login,
logout, expiry, and failed authentication are audited without logging the
token.

Use either the deployment's local HTTPS endpoint or an SSH tunnel that opens
the dashboard as `https://localhost`. Plain HTTP to a private IP is not a
supported operator transport and the server will not weaken the cookie to make
it work. Example tunnel:

```bash
ssh -L 8443:127.0.0.1:443 operator@mesh-host
```

Then open `https://localhost:8443/`, enter the operator token, and use the
dashboard normally. Logout requires the session CSRF token. Existing automation
may continue to send `Authorization: Bearer <OPERATOR_SITE_TOKEN>` for one
release; migrate it to a purpose-specific operator credential before that
compatibility path is removed.

## Automated checks

Pull requests and branch pushes run `.github/workflows/ci.yml`. The workflow:

- installs dependencies from lockfiles
- type-checks, tests, and builds the backend
- builds the frontend and runs Playwright desktop/mobile smoke tests
- compiles Python workers
- validates the Docker Compose model with required secrets represented by CI-only placeholders

Dependabot groups weekly dependency updates for the backend, frontend, and GitHub Actions.

## Clean bootstrap

Set every required value in `.env`, including the immutable
`HEALTHCHECK_SOURCE_REF` and Anubis signing key. Before starting Mosquitto, load
the environment and run `scripts/bootstrap-mosquitto.sh`. The helper creates a
backend-only read ACL and password file, refuses partial or symlinked state, and
does not replace an existing credential set.

## Legacy schema-16 compatibility cutover

An existing database that has `016_stale_mqtt_observer_cleanup.sql` but not
`016_private_prefixes.sql` deliberately fails closed. After a signed backup and
isolated restore preflight, run the one-time reviewed compatibility path:

```bash
docker compose run --rm \
  -e MIGRATION_016_PRIVATE_PREFIXES_APPROVAL=supersede-016-and-017-with-authoritative-privacy-and-026 \
  db-migrate
```

The runner requires the exact sibling migration and both replacement files,
records `superseded-existing` for the unsafe historical rewrites, and installs
the prefix schema/trigger replacement before a new backend starts.
Authoritative public predicates validate path framing and consult the current
private-prefix table for historical as well as new packets. This avoids
decompressing or rewriting Timescale history. It also does not rewrite every
historical packet merely to materialize a topic prefix; public query predicates
safely derive the prefix from `topic` when the legacy column is empty. Read back
the decision before continuing:

```sql
SELECT migration_name, disposition, replacement_name
FROM schema_migration_compatibility
WHERE migration_name = '016_private_prefixes.sql';
```

Never set this approval on a fresh database or for any other missing migration.

## Metrics

Prometheus scrapes the backend registry from `backend:9091/metrics` on the
internal Compose network. Port 9091 is deliberately not published by Compose or
proxied by Nginx; `/metrics` on a public website origin must return 404.

## Health endpoints

- `GET /healthz` is a process liveness check. It deliberately remains healthy during an MQTT outage so Docker does not restart an otherwise functioning API in a loop.
- `GET /readyz` checks database access and MQTT connectivity. It returns `503` with per-dependency state when the real-time platform is not ready.
- `GET /api/health` returns worker history and a top-level `status` (`healthy`, `degraded`, or `critical`) plus machine-readable `problems`.

Current problem codes cover stale public ingest, worker queue backlog, disk pressure, and frontend error spikes. External monitoring should alert on `readyz` failures and critical `/api/health` problems, while allowing a short deploy/reconnect grace period.

The `synthetic-monitor` service independently checks liveness, dependency readiness, the scoped stats API, and delivery of a WebSocket `initial_state`. Results and latency are retained for 14 days in `operational_check_results`. Three consecutive failures trigger a structured log alert and, when `ALERT_WEBHOOK_URL` is configured, a JSON webhook. A successful check after an alert emits a recovery notification.

## Load checks

Run bounded HTTP load locally with:

```bash
cd backend
npm run load:realtime -- --duration 30 --concurrency 25 --max-p95-ms 1500
```

Use `--mqtt-messages 5000` to exercise the bounded MQTT ingest queue with isolated, rejected test envelopes, and `--slow-ws-clients 10` to hold non-reading WebSocket clients during the run. MQTT mode uses `MQTT_BROKER_URL`, `MQTT_USERNAME`, and `MQTT_PASSWORD`; never point it at a broker outside the deployment under test.

## Statistics aggregate rollout

`packet_hourly_stats` is maintained atomically with accepted packet batches.
Populate historical hours with a resumable, one-hour-per-transaction tool:

```bash
docker compose exec backend node dist/tools/backfillStatsRollups.js --hourly-days 8
docker compose exec backend node dist/tools/backfillStatsRollups.js --apply --hourly-days 8
```

The first command is a read-only inventory. The checkpoint and the rollup
slice commit together, so interruption resumes without double-counting. Set
`STATS_AGGREGATE_READS_ENABLED=true` only after backfill. Once the
aggregate-writing backend is live, reconcile the partial cutover hour:

```bash
docker compose exec backend node dist/tools/backfillStatsRollups.js \
  --apply --catch-up-current-hour
```

The reconciliation closes the bounded gap between the completed historical
checkpoint and now one hour at a time, including the current partial hour. Each
slice takes a writer lock with a five-second lock timeout and fails closed; a
gap above 48 hours requires a newly reviewed historical backfill. Transactions
that arrive during a rebuild increment the reconstructed rows after its lock is
released.

A short validation window may additionally set
`STATS_AGGREGATE_SHADOW_ENABLED=true`. The comparison pins one cutoff, scans
the recent source rows once, and runs off the response path with one in-flight
comparison per scope and a five-minute minimum interval. Dimension keys must
match exactly. Count differences may be at most five packets or 0.1%,
whichever is larger, to allow a transaction that commits between the two
snapshots. Treat any `[stats-aggregate-shadow] mismatch` or
`[stats-aggregate-shadow] failed` as a failed rollout and turn aggregate reads
back off. Shadow mode deliberately adds a raw validation scan and must not
remain enabled.

Production aggregate reads combine maintained full-hour rows with raw boundary
fragments, preserving the legacy exact rolling 24-hour/seven-day windows while
keeping raw work bounded to partial hours.

Canonical chart responses are also stored in `stats_chart_snapshots` after
completeness, scope, age, privacy-generation, and 2 MiB payload checks. Schema
version 2 snapshots survive a backend restart: a fresh snapshot is returned
directly, and a stale snapshot remains available for up to six hours while one
refresh runs in the background. Newer snapshots cannot be overwritten by an
older concurrent refresh. A public/private visibility change immediately
invalidates both memory and durable entries, and a generation publication fence
prevents an in-flight old computation from being cached. Observer-filtered chart
responses are never persisted.

For the first migration to durable snapshots, capture a current successful
canonical response from the still-running release and pipe it to the candidate
backend tool after migration 031 has been applied:

```bash
npm run stats:seed-chart-snapshot -- --network ukmesh < verified-ukmesh-charts.json
```

The tool reads JSON only from stdin and rejects incomplete, cross-scope,
future-dated, more-than-six-hour-old, oversized, or pre-visibility-change input.
It binds the response to the current generation and fails if that generation
changes during publication. Verify the stored `scope_key`, schema version,
visibility generation, `generated_at`, and payload byte count before restarting
the serving backend. Do not seed an observer-filtered response.

## Coverage API safety

The legacy unbounded `GET /api/coverage` response has been replaced by a bounded viewport API:

```text
GET /api/coverage?bbox=minLon,minLat,maxLon,maxLat&limit=12&cursor=<node-id>
```

- `bbox` is required and may span at most 20 degrees on either axis.
- `limit` defaults to 12 and is capped at 25.
- Each serialized page has a 5 MiB safety budget.
- A single geometry exceeding that budget is represented by `{node_id, truncated: true}` and can be retrieved through the per-node endpoint.
- `page.nextCursor` continues a partial result when `page.hasMore` is true.
- `GET /api/coverage/:nodeId` remains the preferred interactive-map endpoint.

## Topology API

`GET /api/topology?network=ukmesh&limit=300` returns recent viable repeater relationships, bounded to 500 links and a 30-day observation window. It reports connected components, recently active isolated repeaters, and articulation points labelled as likely bridges because the bounded graph is evidence rather than a complete routing model. Private-node names and positions pass through the same redaction policy used by the node APIs.

## Public API and exports

`GET /api/v1` is the stable discovery endpoint and links to `/api/v1/openapi.yaml`. Positioned nodes can be exported from `/api/v1/exports/nodes.csv` and `/api/v1/exports/nodes.geojson`; both formats are read-only, rate-limited, capped at 5,000 rows, network-scoped, and use the public-node redaction policy.

`GET /api/activity/timeline` is bounded to a 24-hour window and 250 active node IDs per bucket. `GET /api/rf-validation` compares stored terrain-model viability with recent observed evidence and distinguishes likely mismatches, weak evidence, and explicit operator overrides.

## Public status and maintenance

The public `/health` page polls the aggregated `/api/health` contract every minute. Set `MAINTENANCE_ACTIVE=1` and `MAINTENANCE_MESSAGE` during a planned window; the API and page show the notice without leaking hostnames, credentials, addresses, or private node identities.

## Operational runbooks

- [Immutable release and rollback](runbook-release-rollback.md)
- [Encrypted backup, isolated restore, and disaster recovery](runbook-backup-restore.md)
- [Bounded queue and dead-letter recovery](runbook-queue-recovery.md)
- [SRTM cache, alert delivery, observer review, and planned publication](runbook-srtm-alerts-observers.md)
- [Database compression and retention gates](db-lifecycle.md)

Use the `/operations`, `/observer-registrations`, and operator audit pages for
normal mutations. They enforce authorization, CSRF, typed confirmation,
idempotency, capacity, and audit rules that direct Redis/SQL edits bypass.

## Alert first response

Start every alert response by recording the firing time, checking Prometheus
target state, and preserving the relevant ten-minute logs. Acknowledge only
after assigning an operator. The following headings are stable targets for the
Prometheus `runbook_url` annotations.

### BackendDown

Check `docker compose ps backend`, then backend logs and the container OOM/exit
state. If the process is stopped, restart only `backend`; if it repeatedly
fails, keep ingest stopped and roll back to the prior signed digest.

### BackendNotReady

Read `/readyz` and act on the named dependency. Check PostgreSQL, Redis, and
Mosquitto before restarting the API. Liveness remaining green is expected
during a dependency outage.

### SyntheticProbeFailed

Compare the failing blackbox target with direct localhost liveness, stats, and
WebSocket checks. If localhost succeeds, inspect Nginx, Anubis, Cloudflare and
DNS in that order; do not restart the database.

### DependencyExporterDown

Check the named exporter and its dependency independently. An exporter-only
failure loses visibility but does not prove PostgreSQL or Redis is down.
Restore telemetry, then verify the underlying dependency metric.

### MosquittoUnavailable

Check broker health, its WebSocket listener, password/ACL file permissions and
the reloader logs. Do not regenerate credentials over an existing set. If ACL
reconciliation caused the alert, keep the last verified ACL and roll back that
change.

### MeshIngestSilent

Check backend MQTT readiness, broker connected-client events, public topic
rates, and whether this is a genuine quiet period. Compare independent
observers before changing credentials or subscriptions.

### QueueAgeHigh

Open `/operations`, identify the oldest queue and heartbeat, then follow the
queue recovery runbook. Fix the worker/dependency cause before requeueing a
single retained dead job.

### QueueCapacityHigh

Disable the producer feature or planned-coverage admission if growth
continues. Confirm the worker is consuming and the byte/job counters are
consistent. Never make Redis eviction-based or raise the cap without measuring
host memory.

### DeadJobsPresent

Inspect each bounded reason and attempt count in `/operations`. Requeue only
transient failures after the cause is fixed; purge one permanently invalid job
with the exact confirmation string.

### AnalysisLeaseLost

Preserve both worker logs and the analysis run record. The publication fence
prevents a stale writer from committing. Let the current lease expire, resume
from its checkpoint, and investigate clock, database, or long-step latency.

### HealthWorkerHeartbeatStale

Check the `health-worker` container, database reachability, and its last
successful snapshot. Restart only that worker; public health may be stale but
the API should remain available.

### WorkerMetricsDown

Identify the `role` label, inspect that worker's health endpoint and container
state, and restart only the affected worker. If work is queued, treat a stale
heartbeat as the higher-priority symptom.

### ActiveQueueWorkerHeartbeatStale

Stop new optional work, confirm the queue retains its leased payloads, and
restart the affected link/viewshed worker. Allow lease recovery to requeue;
do not manually duplicate the job.

### DatabaseUnavailable

Check TimescaleDB container state, disk, memory, and recent logs. Stop writers
if storage errors appear. Never recreate or delete the volume; escalate to the
signed backup/restore runbook.

If a scheduled Timescale policy logs `failed to start a background worker`,
compare `SHOW max_worker_processes` with
`SHOW timescaledb.max_background_workers`. The Compose default reserves 24
PostgreSQL worker slots for the image's current 16-worker Timescale setting,
its launcher/database schedulers, and logical-replication headroom. Correct the
configuration and restart only TimescaleDB; then require the policy's next
manual or scheduled run to succeed before closing the incident.

### DatabasePoolSaturated

Inspect waiting/active connection metrics, slow route latency and PostgreSQL
activity. Shed optional analysis/load, identify the bounded slow query, and
avoid increasing pool size until connection and memory headroom are measured.

### DatabaseWalRateHigh

Identify current maintenance, backfill, compression or ingest activity. Pause
the optional writer and check replica/backup capacity. A short known migration
burst may be observed; sustained unexplained WAL is an incident.

### HostDiskSpaceLow

Stop optional RF/backfill work and identify the mount from the alert. Preserve
databases and signed backups. Prune only explicitly identified unused build
cache/images or let the bounded SRTM/log policies converge; never run a broad
volume prune.

### BackupReceiptMissing

Check the receipt mount, signature/key permissions, and backup job result.
Create a new encrypted backup and complete an isolated restore drill. Do not
enable destructive retention while evidence is missing.

### BackupStale

Run the backup immediately, verify the signature/checksum, and perform the
isolated drill. Investigate target capacity or scheduling after a fresh
verified receipt restores the recovery guarantee.

### TelemetryIntegrityFailures

Inspect aggregate rejection reasons and a bounded redacted sample. Treat
private/cross-network leakage as an immediate rollback condition. Otherwise
identify the malformed observer/client version without weakening validation.
