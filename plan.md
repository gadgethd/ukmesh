# MeshCore Analytics: codebase audit and implementation plan

Status: implemented; final runtime evidence is bound by the signed deployment
receipt described below

Audit date: 2026-07-29

Audited revision: `b9b2ecdbfaa9688a79a62ccee38d6b406f14deaa`

Repository: `https://github.com/gadgethd/ukmesh.git`

Scope: all 366 Git-tracked files; no project source was changed during the audit

## Implementation outcome

The original read-only audit and its acceptance criteria remain below as the
release contract. The implementation completed every phase without enabling
irreversible packet retention/deletion: those policies intentionally remain
disabled behind the restore, inventory, dry-run, and explicit operator-approval
gates required by Sections 4, 4.4, and 7.3.

| Phase | Implemented evidence |
| --- | --- |
| 0 | Clean CI installs, pinned builds, worker image tests, deterministic Mosquitto bootstrap, migration checksum/lock tests, internal metrics, performance budgets, and signed rollback/restore foundations. |
| 1 | `docs/security-closure.md` maps every finding to the shared visibility, network, webhook, proxy, telemetry, queue, container, and coordinate controls plus their regression tests. |
| 2 | One registered map route, complete cursor delivery, lease-fenced analysis runs, coordinated shutdown, shared bounded input/error handling, and CSRF-protected secure operator sessions. |
| 3 | Explicit terrain outcomes, bounded versioned Redis state machines, database publication fences, recoverable ML checkpoints/leadership, and RF golden/latency/RSS tests in the exact worker images. |
| 4 | Immutable additive migrations, coalesced packet writes, resumable stats rollups with shadow comparison, privacy-versioned durable chart snapshots, gated lifecycle tooling, cache policy enforcement, and accurate public/operator health DTOs. |
| 5 | Privacy-safe inferred nodes and packet arcs, authoritative epoch-scoped stores, corrected Feed connectivity/path state, and repaired repeater/detail/Stats behavior. |
| 6 | Bounded visibility-aware polling/caches, terrain and service-worker lifecycle controls, throttled topology rendering, accessible UI primitives, route-wide axe/keyboard/mobile tests, component extraction, and enforced bundle/CSS budgets. |
| 7 | Low-cardinality metrics, Alloy/Prometheus/Alertmanager validation, encrypted isolated restore drills, signed immutable release/rollback controls, digest/hash dependency pins, SBOM/scan/signing workflow, and fail-closed single-chunk maintenance. |
| 8 | Owner alert delivery history/test actions, observer review states, audited queue/model/planned-publication operations, actionable scoped watchlists, wildcard 404/shared route metadata, runtime feature kill switches, complete generated OpenAPI coverage, and corrected architecture/runbooks. |

The full pre-deployment matrix is rerun from clean lockfile installs. At the
implemented source state it comprises 231 backend unit tests plus isolated
PostgreSQL/Redis integration fixtures, 35 frontend unit tests, 45 Playwright
desktop/mobile/accessibility tests with retries disabled, 21 RF/viewshed worker
tests, four ML worker tests, operational recovery drills, configuration
validators, and dependency policy checks.

The authoritative production result is the newest verified
`/home/ben/meshcore-releases/local-deploy-*.json` receipt and detached
signature. It binds the complete worktree fingerprint to exact image IDs,
schema/migration state, the signed backup and rollback evidence, live privacy
and proxy probes, monitoring state, performance measurements, and the
production soak. A failed required check means this status is not satisfied
and requires a corrected rebuild or the recorded compatible rollback.

## 1. Executive summary

MeshCore Analytics is a single-host, containerized analytics platform for
MeshCore radio networks. Authenticated MQTT observers forward radio envelopes;
the Node/TypeScript backend validates, decodes, deduplicates, classifies,
privacy-filters, persists, and broadcasts them. PostgreSQL/TimescaleDB is the
authoritative store, Redis carries queues and realtime fanout, Python workers
perform terrain/RF/link calculations, and a React/MapLibre application provides
the live dashboard, public UK pages, health/status views, and an authenticated
owner portal. Nginx, Anubis, and Cloudflare form the intended public edge;
Prometheus, Grafana, Loki, and the currently deployed Promtail collector form
the operations stack; Phase 7 replaces the now-EOL collector.

The project has a strong functional base: the backend type-checks and all 164
backend tests pass; the frontend type-checks, has a build/E2E pipeline, and its
two unit tests pass; configuration is generally coherent; privacy and queue
abstractions already exist in several paths. The principal risk is not a lack
of features but inconsistent application of those abstractions. Similar routes, queues,
caches, proxy locations, and worker states often implement subtly different
rules. That has produced several user-visible defects, seven validated security
findings, recoverability gaps, and avoidable database/Redis/browser work.

No P0/critical issue was established in this static audit. The recommended
sequence is:

1. Make CI and regression fixtures capable of proving the intended contracts.
2. Close security/privacy and host-boundary gaps.
3. Repair correctness, lifecycle, queue, and worker recovery.
4. Reduce database, RF, Redis, network, and browser work.
5. Complete broken product features and accessibility.
6. Add observability, backup/restore, immutable release controls, and accurate
   documentation.

The plan deliberately uses additive migrations, dual-read/dual-protocol
transitions, feature flags for risky UI/compute changes, and rollback to known
image versions. It does not rely on destructive down-migrations.

## 2. Intended system and data flow

```text
MeshCore radios
    |
    v
authenticated observers -> Mosquitto MQTT
                              |
                              v
                    backend MQTT ingestion
              validate -> decode -> dedupe -> privacy/spam
                    |                    |
                    v                    v
          TimescaleDB/PostgreSQL     Redis queues/fanout
                    |                    |
                    |             Node + Python workers
                    |          path/RF/viewshed/link/health
                    |                    |
                    +---------+----------+
                              |
                      REST + WebSocket API
                              |
                Cloudflare -> Anubis -> Nginx
                              |
               React dashboard and public UK sites
```

Important intended properties inferred from code and documentation:

- Network scope is currently ambiguous. Runtime and the dedicated test hostname
  support anonymous `ukmesh` and `test` network selection, while the security
  invariant supplied for this audit says public REST/WS should exclude
  test/development identities. The UK Feed also has a separate content-channel
  value named `test`; it is not the same dimension as network scope. An ADR must
  resolve both dimensions before scoping code changes. `all` is not an anonymous
  public scope under either interpretation.
- Names containing the private marker are an opt-out boundary. It must apply to
  direct rows, derived links/paths, caches, exports, WebSocket events, and
  historical views.
- Owner access is based on current grants from the separate owner-auth store,
  not merely on possession of a stale cookie.
- Raw packet history is currently retained indefinitely. This is an operating
  choice, not a safe default for an indefinitely growing public service.
- Planned coverage is an optional, resource-limited capability; ordinary link
  processing is expected to work in a fresh default deployment.
- Public health is meant to report service state, while operator detail and raw
  diagnostics belong behind a local/operator boundary.

## 3. Audit method and verified baseline

The audit partitioned the 366 tracked files into 194 backend files, 102
frontend files, and 70 deployment/worker/documentation files. Every tracked
path was inspected. Large lockfiles, generated dashboard JSON, geospatial
coordinates, and the two font binaries received targeted or machine-assisted
semantic review. Ignored dependencies, build output, runtime secrets, terrain
tiles, live databases, Redis state, Cloudflare configuration, and deployment
hosts were outside the repository inventory.

| Check | Result at audited revision |
| --- | --- |
| Backend `npm test` | 164/164 passed |
| Backend `npm run typecheck` | Passed |
| Backend production dependency audit | 0 vulnerabilities |
| Frontend `npm test` | 2/2 passed; coverage is far too small |
| Frontend `npx tsc --noEmit` | Passed |
| Frontend dependency audit | 3 high advisory entries; PostCSS is reachable only at build time and the React Router advisory is RSC-only, but CI still fails and dependencies should be upgraded |
| Python parsing | All 8 tracked Python files parsed |
| Shell syntax | All tracked shell scripts passed `bash -n` |
| JSON parsing | Passed |
| Docker Compose render | Passed with a complete placeholder environment |
| Security scan | Complete inventory; 7 reportable findings: 3 medium, 4 low |

The security report and sealed machine-readable evidence are under:

`/tmp/codex-security-scans/meshcore-analytics/b9b2ecdbfaa9688a79a62ccee38d6b406f14deaa_20260729T110834Z/`

The report is supplemental; this plan is self-contained because temporary
audit artifacts may not be retained.

## 4. Priorities and sequencing rules

| Priority | Meaning | Release expectation |
| --- | --- | --- |
| P0 | Active critical compromise, unrecoverable loss, or universal outage | Stop release and remediate immediately |
| P1 | Validated security boundary, data correctness, permanent wedge/loss, broken primary feature, or broken deployment gate | Address before broad feature work |
| P2 | Material performance, resilience, accessibility, observability, or maintainability risk | Schedule in the next engineering cycle |
| P3 | Product polish, documentation drift, or lower-risk cleanup | Bundle after foundations are stable |

Rules for the implementing agent:

- Write a failing regression test before each defect fix.
- Keep public visibility and network scoping in shared query/DTO helpers, not in
  each caller.
- Give every queue and long-running job a capacity, lease, retry, deadline,
  terminal state, cleanup operation, and metrics.
- Give every cache a scope key, TTL, entry/byte bound, invalidation rule, and
  observable eviction count.
- Use expand/migrate/contract database changes. Never renumber an applied
  migration; the duplicate `016` names need a recorded compatibility mapping.
- Deploy consumers/readers before producers/writers when changing protocols.
- Do not enable packet deletion until a restore drill and feature-window audit
  prove that rollups preserve advertised behavior.
- Treat privacy regression, data loss, and cross-scope state as automatic
  rollback conditions.

## 5. Decisions required before coding

Record these in a short ADR or `docs/architecture.md` before the dependent
phase starts.

| Decision | Recommended starting point | Affected work |
| --- | --- | --- |
| Network `test` and Feed channel `test` visibility | Resolve these as two independent dimensions. Recommended safe contract: production host defaults/locks to `ukmesh`; the dedicated test host may expose network `test` if approved; neither host permits anonymous `all`. Decide separately whether the Feed content-channel `test` is public. | SEC-16, SEC-09, BE-12, FE-04 |
| Raw packet retention | Choose a finite hot window after measuring product history requirements; compress cold data first, then retain aggregates longer than raw rows. | BE-13, OPS-03 |
| Owner webhook destinations | Prefer a dedicated egress proxy plus public-address validation; decide whether arbitrary public hosts are allowed or an allowlist is acceptable. | SEC-01, BE-17 |
| Operator authentication and transport | Short-lived HttpOnly `Secure`, `SameSite=Strict` session after local token login, served through local HTTPS or an SSH tunnel to HTTPS/localhost; retain bearer auth for automation for one release. Never weaken `Secure` merely to support private-IP HTTP. | BE-03 |
| Mosquitto ACL reload authority | Dedicated internal reconciler/helper with atomic ACL file and broker-local reload; no Docker socket in the public backend. | SEC-02 |
| Telemetry trust | Anonymous browser telemetry is always untrusted diagnostics, even with a build token or nonce. Authoritative health comes only from backend/synthetic signals; sampling, deduplication, and quotas protect diagnostic storage. | SEC-06, BE-14 |
| Planned/live RF service level | Benchmark first; then set job deadline, queue count/byte cap, retry budget, and acceptable path-loss/geometry tolerance. | SEC-03, WK-09, WK-10 |
| Backup objectives | Initial proposal: daily encrypted off-host backup with RPO 24h/RTO 4h, followed by WAL/PITR targeting RPO 15m if justified. | OPS-03 |
| Public planned-node data | Prefer owner/operator-only records plus a separate explicitly publishable DTO. Decide whether public submissions are a future feature. | BE-15 |
| Anonymous coordinate policy | Recommended: exact private-node, private-observer, and private-path coordinates must not enter an anonymous browser response or WebSocket event. Server DTOs redact first; client masking is defense in depth only. | SEC-13, FE-02, FE-03 |
| `/nodes/map` delivery contract | Choose a complete, bounded contract before deleting the duplicate route: viewport/bbox tiles or deterministic cursor iteration with explicit row/byte budgets and frontend completion semantics. Silent truncation is not acceptable. | BE-01, BE-12 |
| Embedded channel key | Confirm it is the documented public MeshCore channel material. If it is secret, rotate and perform history cleanup; do not merely move it to an environment variable. | SEC-12 |

## 6. Phase 0 — establish regression and release guardrails

Goal: make subsequent changes measurable and prevent known broken states from
shipping again. This phase may run in parallel with the small edge proxy fix in
Phase 1.

### 0.1 Repair dependency and test gates

Files:

- `.github/workflows/ci.yml`
- `.github/workflows/frontend-mobile.yml`
- `frontend/package.json`, `frontend/package-lock.json`
- `backend/package.json`
- new `viewshed-worker/tests/**`
- new `ml-path-learner/tests/**`

Implementation:

1. Upgrade PostCSS and React Router/React Router DOM to advisory-fixed compatible
   versions, regenerate the lockfile with `npm install`, run the complete
   frontend suite, and record why the old advisories were not runtime-exploitable
   in this client-only Vite build. Do not add a permanent audit exception when a
   compatible patch exists.
2. Add `npm test` to the frontend CI job before build/E2E.
3. Run viewshed/link/RF imports and `pytest` inside the built worker image so
   the test environment uses the exact matching system GDAL/`osgeo` stack.
   Test the ML worker in its image or a locked equivalent environment. Cover
   `worker.py`, `link_queue_v3.py`, `backfill_profiles.py`, RF modules, and the
   ML worker. Land known-red regression cases with their fixing slice, or mark
   them explicitly non-blocking and linked to their issue until that slice;
   Phase 0 itself must remain green.
4. Build every Dockerfile and relevant Compose profile in CI, then start a
   unique, empty-volume Compose project and wait for health/readiness.
5. Keep secret scanning and add generated SBOM/image scanning later in Phase 7;
   do not run non-reproducible network audits inside production image builds.

### 0.2 Make clean bootstrap deterministic

Files:

- `.github/workflows/ci.yml`
- `.env.example`
- `README.md`
- `mosquitto/mosquitto.conf`
- new `scripts/bootstrap-mosquitto.sh`
- `docs/operations.md`, `docs/contributing.md`

Implementation:

1. Supply every required CI variable, including
   `ANUBIS_ED25519_PRIVATE_KEY_HEX` in nightly load.
2. Create an idempotent bootstrap script that generates a backend Mosquitto
   password and least-privilege ACL before broker startup. Use restrictive
   permissions, back up existing files, and refuse destructive overwrite.
3. Update Quick Start so required secrets and bootstrap order match Compose.
4. Add a clean-checkout smoke job that proves no developer `.env`, ignored
   credential file, or existing Docker volume is required.

### 0.3 Capture contracts and performance baselines

Add an audit fixture or ADR containing:

- API/WS/export/cache visibility matrix that independently varies network
  (`ukmesh`, `test`, `all`) and Feed content channel (`public`, `test`), plus
  host, observer scope, owner scope, and private identities.
- Current response schemas for `/nodes/map`, packet detail, link history,
  planned nodes, health, and owner endpoints.
- Approved `/nodes/map` completeness strategy and numeric row/byte/request
  budgets, including the frontend behavior needed to finish a viewport or
  cursor sequence without showing a silently incomplete map.
- `EXPLAIN (ANALYZE, BUFFERS)` plus p50/p95 and row counts for map, summary,
  charts, coverage, topology, link history, and path resolution on a sanitized
  representative dataset.
- Ingest throughput, packet-batch latency, Redis queue depth/bytes, viewshed
  job duration/RSS, frontend bundle size, map initialization count, browser
  long tasks, and cache memory.
- Numeric Phase 6 release budgets: browser/terrain memory, service-worker
  storage, request/path concurrency, polling interval/deadline, topology node
  count/frame time, bundle/chunk size, and CSS duplication target.

Tests and acceptance:

- A clean checkout builds every image and reaches backend, broker, link worker,
  frontends, and monitoring health.
- Backend tests/typecheck/build, frontend tests/typecheck/build/Playwright, all
  Python tests/imports, Compose render, shell checks, and dependency policy
  pass in CI.
- The harness can express duplicate-route, private-scope, queue-invariant,
  transient-RF, and cross-network-store regressions. Known-red cases become
  blocking only in the slice that ships their fix.
- Baselines are versioned and later phases declare a maximum tolerated
  regression (default: no more than 10% without explicit review).

### 0.4 Land minimum production safety foundations

This subsection must complete before any new production migration or queue
protocol ships.

Files:

- `backend/src/db/migrations.ts`
- migration ledger schema and tests
- minimal backup/restore tooling and `docs/operations.md`
- `backend/src/metrics.ts`, internal metrics route, and core queue/lifecycle
  instrumentation
- release metadata workflow

Implementation:

1. Add a PostgreSQL advisory migration lock, immutable filename/checksum ledger,
   duplicate-prefix detection, and an explicit audited path for non-transactional
   operations such as `CREATE INDEX CONCURRENTLY`.
2. Pin the TimescaleDB image and extension version, take a current production
   snapshot, and demonstrate a minimal restore path before the first new
   migration. Full scheduling, encryption, PITR, and restore automation remain
   in Phase 7.
3. Record immutable image digests for the current and last known-good release.
   Signing/provenance can mature in Phase 7; early rollback instructions must
   say last known-good digest, not assume signatures already exist.
4. Serve the metrics registry on an internal-only endpoint and add the minimum
   metrics needed by Phases 1–3: queue count/bytes/age/rejections, webhook
   outcomes, lifecycle drain, worker heartbeat, lease loss, and job outcomes.
   Essential saturation/error alerts must ship with the queue protocol; richer
   exporters and dashboards remain in Phase 7.

Acceptance:

- Concurrent migration runners serialize and checksum drift fails closed.
- A pinned-version snapshot restores into an empty environment far enough to
  run migrations and application readiness.
- The prior compatible image digest is recorded and retrievable.
- Internal metrics scrape works, public access fails, and core labels pass a
  low-cardinality test.

Rollout/rollback:

- This phase changes CI, tests, bootstrap, and additive safety foundations only.
  Bootstrap must never replace existing credentials without explicit operator
  approval.
- Dependency upgrades are one isolated PR with a lockfile-only rollback path.

## 7. Phase 1 — contain security and privacy boundaries

Goal: close all seven validated security findings and the adjacent
defense-in-depth gaps before enabling more public or owner features.

### 1.1 Owner webhook SSRF (SEC-01, P1)

Files:

- `backend/src/api/routes/owner.ts`
- `backend/src/owner/alertRules.ts`
- new `backend/src/security/outboundWebhook.ts`
- `backend/src/workers/health.ts`
- a uniquely numbered additive owner-alert-delivery migration allocated after
  the Phase 0 migration ledger is active
- new unit/integration tests beside those modules

Implementation:

1. Parse and length-bound the URL; reject credentials, fragments, non-HTTPS
   schemes, disallowed ports, literal loopback/private/link-local/multicast/
   reserved addresses, and ambiguous IPv4/IPv6 encodings.
2. Resolve A and AAAA records immediately before delivery and reject if any
   result is non-public. Disable redirects or validate every hop with the same
   policy. Prevent DNS rebinding by connecting through the validated address
   while preserving TLS hostname verification, or use the approved egress
   proxy.
3. Revalidate at send time, not only at rule creation. Bound concurrent
   deliveries, keep the existing deadline, cancel/drain bodies, and cap response
   bytes.
4. Persist a bounded delivery/outbox record with rule ID, attempt, timestamps,
   sanitized destination host, outcome, and next retry. Add exponential backoff,
   idempotency, and a test-delivery operation.

Acceptance:

- Table tests reject every non-public range, IPv4-mapped IPv6, userinfo,
  alternate encoding, disallowed port, private DNS answer, public-to-private
  redirect, and DNS-rebinding fixture without issuing the internal request.
- A permitted public webhook still receives the documented payload. If webhook
  authenticity is a product requirement, define a separate HMAC header, secret
  storage/rotation, canonical serialization, and replay-window contract before
  calling the payload signed.
- Delivery concurrency, retry count, age, and history retention remain bounded.
- Egress policy independently denies internal networks.

Rollout/rollback:

- Ship schema and disabled delivery history first, then the new sender.
- If delivery breaks, disable webhook delivery and retain queued events; never
  roll back to unrestricted fetching.

### 1.2 Remove backend Docker host authority (SEC-02, P1)

Files:

- `docker-compose.yml`
- `Dockerfile.backend`
- `backend/src/owner/ownerAclReconciler.ts`
- relevant MQTT ACL manager/tests
- `mosquitto/mosquitto.conf`
- new broker-local reload helper under `docker/`

Implementation:

1. Write ACL output atomically to a dedicated shared volume, validate it, retain
   a last-known-good copy, and request reload through a narrow authenticated
   broker-local helper.
2. Land a dual reload mode, verify production readback, then remove
   `/var/run/docker.sock` from the backend.
3. Run the backend with a numeric non-root user, `no-new-privileges`, dropped
   capabilities, a read-only root filesystem, and explicit writable mounts.
4. Add policy-as-code that rejects Docker socket mounts on request-serving
   services.

Acceptance:

- Owner grant/revocation updates the ACL and reloads Mosquitto under concurrent
  changes; invalid ACL output leaves the live ACL untouched.
- Backend cannot list containers, create workloads, mount host paths, or signal
  unrelated processes.
- Backend health and packet ingest pass under non-root/read-only restrictions.

Rollout/rollback:

- Deploy helper and dual mode before removing socket access. Keep the old mode
  for one release only, guarded by an operator-only flag.
- Roll back to the last known-good compatible backend image digest if ACL
  reload health fails; do not
  expose a general host control plane to restore functionality indefinitely.

### 1.3 Bound live viewshed admission (SEC-03, P1)

Files:

- `backend/src/queue/publisher.ts`
- `backend/src/features.ts`
- `backend/src/index.ts`
- `viewshed-worker/worker.py`
- shared queue tests in both languages
- `backend/src/health/status.ts`

Implementation:

1. Make the existing `SADD` plus `LPUSH` admission atomic and enforce a tactical
   global job-count and payload-byte cap plus worker-heartbeat check while
   preserving the current consumer payload. Rejecting admission must remove any
   pending marker created by the same operation.
2. Coalesce repeated coordinate updates to the latest state for one node.
3. Export admission, rejection, count, bytes, oldest age, and
   pending/list-divergence metrics and add essential saturation alerts.
4. Do not invent a second lease/retry/DLQ protocol here. The single versioned
   recoverable state machine, dedicated/no-eviction Redis decision, and
   producer/consumer key migration are implemented once in Phase 3.2.

Acceptance:

- Atomic concurrency/property tests cannot exceed the exact job or byte cap.
- Load above worker drain rate keeps Redis memory and core REST/WS/ingest within
  the Phase 0 budget.
- Rejected and coalesced jobs leave the existing pending set/list consistent.

Rollout/rollback:

- This is a producer-side compatible admission patch. Deploy with the feature
  disabled, validate counters against the existing list/set, then enable.
  Phase 3 performs the only versioned queue-key migration.

### 1.4 Fix public website WebSocket identity (SEC-04, P1)

Files:

- `nginx.website.conf`
- `nginx.app.conf`
- `anubis/botPolicy.yaml`
- `backend/src/http/trustedProxy.ts`
- `backend/src/ws/server.ts`
- `frontend/src/hooks/useWebSocket.ts`
- end-to-end proxy tests

Implementation:

1. Replace overlapping website WebSocket locations with one canonical exact
   `/ws` configuration (or one shared include) that overwrites both
   `X-Real-IP` and `X-Forwarded-For` with `$mesh_client_ip`.
2. Keep backend trusted peers explicit; caller headers from an untrusted peer
   must never influence identity.
3. After the identity fix, stage-test whether Anubis can require a solved
   challenge instead of unconditionally allowing all `/ws` handshakes.

Acceptance:

- At least 100 distinct test client IPs can connect concurrently.
- One IP exceeding 20 connections or the handshake budget does not affect a
  second IP.
- `/ws` and `/ws/` cannot select different forwarding contracts.
- Spoofed forwarding headers are overwritten.

Rollout/rollback:

- Deploy the Nginx identity fix independently first. Treat any cross-user quota
  sharing as rollback-worthy. Change Anubis policy only after browser reconnect
  tests; its policy can roll back separately.

### 1.5 Scope public link history (SEC-05, P1)

Files:

- `backend/src/api/routes/productFeatures.ts`
- `backend/src/http/requestScope.ts`
- `backend/src/api/utils/networkFilters.ts`
- new repository/query helper and route tests
- `frontend/src/components/Map/LinkQualitySparkline.tsx`

Implementation:

1. Resolve the explicit public network from host/header/query under the agreed
   contract.
2. Join both endpoint nodes; require same requested scope and public visibility.
   Apply the private marker before reading derived radio/link rows.
3. Return indistinguishable 404 responses for hidden, cross-scope, missing, and
   mixed pairs. Include scope in frontend request/cache keys.
4. Prefer a database view that defines a public visible link once for history,
   map, topology, export, cache warmup, and WebSocket use.

Acceptance:

- Fixtures cover visible, private, cross-network, missing, reversed, and mixed
  pairs under every host/network exposure approved by the ADR.
- `A:B` and `B:A` enforce identical authorization and cache behavior.
- No link-derived response reveals the existence or activity of a private node.

### 1.6 Make health telemetry trustworthy (SEC-06, P1)

Files:

- `backend/src/api/routes/telemetry.ts`
- `backend/src/health/status.ts`
- `backend/src/api/bootstrap/limiters.ts`
- frontend error reporter/bootstrap HTML
- migration for bounded aggregation/retention if needed

Implementation:

1. Separate untrusted diagnostic volume from authoritative health.
2. Remove frontend-error counts from authoritative warning/critical health.
   Backend checks, worker heartbeats, synthetic probes, and other server-owned
   evidence are the authoritative sources.
3. Apply a dedicated lower rate/storage quota, sampling, normalized fingerprint
   deduplication, and per-source contribution cap to diagnostic reports. A
   build token or same-origin nonce may reduce casual noise but must not be
   treated as user identity or authenticity.
4. Retain raw details briefly and aggregates longer. Show diagnostic trends
   only in the operator view with an explicit untrusted label; public status
   must not derive severity from them.

Acceptance:

- One anonymous or repeated source cannot set warning/critical health.
- Replayed and duplicate reports remain within storage/rate bounds.
- Backend/synthetic failures still trigger deterministic health alerts without
  relying on browser telemetry.

### 1.7 Pin external health-check build input (SEC-07, P1)

Files:

- `Dockerfile.mesh-health-check`
- `docker-compose.yml`
- `.env.example`
- CI release workflow and operations docs

Implementation:

1. Require a full reviewed upstream commit and verify fetched `HEAD` exactly
   matches it; use `npm ci`.
2. Prefer building the external project in its own trusted pipeline and
   referencing an immutable image digest. Phase 7 adds signing,
   SBOM, and provenance; the immediate finding closes when build input is bound
   to a reviewed immutable commit and output digest.
3. Fail Compose/release validation when the pin is absent, mutable, or
   unapproved.

Acceptance:

- Two builds from one pin consume the same declared source and lockfile.
- A branch/tag or mismatched commit fails before executable content is copied.
- Release inventory records repository revision, source pin, and resulting image
  digest; Phase 7 extends this record with SBOM, scan, signature, and
  provenance.

### 1.8 Adjacent hardening, retained as non-vulnerabilities

These did not survive attack-path calibration but should be fixed with the
nearest work:

- `backend/src/api/utils/localOnly.ts`: accept forwarding metadata only from a
  trusted socket peer; require operator auth for test diagnostics. Add a direct
  remote peer plus private spoofed-header 403 test.
- `backend/src/stats/statsRepository.ts`: scope prefix candidates by network
  for correctness under whichever network ADR is approved.
- `scripts/generate-observer-key.ts`: create parent directory `0700`, file
  `0600`, refuse unsafe symlinks/existing paths, and avoid printing private
  material by default. Add `scripts/package-lock.json`.
- `README.md`: make all public HTTP Cloudflare routes target Anubis sidecars;
  direct MQTT routing remains intentional.
- Immediately fail closed or narrow anonymous `/planned-nodes`: remove
  `owner_pubkey` and free-form `notes`, add an explicit publish/visibility
  predicate and pagination, and return no records until the public DTO decision
  is approved. Phase 8 can productize a reviewed public view.
- Planned coverage: retain existing admission controls but add a per-job
  execution budget and load test in Phase 3.

### 1.9 Enforce one anonymous coordinate boundary (SEC-13, P1 hardening)

Files and sinks:

- server DTOs/queries that feed packet detail, Stats decoded paths, topology,
  coverage/LOS, observers, planned nodes, and map layers
- `frontend/src/components/Map/DeckGLOverlay.tsx`
- `frontend/src/pages/ukmesh/PacketDetailPanel.tsx`
- `frontend/src/pages/StatsPage.tsx`
- `frontend/src/pages/TopologyPage.tsx`
- `frontend/src/components/Map/MapLibreMap.tsx`
- shared client privacy/masking utility and E2E fixtures

Implementation:

1. Implement the decision-table recommendation: exact private-node,
   private-observer, private-relay/path, raw-advertisement, LOS, topology, and
   planned-node coordinates do not enter anonymous REST/WS payloads.
2. Create privacy-safe server DTOs/views and apply them before caching,
   exporting, fanout, and response serialization. Add a defense-in-depth client
   mask before any GeoJSON, Deck.gl picking object, raw packet view, table, or
   tooltip.
3. Inventory and test every secondary map and textual coordinate sink, not only
   the primary map. Keep heatmap disabled until all fixtures pass.

Acceptance:

- Public browser/network traces contain no prohibited exact coordinate or
  recoverable raw advert for every listed sink.
- Server tests fail before a prohibited coordinate reaches cache or fanout;
  client tests prove a malformed/legacy payload is still masked.
- Owner/operator views retain authorized exact data through separate DTOs and
  cannot be cached under anonymous keys.

Phase 1 completion gate:

- All seven security regression suites pass.
- Public privacy matrix passes for REST, WS, exports, derived paths/links,
  caches, both network-scope outcomes in the ADR, and the independent Feed
  channel dimension.
- No Internet-facing service mounts the Docker socket or runs with unnecessary
  host authority.
- Security report findings have been revalidated against the implementation and
  closed with evidence.

## 8. Phase 2 — backend correctness, lifecycle, and operator usability

### 2.1 Remove duplicate route and unify map/stat semantics

Files:

- `backend/src/api/routes/nodes.ts`
- `backend/src/stats/statsRepository.ts`
- `backend/src/api/routes.ts`
- new route-registration and repository integration tests

Implementation:

1. Delete the first `GET /nodes/map` handler at the audited
   `nodes.ts:84-105`; retain one limited implementation.
2. Define one public map predicate/DTO for roles, coordinate validity,
   freshness, visibility, network, observer, and allowed fields. Reuse the same
   predicate for map counts and relevant statistics.
3. Implement the Phase 0 ADR's complete bounded delivery contract. If it is
   bbox/viewport based, update the frontend for pan/zoom loading and overlap
   deduplication; if cursor based, fetch through completion with a snapshot
   token. Never silently truncate the initial map.
4. Add a route registry assertion that fails for duplicate method/path pairs
   and large-result tests for the approved row/byte/request budgets.

Acceptance:

- Exactly one handler exists and it is protected by `nodesLimiter`.
- The client can prove when its current viewport/snapshot is complete and
  presents explicit loading/error state for an incomplete sequence.
- Map rows and map summary counts agree on the same fixture.
- Private, stale, invalid-coordinate, wrong-role, and wrong-scope nodes are
  consistently included or excluded according to the ADR.

### 2.2 Lease and atomically complete analysis runs

Files:

- a uniquely numbered additive `analysis_leases` migration allocated after the
  Phase 0 migration ledger is active
- `backend/src/analysis/runState.ts`
- `backend/src/analysis/boundedRun.ts`
- `backend/src/workers/path-history.ts`
- `backend/src/path-learning/rebuild.ts`
- `backend/src/spam/analyzer.ts`

Implementation:

1. Add lease owner/token, expiry, heartbeat, attempt, and terminal reason.
2. Claim, heartbeat, publish, and finish with compare-and-swap on the lease
   token. Make completion and active-run clearing one transaction.
3. Reclaim expired runs as failed/retryable; impose a work deadline and pass an
   abort signal through database, resolver, and worker-thread operations.
4. Compare privacy/model generation before publishing canonical results.
5. After draining legacy workers, run an audited one-shot transaction that
   marks orphaned legacy `active_run_id` rows failed and clears them before
   lease enforcement. Report every affected workload and require operator
   approval.

Acceptance:

- Killing a worker mid-run permits exactly one reclaim after TTL.
- Two workers cannot publish the same scope/generation.
- A failure at every transaction boundary cannot leave a permanent
  `active_run_id`.
- Work stops after lease loss or deadline and reports the correct terminal
  state.

Rollout/rollback:

- Deploy additive columns, then lease-aware readers/consumers, drain legacy
  workers, perform/read back the orphan cleanup, and only then require leases.
  Retain old columns for one release.

### 2.3 Graceful process shutdown

Files:

- `backend/src/index.ts`
- `backend/src/mqtt/client.ts`
- `backend/src/db/packetBatch.ts`
- `backend/src/ws/server.ts`
- `backend/src/queue/publisher.ts`
- `backend/src/path-beta/resolvePool.ts`
- database/owner-auth pools and every timer-owning worker

Implementation order:

1. Add a lifecycle coordinator and idempotent `close()` contract.
2. On SIGTERM/SIGINT, mark readiness draining; stop HTTP/MQTT/job admission and
   timers; stop MQTT reconnect; close HTTP keep-alive and WS with code 1001.
3. Flush accepted packet batches, finish or release leases, then close worker
   threads, Redis, and PostgreSQL pools.
4. Enforce a configurable deadline and emit outstanding-resource diagnostics
   before forced exit. Any unhandled rejection or uncaught exception must itself
   initiate the same idempotent drain and exit non-zero; continuing in an
   uncertain state is not allowed.

Acceptance:

- Define application acceptance explicitly. Every packet handed to a
  persistence batch before admission closes must either be stored or produce a
  recorded failure before exit; every admitted durable job must be completed or
  recoverable. Do not equate MQTT broker acknowledgment with database
  persistence.
- Readiness fails before listeners stop, repeat signals are harmless, and exit
  occurs within 30 seconds.
- No timer, socket, worker thread, Redis connection, or pool keeps the process
  alive after successful drain.

### 2.4 Central error and input handling

Files:

- all `backend/src/api/routes/*.ts`, starting with `owner.ts`, `misc.ts`,
  `nodes.ts`, and `radio.ts`
- new shared async handler/error middleware and schema/parser utilities

Implementation:

- Wrap Express 4 async handlers so rejected promises always produce a
  structured response and request ID.
- Centralize bounded integer/hour/cursor/hash/coordinate/observer/network
  parsing; reject negative, NaN, infinite, oversized, and ambiguous values with
  400.
- Add upstream connect/total timeouts, body-size caps, allowed content types,
  and circuit behavior to radio proxying.
- Keep internal error detail in logs, not public payloads.

Acceptance:

- Injected database failures in every owner alert handler return 500 promptly
  without hanging or unhandled rejection.
- Invalid parameters never reach SQL and table-driven boundary tests cover
  every shared parser.
- Slow, oversized, redirecting, and invalid-content upstream radio responses
  fail within the configured budget.

### 2.5 Browser-usable operator site

Files:

- `backend/src/backend-site/routes.ts`
- `backend/src/backend-site/template.html`
- new `backend/src/security/operatorSession.ts`
- operator site tests and operations docs

Implementation:

- Add local-only token login that exchanges the operator token for a short-lived
  `HttpOnly`, `Secure`, `SameSite=Strict` session; keep bearer authentication
  for automation for one release.
- Document and test the supported secure transport: local HTTPS or an SSH
  tunnel to the HTTPS/localhost endpoint. Ordinary private-IP HTTP is not a
  supported reason to remove the `Secure` cookie flag.
- Add CSRF protection, logout, expiry, audit logging, and a strict CSP.
- Cache/aggregate the expensive dashboard payload and replace two-second
  polling with a visibility-aware slower interval or server events.

Acceptance:

- A normal browser using the documented local HTTPS/SSH path can log in and
  load all operator API calls without an extension or token in JavaScript/local
  storage.
- Remote/non-trusted peers cannot initiate a session.
- Expired/logout/CSRF cases fail closed; automated bearer clients remain
  compatible during transition.

Phase 2 rollback:

- Schema changes remain additive. Keep old header auth and compatible job reads
  for one release. Small route and lifecycle fixes should be independently
  revertible; never roll back privacy predicates or restore an unrecoverable
  run protocol.

## 9. Phase 3 — make workers and queues recoverable

Goal: ordinary link/RF work succeeds on a fresh deployment, transient failures
remain retryable, and no terminal job leaks capacity or commits after losing its
lease.

### 3.1 Centralize terrain acquisition and result classification

Files:

- `viewshed-worker/rf/terrain.py`
- `viewshed-worker/worker.py`
- `viewshed-worker/rf/loss.py`
- `viewshed-worker/backfill_profiles.py`
- new `viewshed-worker/tests/**`
- `docker-compose.yml`

Implementation:

1. Create one `ensure_tiles_for_link()`/terrain context boundary used by
   physical links, planned links, diagnostics, ordinary coverage, and
   backfills. A fresh default `link-worker` must be able to download its
   required bounded tiles without relying on the profile-gated viewshed worker.
2. Replace `None`/empty geometry overloading with explicit outcomes:
   `Computed`, `RetryableTerrainError`, `PermanentOutOfScope`,
   `InvalidJob`, and `Cancelled`.
3. Never store current-model empty coverage for a timeout, failed download,
   corrupt tile, GDAL error, clipping failure, database error, or deadline.
   Persist a separate permanent reason for valid out-of-bounds/unsupported
   cases.
4. Replace positional `compute_path_loss()` tuples with a named result object
   shared by worker and backfill, or retire the broken backfill if it is no
   longer required.
5. Bound URL, redirect, tile size, decompressed size, time, file path, and cache
   eviction behavior. Use atomic downloads and validate before promotion.
6. Inventory existing current-model empty coverage rows. Use available
   provenance/timestamps/job logs to classify legitimate permanent empty
   results; mark ambiguous/transient rows stale and requeue or delete their
   calculated marker before new writers are enabled. Report counts and sample
   every destructive classification.

Acceptance:

- An empty SRTM volume downloads a fixture tile and completes an ordinary link.
- Transient HTTP, timeout, corrupt, GDAL, and database failures retry and leave
  no “already calculated” marker.
- An out-of-UK job terminates once with a durable permanent reason.
- Backfill fixture stores the expected profile rather than raising on tuple
  unpacking.
- Planned jobs cannot become `ready` with a transient empty placeholder.
- Legacy transient/ambiguous empty rows are no longer treated as calculated;
  legitimate permanent empty outcomes remain explicitly classified.

Rollout/rollback:

- Keep result/status schema additive. Dual-read old null/empty state, but write
  only explicit new outcomes after rollout.
- A feature flag may select the legacy calculator for output comparison; it
  must not re-enable transient-empty success.

### 3.2 Repair link-v3 and ordinary viewshed queue state

Files:

- `viewshed-worker/link_queue_v3.py`
- `backend/src/queue/linkQueueV3.ts`
- `backend/src/queue/publisher.ts`
- `viewshed-worker/worker.py`
- queue health/status and operator tooling
- property/state-machine tests in TypeScript and Python

Implementation:

1. Define one versioned state model: queued, leased, retry-wait, succeeded,
   dead, expired, and purged.
2. Make admit, claim, renew, ACK, NACK, requeue, dead-letter, and purge atomic.
   Active capacity and the DLQ have separate count/byte accounting. Final NACK
   atomically removes active capacity and moves the payload/dedupe/attempt state
   into the bounded DLQ; requeue reverses that move under admission checks;
   purge alone deletes retained payload and dedupe state.
3. Give dead jobs a count/byte cap, retention, and bounded operator
   requeue/purge command. Define what happens when the DLQ itself is full.
4. Make `LeaseRenewer` reconnect/back off on Redis errors and communicate
   definitive lease loss to the main job. Redis notification alone cannot fence
   a PostgreSQL commit: add a unique job/idempotency key and commit marker, take
   the scoped PostgreSQL advisory lock, and recheck lease token plus
   model/privacy generation inside the output transaction before commit.
5. Add an invariant repair command that scans the bounded keyspace, reports
   discrepancies, and can repair counters under an operator lock.
6. Put durable queue keys on a dedicated Redis instance or configure verified
   `noeviction`; never allow generic cache eviction to delete list/hash/lease
   state. Disposable caches must use separate memory/recovery policy.

Acceptance:

- Randomized state-machine tests always make recorded count/bytes equal actual
  queued/in-flight active state and separate DLQ state.
- Final NACK restores active capacity while retaining a requeueable DLQ payload;
  purge deletes the DLQ payload/dedupe and decrements DLQ accounting.
- Worker death produces one recoverable retry; lease loss prevents duplicate
  committed output through the database transaction fence and unique job key.
- Redis interruption during renewal does not silently continue ownership.
- Depth, bytes, oldest age, lease losses, retries, dead jobs, and repair deltas
  are observable.

Rollout/rollback:

- Deploy consumers that understand old and new keys first, freeze producers,
  drain/snapshot old active work, reconcile counters, then switch producers.
  Retain old keys read-only for the maximum retry/DLQ window. Essential queue
  alerts must be live before producer cutover.

### 3.3 Repair ML learner semantics before enabling it

Files:

- `ml-path-learner/worker.py`
- additive migration for cursor/lease state
- new `ml-path-learner/tests/**`
- `docker-compose.yml`

Implementation:

- Replace timestamp-only checkpointing with a stable tuple such as
  `(observed_at, packet_id)` and deterministic ordering.
- Use bounded transactions/savepoints. Do not advance the checkpoint beyond
  failed work, and compute inserted counts only after commit.
- Sample/filter in SQL before loading training data.
- Add a single-leader lease, heartbeat, run deadline, graceful shutdown,
  model/data version, and publication compare-and-swap.

Acceptance:

- More than 5,000 rows sharing one timestamp are processed exactly once across
  batches and restarts.
- Injected row failure preserves already committed batches and does not skip
  failed/later rows.
- Two replicas produce one active run and a crashed leader is reclaimed.
- Memory use is bounded by configured sample/batch size.

Rollout/rollback:

- Keep the service disabled in Compose until all tests and a shadow-data run
  pass. Additive state may remain unused on rollback.

### 3.4 Optimize RF computation against a correctness budget

Files:

- `viewshed-worker/worker.py`
- `viewshed-worker/rf/loss.py`
- benchmark/golden fixtures under `viewshed-worker/tests/`
- `docker-compose.yml`

Implementation:

1. Instrument tile acquisition, VRT construction, radial calculation,
   polygonization, peer lookup, per-link calculation, DB write, duration, and
   peak RSS.
2. Replace repeated full-prefix loss calculations along each of 360 rays with
   incremental/vectorized state.
3. Reuse one tile/VRT context per job and spatially index eligible peers before
   selecting the bounded nearest/relevant set.
4. Add a configurable hard job deadline, cancellation checks inside loops, and
   model-version separation for the new algorithm.

Acceptance:

- Golden fixtures stay inside an agreed path-loss and geometry tolerance.
- Representative p50/p95 duration and peak RSS meet the reviewed Phase 0 SLO.
  A 2x p95 improvement is a stretch objective, not a release gate if the
  approved SLO is already met.
- Deadline produces retry/dead status, never empty success.
- No job exceeds its container memory/CPU budget and cancellation latency is
  bounded.

## 10. Phase 4 — database, ingest, API, and cache efficiency

### 4.1 Make migrations safe and attributable

Files:

- `backend/src/db/migrations.ts`
- `backend/src/db/migrations/README.md`
- historical migrations `016_private_prefixes.sql`,
  `016_stale_mqtt_observer_cleanup.sql`, and
  `018_api_performance_indexes.sql` for inventory only; keep their bytes
  unchanged
- new bounded backfill tools under `backend/src/tools/`

Implementation:

1. Extend/read back the Phase 0 advisory lock and immutable migration ledger.
   Inventory every environment and record a compatibility mapping for the two
   existing `016` filenames; enforce unique numeric prefixes for new files.
2. Never edit, rename, or reformat an applied historical migration after
   checksums exist. For an environment where 016/018 is still pending, use an
   approved runbook: either execute the original byte-identical file in a
   backed-up maintenance window, or explicitly record a reviewed supersession
   and apply a new schema-only migration plus bounded backfill. Never silently
   skip it.
3. Keep new schema migrations additive, short, and protected by lock/statement
   timeouts. Support audited non-transactional operations such as
   `CREATE INDEX CONCURRENTLY` without wrapping them in an incompatible
   transaction.
4. Implement future/replacement historical rewrites as resumable bounded tools
   with checkpoint, dry-run, batch size, pause, progress, verification, and
   idempotent restart. The old 016/018 files remain byte-identical.

Acceptance:

- Two migration runners cannot execute concurrently.
- Altered applied migration content fails before execution.
- Every environment has an explicit 016/018 status or supersession record; no
  checksum is rewritten to hide history.
- A production-sized backfill can stop/restart without long locks, skipped
  rows, duplicate results, or `statement_timeout=0`.
- Large indexes use the tested concurrent path and survive interruption/retry.
- CI tests an upgrade from the previous release snapshot.

### 4.2 Consolidate ingest writes

Files:

- `backend/src/mqtt/client.ts`
- `backend/src/db/packetBatch.ts`
- `backend/src/db/index.ts`
- schema/indexes supporting observer and network rollups

Implementation:

1. Make the packet batch transaction maintain all observer fields now written
   by the separate MQTT `upsertNode`; remove the redundant per-packet upsert.
2. Batch/coalesce node network sightings and rollup deltas. Preserve raw packet
   persistence as the primary accepted-work boundary.
3. If derived updates move asynchronously, add an idempotent outbox/repair job,
   lag metric, and replay cursor; do not silently drop them.
4. Review hot-row/index contention using real query plans and reduce unnecessary
   updates when values have not changed.

Acceptance:

- One observer update occurs per batch rather than two writes per packet.
- MQTT-to-DB-to-WS integration fixtures produce identical packet, node-online,
  observer, privacy, network-sighting, and rollup results.
- Sustained-load throughput improves and DB CPU/WAL per packet falls from the
  baseline without increasing application-accepted persistence failures.

### 4.3 Replace full-table statistics with aggregates

Files:

- `backend/src/stats/statsRepository.ts`
- `backend/src/api/bootstrap/caches.ts`
- new additive stats aggregate migration
- new resumable aggregate backfill tool
- stats route/repository/load tests

Implementation:

1. Collapse the 17 chart scans into grouped queries and appropriate
   hourly/daily Timescale continuous aggregates or maintained rollup tables.
2. Create aggregates with no initial data; backfill one bounded time slice per
   transaction with a checkpoint.
3. Add indexes aligned to final map, privacy, network, observer, time, and
   link-history predicates. Validate with `EXPLAIN (ANALYZE, BUFFERS)`.
4. Add `STATS_AGGREGATE_READS_ENABLED`; shadow-query old and new paths, compare
   totals/percentiles across scopes, then switch.
5. Replace long cold-cache recomputation with snapshot versioning and
   stale-while-revalidate that has single-flight refresh and explicit failure
   behavior.

Acceptance:

- Shadow outputs match within documented exact/tolerance rules.
- Stats p95 and DB CPU meet the target set from Phase 0; no endpoint launches
  an unbounded number of full raw scans.
- Cache refresh cannot stampede and stale data remains clearly timestamped.
- A backfill can resume and does not block ingest.

Rollout/rollback:

- Schema is expand-only and legacy queries remain available for one release.
  Disable the read flag to roll back; do not delete new aggregates immediately.

### 4.4 Introduce an explicit data lifecycle

Files:

- additive lifecycle migration after approval
- `docs/db-lifecycle.md`
- `README.md`
- backup/restore tooling from Phase 7
- affected stats/path/export queries

Implementation:

1. Inventory every advertised lookback and dependency on raw packets.
2. Enable Timescale compression for cold chunks first and measure CPU/disk.
3. After restore drills and aggregate cutover, enable retention table by table:
   raw packets, frontend errors, operational snapshots, link/job history, dead
   jobs, logs, and derived models each need separate policy.
4. Preserve longer-lived privacy-safe aggregates/archive if product requires
   historical trends. Define deletion/export handling for owner/private data.

Acceptance:

- A dry run reports exact rows/chunks and every feature that would lose history.
- Backup freshness and successful restore are hard gates before deletion.
- All documented history windows work using retained data/aggregates.
- Disk/WAL/chunk growth and retention failures alert.

Rollback:

- Compression and retention flags ship disabled. Once data is deleted, rollback
  requires tested restore, so enable only after explicit operator approval.

### 4.5 Bound backend caches and external work

Files:

- `backend/src/path-beta/resolveCache.ts`
- `backend/src/cache/boundedTtlMap.ts`
- owner/spam/API cache modules
- `backend/src/api/routes/radio.ts`
- cache metrics and tests

Implementation:

- Replace plain maps with `BoundedTtlMap` or an extended byte-aware variant.
- Key entries by privacy generation, network, observer, and owner/session where
  relevant; invalidate without full-map scans.
- Define maximum entries/bytes, TTL, negative-cache policy, single-flight
  behavior, and eviction for every cache.
- Bound outbound concurrency, time, redirects, and response bytes.

Acceptance:

- A reviewed registry/static check covers process-lifetime data caches and fails
  when one lacks a policy; it explicitly excludes short-lived algorithmic,
  request-local, and in-flight coordination maps.
- Soak/load tests keep heap under its budget and network switches or privacy
  generation changes cannot reuse stale cross-scope values.

### 4.6 Correct health resource semantics

Files:

- `backend/src/health/status.ts`
- `docker-compose.yml`
- public/operator health DTOs

Implementation:

- Measure the actual database, Redis, terrain, and log volumes rather than
  container `/`.
- Split public coarse health/readiness from authenticated operator detail.
- Keep process CPU/memory labels accurate to the container/cgroup being
  measured and expose unknown rather than misleading host/server values.

Acceptance:

- Filling a fixture data volume drives the correct warning while unrelated root
  filesystem usage does not.
- Public responses disclose no unnecessary platform detail; operator responses
  retain actionable evidence.

## 11. Phase 5 — repair frontend correctness and state contracts

Goal: make headline features work, make snapshots authoritative, prevent
cross-network stale state, and remove races and misleading UI.

### 5.1 Spam Watch map lifecycle

Files:

- `frontend/src/pages/SpamTransparencyPage.tsx`
- `frontend/src/pages/spam-page.css`
- `frontend/test/e2e/public.spec.ts`

Implementation:

- Render the map container unconditionally.
- Create one MapLibre instance independently of incident data, wait for `load`,
  and update one source as loading/empty/error/data states change.
- Clean up the instance and pending fetches on navigation.

Acceptance:

- Delayed, empty, failed, and refreshed zone responses all produce the correct
  overlay and never prevent map initialization.
- Only one map exists and navigation removes it.

### 5.2 Complete inferred nodes and packet arcs

Files:

- `frontend/src/hooks/useNodes.ts`
- `frontend/src/components/Map/geojsonBuilders.ts`
- `frontend/src/components/Map/MapLibreMap.tsx`
- `frontend/src/components/Map/LiveOverlayController.tsx`
- `frontend/src/components/Map/DeckGLOverlay.tsx`
- `frontend/src/config/features.ts`
- new fail-closed runtime feature configuration loaded before map startup
- tests and README

Implementation:

1. Build inferred-node features from the actual inferred collection and
   active-ID set, with an explicit `is_inferred` property and distinct style.
2. Populate bounded, expiring arcs only when both privacy-safe endpoints can be
   resolved. If product does not want arcs, remove the dead control and claims
   instead of keeping a no-op feature.
3. Apply the shared privacy mask before features reach MapLibre/Deck.gl and cap
   collection size/age.
4. Implement and verify the layers in this phase while disabled by default.
   Phase 8 performs the product go/no-go and production enablement; it does not
   reimplement them.

Acceptance:

- Fixture inferred nodes appear, update, and disappear.
- One packet creates the expected arc and it expires.
- Private endpoints never enter GeoJSON, picking data, cache, or tooltip.

Rollout:

- Keep inferred nodes, arcs, and heatmap independently feature-flagged through
  same-origin runtime configuration that is loaded before map startup and
  defaults false on failure. Current `VITE_*` build-time flags alone are not an
  immediate kill switch. Test on the dedicated test hostname first; an operator
  must be able to disable the layer without rebuilding the image and clients
  must receive the change inside the documented cache/config TTL.

### 5.3 Make realtime stores authoritative and scoped

Files:

- `frontend/src/hooks/useLinkState.ts`
- `frontend/src/hooks/useNodes.ts`
- `frontend/src/hooks/useCoverage.ts`
- `frontend/src/hooks/useAppMessageHandler.ts`
- `frontend/src/hooks/useDashboardStats.ts`
- `frontend/src/App.tsx`
- new store tests

Implementation:

- Treat every initial snapshot, including `[]`, as authoritative.
- Delete a link when `itm_viable` becomes false; define unknown/null behavior.
- Add `reset(scope)` and epoch/version actions and call them before
  network/observer changes. Ignore late messages from older epochs.
- Canonicalize IDs at store boundaries.
- Pass actual packet batch size to dashboard statistics.

Acceptance:

- Empty snapshot clears prior links; `true -> false -> true` transitions are
  exact.
- Network/observer switch cannot display prior nodes, links, coverage, arcs,
  sparklines, counts, or pending fetch results.
- A batch of N packets increments observed count by N.
- Out-of-order snapshot/live fixtures converge deterministically.

### 5.4 Fix Feed state, path lifecycle, and connectivity

Files:

- `frontend/src/pages/ukmesh/UKFeedPage.tsx`
- `frontend/src/hooks/useWebSocket.ts`
- public/mobile E2E tests

Implementation:

- Do not validate/reset the stored IATA region until asynchronous options have
  finished loading.
- Model packet path as `idle | unavailable | settling | loading | ready |
  error`; a packet with no hashes is `unavailable`, not forever loading.
- Use actual WebSocket ready state plus last-message age for status. Distinguish
  connected-and-quiet, reconnecting, offline-with-cache, and live.
- Abort old scope requests and stop eager background resolution for every recent
  packet; resolve selected/near-viewport items under the numeric concurrency
  cap approved in Phase 0.

Acceptance:

- Stored region survives reload.
- No-hash packet shows a terminal “no path available” state.
- Quiet connection remains connected and cached disconnected data is not
  labelled live.
- Old network requests cannot overwrite current feed/path state.

### 5.5 Fix repeater, packet-detail, Stats, and count defects

Files:

- `frontend/src/pages/ukmesh/UKRepeaterSearchPage.tsx`
- `frontend/src/pages/ukmesh/PacketDetailPanel.tsx`
- `frontend/src/pages/StatsPage.tsx`
- `frontend/src/hooks/usePacketDetailData.ts`
- `frontend/src/hooks/useAppMessageHandler.ts`
- `frontend/src/hooks/useDashboardStats.ts`

Implementation:

- Request `advert_count`, `elevation_m`, and every displayed repeater field.
  Use null checks so longitude `0` is valid. Abort or sequence A-to-B detail
  requests.
- Remove the second propagation unit suffix.
- Check every Stats response status; add error, empty, retry, and last-updated
  states. Supply a glyph-enabled style or remove the unsupported text layer.
- Cache global radio statistics once per scope rather than under packet keys;
  skip path calls when no hashes exist.

Acceptance:

- Rapid A→B selection can never show A under B; longitude zero renders.
- Propagation units appear exactly once.
- Failed/empty Stats requests show useful retryable feedback rather than blank
  content.
- Packet detail makes no unnecessary global-radio or empty-path requests.

Phase 5 rollback:

- Preserve API shapes and tolerate old local-storage values during the release.
- Ship broken-feature repairs in small PRs with feature flags for new layers;
  retain the security property during rollback. If an epoch/reset change is
  faulty, disable the affected UI, clear scoped in-memory/persisted state, and
  roll forward or restore the prior privacy-safe reader; do not force faulty
  state code to remain live or restore a cross-scope reader.

## 12. Phase 6 — frontend performance, accessibility, and architecture

### 6.1 Standardize polling, requests, and caches

Add:

- `frontend/src/utils/scopedCache.ts`: TTL/LRU with entry and estimated-byte
  bounds, scope invalidation, single-flight, and metrics/debug snapshot.
- `frontend/src/hooks/useVisibilityPoll.ts`: one request at a time,
  `AbortController`, timeout, page-visibility gating, jitter, backoff, and
  immediate refresh on return.
- checked, abort-aware response helpers in `frontend/src/utils/api.ts`.

Migrate:

- `frontend/src/components/Map/MapLibreMap.tsx`
- `frontend/src/hooks/usePacketDetailData.ts`
- `frontend/src/pages/ukmesh/UKFeedPage.tsx`
- `frontend/src/pages/ukmesh/UKRepeaterSearchPage.tsx`
- `frontend/src/pages/OwnerPortalPage.tsx`
- `frontend/src/components/LiveStatsSection.tsx`
- `frontend/src/pages/StatusPage.tsx`
- `frontend/src/pages/ukmesh/UKCompanionPage.tsx`
- `frontend/src/components/Map/LinkQualitySparkline.tsx`

Implementation:

- Key all cache and request state by network, observer, owner session, privacy
  generation, and relevant query parameters.
- Bound node links, packet detail, repeater detail, owner last-hop, Feed paths,
  and every other module cache; clear owner data on logout.
- Refresh observer health when scope changes.
- Replace perpetual two-second planned coverage polling with bounded,
  visibility-aware job polling and a terminal timeout/error action.
- Keep the owner MapLibre instance stable and update its sources rather than
  rebuilding every ten seconds.
- Include network/observer in sparkline requests and aggregate/cap concurrent
  charts rather than mounting up to eight independent unscoped calls.

Acceptance:

- No polling occurs while hidden; returning triggers one refresh.
- A slow request cannot overlap its successor or overwrite a newer scope.
- Every cache has documented scope, TTL, count/byte bound, and invalidation.
- Owner map construction count remains one across repeated live polls.
- Feed path concurrency, polling deadlines, and cache memory never exceed the
  numeric Phase 0 budgets.

### 6.2 Bound browser terrain, service-worker, topology, and list work

Files:

- `frontend/src/utils/terrainSampler.ts`
- `frontend/public/sw.js`
- `frontend/src/main.tsx`
- `frontend/src/pages/TopologyPage.tsx`
- `frontend/src/pages/SpamTransparencyPage.tsx`
- `frontend/src/pages/ukmesh/UKFeedPage.tsx`

Implementation:

- Terrain sampler: add in-flight tile deduplication, true LRU, abort support,
  the numeric Phase 0 memory budget, and `ImageBitmap.close()` on eviction.
- Service worker: use a new cache namespace, the Phase 0 storage quota,
  amortized pruning,
  and metadata rather than `cache.keys()` on every tile write. Do not force
  reload loops on controller/update changes.
- Topology: keep D3 positions in the rendering layer or throttle immutable
  React snapshots to a visible-frame budget rather than copying every node on
  every simulation tick.
- Use measured/dynamic virtualization for expandable rows, or render a bounded
  non-virtualized detail region outside the fixed-height list.

Acceptance:

- A 30-minute map/terrain soak remains under the numeric Phase 0 ceiling, closes
  evicted bitmaps, and issues one fetch per concurrent tile key.
- Service-worker tests cover warm and cold offline navigation, an update
  arriving during an in-progress interaction, explicit deferred activation,
  rollback to the previous worker/cache, and cleanup of the retained namespace
  after the rollback window; no reload loop occurs and pruning is amortized.
- Representative topology stays inside the Phase 0 node-count/frame-time
  budget with no
  per-tick full React state copy.
- Expanded Feed/Spam rows never overlap or disappear.

Rollout/rollback:

- Bump the service-worker cache namespace and retain the previous namespace for
  one rollback window. Activation must be explicit/deferred while the user has
  unsaved or in-progress state. Release browser-cache changes separately from
  map logic and remove the old namespace only after rollback expiry.

### 6.3 Build shared accessible primitives

Add:

- `frontend/src/components/ui/Dialog.tsx`
- `frontend/src/components/ui/Tabs.tsx`
- `frontend/src/components/ui/Combobox.tsx`
- `frontend/src/hooks/useReducedMotion.ts`

Prefer a maintained, proven accessible primitive library that fits the bundle
and styling constraints. If custom Dialog/Combobox primitives are retained,
budget explicit cross-browser, IME/composition, nested-dialog, screen-reader,
and focus-restoration testing rather than relying on ARIA attributes alone.

Migrate:

- dialogs in `DisclaimerModal.tsx`, `StatsPage.tsx`, and `UKFeedPage.tsx`
- tabs in `NodePopupContent.tsx` and `StatsPage.tsx`
- search in `NodeSearch.tsx` and `UKRepeaterSearchPage.tsx`
- interactive rows in `PacketFeed.tsx` and `UKFeedPage.tsx`
- range inputs in `FilterPanel.tsx` and `MobileControls.tsx`
- animations in `StatsPanel.tsx`, `globals.css`, `map-app.css`,
  `spam-page.css`, and `feed-page.css`

Acceptance:

- Dialogs trap focus, close with Escape, restore focus, and have labelled
  semantics.
- Tabs implement arrow/Home/End movement and linked tabpanels.
- Search exposes labelled combobox/listbox/options and active descendant.
- Rows work with Enter and Space; all controls have programmatic labels.
- Reduced-motion preference stops nonessential infinite and timer-driven
  animation.
- Every interactive element has a visible `:focus-visible` indicator. Touch
  controls target 44 CSS pixels where layout permits and never fall below the
  approved WCAG target-size/spacing rule.
- Charts and map-only summaries have an accessible text/table equivalent for
  their important values and trends.
- Axe reports no serious/critical issue on each public route and representative
  dashboard state.
- Keyboard plus NVDA/Firefox and VoiceOver/Safari smoke tests cover dialogs,
  combobox IME input, tabs, map controls, and nested overlays.
- A keyboard-only user can search/select a node and read its details without
  needing the map canvas.

### 6.4 Decompose large components and CSS

Targets:

- `MapLibreMap.tsx` (map lifecycle, source controller, polling, interaction,
  popup)
- `OwnerPortalPage.tsx` (session, live data, map, sections)
- `UKFeedPage.tsx` (controller, filters, list, path resolver, dialogs)
- `StatsPage.tsx` (data, charts, decoded-path map, dialogs)
- `ukmesh/PacketDetailPanel.tsx` (data presentation, path map, packet decoder)
- `backend/src/path-beta/resolver.ts`, `backend/src/db/index.ts`,
  `backend/src/mqtt/client.ts`, and `backend/src/stats/statsRepository.ts`

CSS:

- Keep tokens, reset, and site-wide typography global.
- Co-locate page/component styles or use CSS modules.
- Move dashboard selectors out of `globals.css`.
- Remove the 71 duplicate selectors before relying on a different import order.

Acceptance:

- Agree a soft 500–700 line ceiling for page/controller files; deviations need
  a written reason.
- Extracted controllers/hooks have focused tests and stable public interfaces.
- Duplicate-selector count meets the numeric Phase 0 target and no feature
  relies on load-order overrides.
- Desktop/mobile screenshots and the numeric bundle/chunk budgets do not
  regress.

## 13. Phase 7 — observability, data safety, and release hardening

### 7.1 Expand and operationalize metrics

Files:

- `backend/src/metrics.ts`
- `backend/src/index.ts`
- internal metrics route/middleware
- Node and Python queue/worker modules
- `logging/prometheus.yml`

Implementation:

1. Verify and extend the Phase 0 internal-only metrics endpoint. Do not publish
   raw operational labels on the public origin.
2. Actually increment MQTT message/outcome and DB-pool metrics. Add route
   latency/status, packet batch, WS admission, webhook delivery, cache,
   analysis lease, queue count/bytes/age/retry/dead, SRTM, RF phase, worker
   heartbeat, graceful shutdown, and privacy-filter metrics.
3. Keep labels low-cardinality; never use node ID, owner, webhook URL, packet
   hash, or exception message as a label.

Acceptance:

- Prometheus scrape returns valid data while public external access is denied.
- Each critical queue/dependency/worker has outcome, saturation, latency, and
  freshness signals.
- A label-cardinality test and scrape-size budget pass.

### 7.2 Complete monitoring and alerting

Files:

- `logging/promtail.yaml`
- new Grafana Alloy configuration and migration tests
- `logging/prometheus.yml`
- `logging/loki.yaml`
- `logging/grafana/**`
- `docker-compose.yml`
- new `logging/alertmanager.yml`
- new `logging/rules/*.yml`
- `docs/operations.md`

Implementation:

- Replace Promtail, which is [end of life as of 2026-03-02](https://grafana.com/docs/loki/latest/send-data/promtail/),
  with Grafana Alloy or another maintained collector. Use the
  [official Alloy migration path](https://grafana.com/docs/alloy/latest/set-up/migrate/from-promtail/)
  and persist positions/checkpoints in a named volume.
- Collect bounded log files or use a supported logging driver. Do not mount the
  raw Docker socket into a collector; a read-only filesystem mount does not
  make Docker API methods read-only. If container metadata is essential, use a
  separately authenticated GET-only proxy.
- Add Postgres/Timescale, Redis, Mosquitto, container/disk, and worker
  exporters or equivalent collectors.
- Add Alertmanager and rules for readiness, ingest silence, queue age/capacity,
  dead jobs, lease loss, worker heartbeat, DB pool/WAL/disk, backup age,
  telemetry integrity, and synthetic failures.
- Add health checks to monitoring services.
- Make provisioned dashboards non-editable or define an explicit export-to-Git
  workflow. Remove/disable the stale ML dashboard until the worker is enabled.
- Put runbook links and ownership in alert annotations.

Acceptance:

- Alloy/collector restart does not replay an already-ingested sentinel line;
  labels and Loki queries remain compatible through the cutover.
- Synthetic firing and recovery reach a test receiver.
- Killing each worker or filling a bounded test queue produces the named alert
  inside its documented grace period.
- All rules, dashboards, and Compose health checks validate in CI.

### 7.3 Backups, restore drills, and retention gates

Files:

- new encrypted off-host PostgreSQL backup/restore tooling or pgBackRest config
- Compose service/config if self-hosted
- `docs/db-lifecycle.md`
- `docs/operations.md`
- `README.md`

Implementation:

- Back up the analytics and owner-auth databases, required Mosquitto
  credential/ACL state, and critical configuration. Keep secrets encrypted and
  access-separated.
- Classify Redis state explicitly. Durable ready/in-flight/retry/dead jobs,
  dedupe/counters, lease/commit markers, and planned-job coordination must
  either be persisted/restored consistently or deterministically rebuilt from
  PostgreSQL under a documented fenced recovery procedure. Name disposable
  caches separately and prove their loss is safe.
- Automate restore into an empty isolated environment, migrate it, sample
  row/checksum integrity, verify owner lookup, and run readiness/smoke tests.
- Alert on backup age/failure. Add WAL/PITR only after credentials/storage and
  RPO are approved.
- Make every retention activation require a fresh successful restore receipt.

Acceptance:

- The agreed initial RPO/RTO is demonstrated, not merely documented.
- Restore to an empty environment passes application smoke tests.
- A missing/stale backup blocks destructive retention or maintenance.

### 7.4 Immutable, reversible deployment

Files:

- `scripts/replace-container.sh`
- all Dockerfiles
- `docker-compose.yml`
- release workflow
- operations docs

Implementation:

1. Publish one immutable signed image digest per service/release.
2. Replace force-recreate/no-deps deployment with: preflight, backup gate,
   migration job, deploy digest, readiness wait, smoke checks, metric check, and
   automatic prior-digest rollback only when the prior image has passed a
   compatibility test against the post-migration schema.
3. Record deployed revision/digest/schema version. Keep the previous compatible
   image for at least one release window.
4. Treat migrations as forward-compatible expansion; rollback deploys old code
   against the compatible newer schema, never a destructive down-migration.

Acceptance:

- A deliberately failed readiness/smoke check automatically restores the prior
  signed digest only when schema compatibility is proven. Otherwise rollout
  stops before traffic and requires the documented manual recovery path.
- Release status identifies exact source, image, migration, and configuration.
- A partial service update cannot skip required dependency/migration checks.

### 7.5 Supply-chain and container policy

Files:

- all Dockerfiles
- `docker-compose.yml`
- `.github/dependabot.yml`
- CI/release workflows
- Python requirements/locks
- `scripts/package.json` plus new lockfile

Implementation:

- Pin production/base images by digest with automated Docker update PRs.
- Add pip update automation and hash-locked Python dependencies.
- Generate SBOMs, scan images, sign/prove release artifacts, and define an
  explicit waiver process. No unwaived critical finding may release.
- Use non-root users, read-only roots, `cap_drop: [ALL]`,
  `no-new-privileges`, bounded tmpfs, and explicit writable data volumes where
  compatible.
- Pin Actions to immutable commits if repository policy requires it.

Acceptance:

- Rebuilds consume only pinned source, images, and dependencies.
- Every service passes smoke tests under hardened permissions.
- Release inventory contains digest, SBOM, scan, signature, and provenance.
- Dependabot/update automation covers npm, Actions, Docker, and Python.

### 7.6 Replace dangerous maintenance behavior

Files:

- `vacuum-compressed-chunks.sh`
- `docs/db-lifecycle.md`
- `docs/operations.md`

Implementation:

- Remove unsupervised blanket `VACUUM FULL`. Require dry-run output, explicit
  database/chunk selection, lock/statement timeout, disk-space preflight,
  backup freshness, one-chunk approval, progress/checkpoint, and DBA review.
- Prefer ordinary vacuum/reindex or online maintenance based on measured bloat.

Acceptance:

- Default invocation is read-only and prints exact intended work.
- Insufficient disk, stale backup, lock contention, or timeout aborts safely.
- A staged fixture proves resumability and no long unbounded lock.

## 14. Phase 8 — product completion and documentation

These features should follow the security/reliability foundations so they reuse
durable events, scoped stores, bounded jobs, and real metrics.

### 8.1 Owner alert experience

Backend:

- durable alert events/outbox, per-channel attempts, idempotency, bounded retry,
  delivery history, test delivery, last success/error, pause reason, and audit.

Frontend:

- owner portal list/detail for rules and delivery history, “send test,” clear
  validation, retry status, and accessible controls.

Acceptance:

- Every triggered alert is durably traceable to zero or one successful
  delivery per idempotency key.
- Owner sees only granted-node events and sanitized destination information.
- Retry/dead-letter behavior is operator-visible and bounded.

### 8.2 Observer registration workflow

Files:

- existing registration API/schema
- a new operator queue/service and operator UI
- owner/operator audit records and runbook

Implementation:

- Give submissions explicit `pending`, `approved`, `rejected`, `expired`, and
  `provisioned` states; validate/normalize PII; define retention and access.
- Add operator review, duplicate detection, decision reason, audit trail, and
  notification/provisioning handoff.

Acceptance:

- A submission cannot vanish into an unconsumed table.
- PII is operator-only, retention-bounded, and absent from public logs/metrics.

### 8.3 Job and model operations

- Add an operator view/API for analysis runs, live/planned viewshed, link-v3,
  ML runs, queue capacity, lease owner/expiry, attempts, oldest age, dead-letter
  reason, requeue, purge, and repair.
- Add path-confidence calibration trends by model version/network and compare
  predictions with later observations.

Acceptance:

- Operators can diagnose and recover a stuck/dead job without direct Redis/SQL
  mutation.
- Every action is authorized, audited, idempotent, and protected from bulk
  accidents.

### 8.4 Complete public discovery features

- Decide the public planned-node contract. Prefer owner/operator records and a
  separately publishable, privacy-reviewed DTO with pagination and expiry.
- Make Watchlist entries actionable: restore filter/search/selection, navigate
  to the saved item, and expose the panel on mobile.
- Add a wildcard 404 route.
- Use one route metadata source for runtime routing, canonical tags, sitemap,
  and build-time SEO.
- Enable and productize the already implemented arcs/inferred layers only after
  Phase 5 privacy/correctness tests and the runtime kill switch pass.

Acceptance:

- Planned records never expose owner public keys or free-form private notes
  unless an explicit reviewed publish action exists.
- Watchlist restore is deterministic across desktop/mobile and network scope.
- Invalid URLs render a useful 404; every public route has consistent canonical
  metadata and sitemap coverage.

### 8.5 API and architecture contracts

Files:

- `docs/openapi.yaml`
- request/response schemas
- route/repository/service modules
- `docs/architecture.md`, `README.md`, `docs/frontend-map.md`,
  `docs/operations.md`

Implementation:

- Generate or validate OpenAPI from the same schemas used at runtime; the
  current document covers only a small fraction of routes and advertises
  behavior that runtime rejects.
- Move SQL out of route modules, prioritizing nodes, planned coverage,
  topology, RF validation, owner, and product features.
- Delete confirmed dead `backend/src/api/caches.ts`.
- Correct nonexistent component references, route/feature claims, service
  topology, bootstrap, migrations, monitoring, Anubis ingress, retention, and
  runbooks.

Acceptance:

- CI fails when a route lacks or contradicts its OpenAPI contract.
- Example requests pass contract tests for public, test, owner, operator, and
  error cases.
- Documentation can bootstrap, deploy, roll back, restore, recover queues,
  manage SRTM, and respond to alerts without undocumented shell/SQL knowledge.

## 15. Cross-phase test and release matrix

Run the smallest relevant suite during development and the full matrix before
each phase is considered complete.

| Layer | Required gate |
| --- | --- |
| Backend fast | `cd backend && npm ci && npm run typecheck && npm test && npm run build` |
| Frontend fast | `cd frontend && npm ci && npm test && npx tsc --noEmit && npm run build` |
| Frontend browser | All Playwright desktop/public/dashboard/mobile projects plus axe and privacy fixtures |
| Python | Build worker images; run `pytest`, imports, and CLI smoke tests inside the exact GDAL/viewshed and ML image environments |
| Database | Pinned Timescale version; current backup/restore preflight; fresh and previous-release migrate; concurrent runner; concurrent-index retry; backfill resume; query-plan budgets |
| Redis/queues | Randomized state-machine/invariant tests, crash/lease-loss/restart, capacity/byte/load tests |
| MQTT E2E | Broker credential bootstrap, radio envelope to DB/rollup/WS, private/test/network fixtures, graceful termination |
| Proxy E2E | Cloudflare-header fixture through Anubis/Nginx to REST/WS, spoof resistance, per-client quotas |
| Containers | Build every Dockerfile/profile; non-root/read-only smoke; health checks; no Docker socket |
| Dependencies | npm audit policy, Python/image scan, SBOM, signature/provenance |
| Operations | Alert firing/recovery, persisted Alloy/collector position, PostgreSQL plus durable-Redis recovery, empty-environment restore, schema-compatible failed-deploy rollback |
| Performance | Compare p50/p95, DB CPU/WAL, heap/RSS, queue bytes/age, browser memory/long tasks, and bundle size with Phase 0 |

Release order for each substantial protocol/schema change:

1. Land tests and observability.
2. Deploy additive schema or compatible consumers.
3. Backfill/shadow/dual-read and compare.
4. Deploy producers/writers behind a flag.
5. Enable on the test hostname or one worker.
6. Expand gradually while watching correctness, privacy, queue, and SLO gates.
7. Keep the previous known-good digest that has passed compatibility against
   the new schema, plus the old read path, for one release window.
8. Contract/remove old state only after readback, restore, and rollback windows
   pass.

## 16. Complete issue register

This register is the audit backlog. Line numbers refer to the audited revision
and will move as implementation proceeds.

### Security and privacy

| ID | Pri | Finding and evidence | Planned resolution |
| --- | --- | --- | --- |
| SEC-01 | P1 | Owner webhook accepts only an HTTPS prefix and later calls unrestricted `fetch` (`backend/src/api/routes/owner.ts:233-257`, `backend/src/owner/alertRules.ts:39-53`). Blind SSRF is reachable by an authenticated owner. | Phase 1.1 |
| SEC-02 | P1 | Public backend mounts `/var/run/docker.sock` read-write and runs without a non-root `USER` (`docker-compose.yml:205-211`, `Dockerfile.backend:9-19`). A backend compromise can become host control. | Phase 1.2 |
| SEC-03 | P1 | Radio-influenced distinct node IDs can grow the live viewshed Redis set/list without a global count or byte cap (`backend/src/index.ts:104-111`, `backend/src/queue/publisher.ts:54-83,125-131`). | Phase 1.3 |
| SEC-04 | P1 | Website exact `/ws` omits forwarded client identity, collapsing all clients into the proxy's quota (`nginx.website.conf:68-76`, `backend/src/http/trustedProxy.ts:25-35`, `backend/src/ws/server.ts:432-448`). | Phase 1.4 |
| SEC-05 | P1 | Anonymous link history queries global pair records without network/private-node scope (`backend/src/api/routes/productFeatures.ts:70-94`). | Phase 1.5 |
| SEC-06 | P1 | Anonymous frontend error rows directly set health warning/critical at 25/100, inside one IP's 120/minute quota (`backend/src/api/routes/telemetry.ts:16-45`, `backend/src/health/status.ts:341-346,513-519`, `backend/src/index.ts:150-157`). | Phase 1.6 |
| SEC-07 | P1 | Health-check image defaults to fetching and executing upstream `main` (`docker-compose.yml:535-542`, `Dockerfile.mesh-health-check:3-24`). | Phase 1.7 |
| SEC-08 | P2 | `requireLocalOnly` can accept caller-supplied private forwarded values if backend ingress changes, although default Compose binds loopback (`backend/src/api/utils/localOnly.ts:24-49`, `docker-compose.yml:203-204`). | Phase 1.8 |
| SEC-09 | P2 | Scoped decoded-path statistics resolve prefixes against all positioned repeaters, not the requested network (`backend/src/stats/statsRepository.ts:320-345,403-432`). Under current runtime exposure this is correctness rather than a validated disclosure; the ADR may strengthen the boundary. | Phases 1.8/4.3 |
| SEC-10 | P2 | Observer key generator writes a private seed/PEM without explicit `0600` (`scripts/generate-observer-key.ts:48-64`). Effective exposure depends on umask and directory. | Phase 1.8 |
| SEC-11 | P2 | README Cloudflare examples target origins while Compose says public HTTP must target Anubis (`README.md:208-220`, `docker-compose.yml:637-640`). | Phases 1.8/8.5 |
| SEC-12 | P2 | A literal MeshCore channel key exists in a tracked backfill script (`backend/backfill-transport-codes.mjs:32-40`). It appears to be intentional public channel material but needs owner classification. | Decision gate / Phase 8.5 |
| SEC-13 | P2 | Heatmap/packet-detail path and advertisement coordinates use multiple rendering paths; candidates were suppressed because APIs/privacy masking provide counterevidence, not because frontend sinks are intrinsically safe. | Phases 1.9/5 |
| SEC-14 | P2 | Planned coverage performs expensive radial/peer work but has rate/global caps, feature gating, and worker isolation; no security outage was demonstrated (`backend/src/api/routes/plannedCoverage.ts:114-140`, `viewshed-worker/worker.py:504-539,879-938`). | Phases 3.4/6.1 |
| SEC-15 | P2 | Frontend lock resolves vulnerable advisory versions of PostCSS and React Router; audited attack modes were not applicable, but the high audit gate fails. | Phase 0.1 |
| SEC-16 | P1 | Runtime/test-host behavior and the supplied security invariant disagree about anonymous network `test`; UK Feed also has a separate content-channel `test` value. Encoding either interpretation before an ADR risks privacy or feature breakage. | Decision gate / Phases 0.3/1 |

### Backend, API, database, and lifecycle

| ID | Pri | Finding and evidence | Planned resolution |
| --- | --- | --- | --- |
| BE-01 | P1 | `GET /nodes/map` is registered twice; the first unbounded handler shadows the intended limited implementation (`backend/src/api/routes/nodes.ts:84-105,317-345`). | Phase 2.1 |
| BE-02 | P1 | Analysis `active_run_id` has no expiry/heartbeat and completion is split across autocommit updates (`backend/src/analysis/runState.ts:35-45,92-127`). Crashes can wedge a workload. | Phase 2.2 |
| BE-03 | P1 | Operator HTML requires bearer/custom headers but browser fetches send neither (`backend/src/backend-site/routes.ts:87-117,132-137`, `template.html:325-346`). | Phase 2.5 |
| BE-04 | P1 | No coordinated SIGTERM/SIGINT drain exists; packet batches and MQTT flush can remain in flight (`backend/src/index.ts:199-230`, `backend/src/db/packetBatch.ts:46-74,214-230`, `backend/src/mqtt/client.ts:526-531`). | Phase 2.3 |
| BE-05 | P1 | Migrations perform whole-table rewrites despite policy; duplicate `016` exists; runner lacks advisory lock/checksum (`016_private_prefixes.sql:1,150-251`, `018_api_performance_indexes.sql:15-22`, `backend/src/db/migrations.ts:12-58`). | Phases 0.4/4.1 |
| BE-06 | P1 | Owner alert async handlers lack Express 4 error boundaries and may hang/reject outside middleware (`backend/src/api/routes/owner.ts:219-270`). | Phase 2.4 |
| BE-07 | P2 | Metrics are registered but `/metrics` is absent; MQTT and DB metrics are not populated (`backend/src/metrics.ts:9-45`, `logging/prometheus.yml:5-9`). | Phases 0.4/7.1 |
| BE-08 | P2 | Stats charts launch 17 scans and cold recompute has taken about 80 seconds/DB CPU saturation (`backend/src/stats/statsRepository.ts:117-125`, `backend/src/api/bootstrap/caches.ts:5-10`). | Phase 4.3 |
| BE-09 | P2 | MQTT and packet batch redundantly update observer rows, followed by separate rollup/sighting writes (`backend/src/mqtt/client.ts:832-838`, `backend/src/db/packetBatch.ts:177-191`, `backend/src/db/index.ts:518-575,644-655`). | Phase 4.2 |
| BE-10 | P2 | Negative/NaN limits can reach SQL; radio proxy lacks timeout/body bound (`backend/src/api/routes/misc.ts:39-49`, `nodes.ts:648-675`, `radio.ts:6-40`). | Phase 2.4 |
| BE-11 | P2 | Resolve/sticky caches are unbounded maps and invalidation scans the map (`backend/src/path-beta/resolveCache.ts:11-37,48-86`) despite an available bounded cache. | Phase 4.5 |
| BE-12 | P2 | Map and stats disagree on roles/freshness, so headline totals can conflict (`nodes.ts:84-101`, `statsRepository.ts:716-732`). | Phases 0.3/2.1 |
| BE-13 | P2 | Raw packets and derived state are retained indefinitely; schema deliberately removes retention policies (`backend/src/db/schema/base.sql:1-2,64-101`, `README.md:316-320`). | Phase 4.4 |
| BE-14 | P2 | Health disk check observes container root rather than DB/Redis/terrain volumes; public “server” resource labels can mislead. | Phase 4.6 |
| BE-15 | P2 | Anonymous `/planned-nodes` exposes all proposed records, owner public keys, coordinates, names, and notes without lifecycle/pagination (`backend/src/api/routes/misc.ts:113-123`). Product intent is unclear. | Immediate containment in Phase 1.8; product decision in Phase 8.4 |
| BE-16 | P2 | Observer registration PII enters storage without an implemented operator workflow, status lifecycle, notification, or retention process. | Phase 8.2 |
| BE-17 | P2 | Owner alerts have no durable event/outbox/history/test delivery despite product claims; sequential delivery can make polling slow. | Phases 1.1/8.1 |
| BE-18 | P2 | Database numeric environment parsing lacks consistent minimum/maximum validation, permitting invalid pool/time settings. | Phase 2.4 |
| BE-19 | P2 | Unhandled rejection logging continues the process in uncertain state; no fatal/drain policy exists. | Phase 2.3 |
| BE-20 | P3 | SQL remains embedded in many routes; several core modules are very large; `backend/src/api/caches.ts` is an unused unbounded predecessor. | Phases 6.4/8.5 |
| BE-21 | P3 | OpenAPI covers only a small subset of roughly 59 routes and disagrees about public `all` scope. | Phase 8.5 |
| BE-22 | P3 | Schema comments/documentation disagree with runtime node roles and service layering. | Phase 8.5 |

### Python workers, queues, CI, and operations

| ID | Pri | Finding and evidence | Planned resolution |
| --- | --- | --- | --- |
| WK-01 | P1 | Fresh default link worker cannot acquire terrain; only the profile-gated viewshed worker downloads tiles (`viewshed-worker/worker.py:1007-1010`, `rf/terrain.py:151-158`, `docker-compose.yml:373-440`). | Phase 3.1 |
| WK-02 | P1 | Transient terrain/GDAL failures are stored as current-version empty success and suppressed from retry; planned jobs can also become ready empty (`worker.py:618-663,771-782,1489-1525,1588-1603`). | Phase 3.1 |
| WK-03 | P1 | `backfill_profiles.py` unpacks three values while `compute_path_loss` returns two, so its update path skips every valid row (`backfill_profiles.py:55-76`, `rf/loss.py:21-38`). | Phases 0.1/3.1 |
| WK-04 | P1 | Dead link jobs retain counters/payload/dedupe and permanently consume queue capacity (`viewshed-worker/link_queue_v3.py:35-68,99-139`). | Phase 3.2 |
| WK-05 | P1 | Redis error terminates `LeaseRenewer`; work may continue and commit after ownership is lost (`link_queue_v3.py:271-299`). | Phase 3.2 |
| WK-06 | P1 | ML timestamp-only cursor plus 5,000 limit permanently skips rows sharing the boundary timestamp (`ml-path-learner/worker.py:238-255`). | Phase 3.3 |
| WK-07 | P1 | ML insert rollback can erase earlier writes while processing advances the checkpoint (`ml-path-learner/worker.py:359-394`). | Phase 3.3 |
| WK-08 | P2 | ML loads the full gold table before sampling and lacks leader lease/deadline (`ml-path-learner/worker.py:408-446`). It is disabled today. | Phase 3.3 |
| WK-09 | P2 | RF radial loop recomputes every growing prefix across 360 rays, roughly 180 million prefix elements at maximum settings (`viewshed-worker/worker.py:504-539`). | Phase 3.4 |
| WK-10 | P2 | Planned coverage evaluates up to 60 peer terrain paths serially (`worker.py:879-938`). | Phase 3.4 |
| CI-01 | P1 | Nightly Compose omits required Anubis key and does not create ignored Mosquitto password/ACL files, so a clean job cannot start (`.github/workflows/ci.yml:91-125`, `docker-compose.yml:648,670,692`). | Phase 0.2 |
| CI-02 | P1 | Python CI is syntax-only, omits queue/backfill modules, installs no dependencies, and runs no tests/import/image smoke; GDAL tests need the exact worker image. | Phase 0.1 |
| CI-03 | P2 | Normal CI builds no containers; nightly covers only a subset, allowing frontends, workers, Nginx, profiles, and health source integration to rot. | Phase 0.1 |
| CI-04 | P2 | Main CI omits the frontend unit suite. | Phase 0.1 |
| OPS-01 | P1 | `scripts/replace-container.sh` force-recreates with `--no-deps` and has no migration, readiness, smoke, immutable record, or rollback. | Phase 7.4 |
| OPS-02 | P1 | `vacuum-compressed-chunks.sh` runs blocking `VACUUM FULL` over every compressed chunk without safe preflights or resumability. | Phase 7.6 |
| OPS-03 | P1 | No general backup, PITR, scheduled restore test, or capacity/retention gate exists. | Minimum preflight in Phase 0.4; full system in Phase 7.3 |
| OPS-04 | P2 | Promtail positions live under ephemeral `/tmp`, causing replay after restart (`logging/promtail.yaml:5-9`, `docker-compose.yml:750-760`), and Promtail is now EOL. | Migrate to Alloy in Phase 7.2 |
| OPS-05 | P2 | Prometheus scrapes only backend (currently 404), with no DB/Redis/MQTT/worker/container/disk exporters, Alertmanager, or rules. | Minimum in Phase 0.4; expansion in Phases 7.1/7.2 |
| OPS-06 | P2 | Monitoring services lack health checks; provisioned dashboards are editable and the ML dashboard targets a disabled service. | Phase 7.2 |
| OPS-07 | P2 | Base/service images use mutable tags; Python has no hash lock; Dependabot omits Docker/pip; scripts lack a lock; no SBOM/signing/provenance. | Phases 1.7/7.5 |
| OPS-08 | P2 | Runtime containers generally lack explicit non-root user, read-only filesystem, capability drops, and no-new-privileges. | Phases 1.2/7.5 |
| OPS-09 | P2 | Quick Start omits required secrets and has impossible Mosquitto ordering; architecture/topology and Cloudflare instructions drift from Compose. | Phases 0.2/8.5 |
| OPS-10 | P2 | Operations docs lack release/rollback, backup/restore, dead-letter, SRTM cache, capacity, alert delivery, and disaster-recovery runbooks. | Phases 7/8.5 |

### Frontend correctness, performance, accessibility, and maintainability

| ID | Pri | Finding and evidence | Planned resolution |
| --- | --- | --- | --- |
| FE-01 | P1 | Spam Watch map ref does not exist when its one-shot effect runs, so the main map never initializes (`SpamTransparencyPage.tsx:434-477,527-531`). | Phase 5.1 |
| FE-02 | P1 | Packet arcs are never populated and overlay receives `arcs={[]}` despite product claims (`useNodes.ts:83-97`, `LiveOverlayController.tsx:177-183`). | Phase 5.2 |
| FE-03 | P1 | Inferred nodes are polled but ignored by MapLibre/GeoJSON, which hard-codes `is_inferred: false` (`App.tsx:286-331`, `MapLibreMap.tsx:99-123`, `geojsonBuilders.ts:94-158`). | Phase 5.2 |
| FE-04 | P1 | Link updates never delete nonviable links and empty snapshots do not clear old network state (`useLinkState.ts:76-137`). | Phase 5.3 |
| FE-05 | P1 | Repeater details omit displayed fields, reject longitude zero, and allow A→B request races (`UKRepeaterSearchPage.tsx:183-190,236-255,379-409`). | Phase 5.5 |
| FE-06 | P1 | Stored Feed region resets to `all` before asynchronous options load (`UKFeedPage.tsx:395-401,485-510,546-551`). | Phase 5.4 |
| FE-07 | P1 | Packets without path hashes are skipped by resolution but rendered as permanently loading (`UKFeedPage.tsx:635-638,716-718,869-878,918-925`). | Phase 5.4 |
| FE-08 | P1 | Propagation formatter includes units and JSX appends another `s` (`PacketDetailPanel.tsx:470-479,561-565`). | Phase 5.5 |
| FE-09 | P1 | Observed packet count increments once per animation-frame batch, not per packet (`useAppMessageHandler.ts:181-184`, `useDashboardStats.ts:36-40`). | Phases 5.3/5.5 |
| FE-10 | P1 | Stats ignores response status and renders failures blank; its raster style lacks glyphs for a text layer (`StatsPage.tsx:234-247,303-315,333-367,429-442`). | Phase 5.5 |
| FE-11 | P1 | Feed ignores actual WebSocket ready state and infers connectivity only from packet age (`UKFeedPage.tsx:452-453,685-687`). | Phase 5.4 |
| FE-12 | P2 | Owner map is destroyed/rebuilt on each ten-second data refresh (`OwnerPortalPage.tsx:683-841,982-1015`). | Phase 6.1 |
| FE-13 | P2 | Owner/session/live/last-hop polling overlaps, continues off-tab, and lacks consistent timeout/cancellation (`OwnerPortalPage.tsx:890-923,982-1052`). | Phase 6.1 |
| FE-14 | P2 | Planned coverage polls every two seconds indefinitely without visibility gating/backoff (`MapLibreMap.tsx:329-357`). | Phase 6.1 |
| FE-15 | P2 | Observer health is fetched once, unscoped, and not refreshed on network switch (`MapLibreMap.tsx:775-819`). | Phase 6.1 |
| FE-16 | P2 | Node link, packet, repeater, owner last-hop, and Feed path module caches are unbounded and incompletely scoped. | Phase 6.1 |
| FE-17 | P2 | Feed starts background path resolution for every recent path packet rather than on user/viewport demand (`UKFeedPage.tsx:711-750`). | Phases 5.4/6.1 |
| FE-18 | P2 | Packet detail caches global radio stats under packet keys and calls path API without hashes (`usePacketDetailData.ts:31-55,87-109`). | Phase 5.5 |
| FE-19 | P2 | Terrain sampler may retain about 200 MB decoded data, has no in-flight dedupe, FIFO eviction, and never closes bitmaps (`terrainSampler.ts:7-12,39-58`). | Phase 6.2 |
| FE-20 | P2 | Service worker prunes with `cache.keys()` on every write, allows 8,000/~120 MB tiles, and update handling can force reload (`public/sw.js:1-6,25-52`, `main.tsx:36-56`). | Phase 6.2 |
| FE-21 | P2 | Topology copies every simulation node into React state on every D3 tick (`TopologyPage.tsx:90-122`). | Phase 6.2 |
| FE-22 | P2 | Spam/Feed fixed-height virtualization conflicts with expanded detail rows. | Phase 6.2 |
| FE-23 | P2 | Link sparklines omit network/observer scope and up to eight independent requests mount per popup (`LinkQualitySparkline.tsx:14-21`, `NodePopupContent.tsx:182-201`). | Phase 6.1 |
| FE-24 | P2 | Dialogs lack focus trap/restore and consistent Escape handling (`DisclaimerModal.tsx:8-42`, `StatsPage.tsx:936-975`, `UKFeedPage.tsx:983-1031`). | Phase 6.3 |
| FE-25 | P2 | Tabs lack full tab/tabpanel and keyboard semantics (`NodePopupContent.tsx:62-73`, `StatsPage.tsx:444-457`). | Phase 6.3 |
| FE-26 | P2 | Packet rows emulate buttons incompletely; Feed articles are not keyboard focusable (`PacketFeed.tsx:49-76`, `UKFeedPage.tsx:855-868`). | Phase 6.3 |
| FE-27 | P2 | Node/repeater searches lack combobox/listbox labels, active descendant, and arrows (`NodeSearch.tsx:114-140`, `UKRepeaterSearchPage.tsx:269-310`). | Phase 6.3 |
| FE-28 | P2 | Range input visible labels are not programmatically associated (`FilterPanel.tsx:153-166`, `MobileControls.tsx:88-101`). | Phase 6.3 |
| FE-29 | P2 | Reduced-motion handling still permits pulse/timer/page animation (`globals.css:262-265`, `StatsPanel.tsx:14-37`). | Phase 6.3 |
| FE-30 | P2 | Watchlist can only display/remove and is absent from mobile controls (`WatchlistPanel.tsx:21-28`, `MobileControls.tsx:37-109`). | Phase 8.4 |
| FE-31 | P2 | MapLibre, Owner, Feed, Stats, and packet-detail components range from ~845 to 1,652 lines and mix data, polling, rendering, and map lifecycle. | Phase 6.4 |
| FE-32 | P2 | Eleven CSS files total 7,761 lines with 71 duplicate selectors and load-order risk. | Phase 6.4 |
| FE-33 | P3 | No wildcard/404 route exists (`frontend/src/main.tsx:69-89`). | Phase 8.4 |
| FE-34 | P3 | Build-time SEO/sitemap routes and runtime SEO routes drift (`vite-seo.ts:5-54,139-234`, `config/seo.ts:8-85`). | Phase 8.4 |
| FE-35 | P3 | Architecture docs reference nonexistent `NodeDetailDrawer.tsx`; README route/feature claims drift. | Phase 8.5 |

## 17. Suggested pull-request slices and dependencies

Keep each slice independently testable and reversible. Parallel work is safe
only where the dependency column permits it.

| Order | Slice | Depends on |
| --- | --- | --- |
| 1 | CI dependency patches, frontend unit gate, worker-image test skeleton, clean Mosquitto bootstrap | none |
| 2 | Migration lock/checksum ledger, pinned Timescale snapshot/restore preflight, known-good digests, internal metrics endpoint | slice 1 |
| 3 | Website `/ws` identity forwarding and proxy E2E test | slice 1 test harness only |
| 4 | Network/channel ADR, coordinate DTO boundary, link-history scope, planned-node containment, trusted local-only check | slice 2 |
| 5 | Webhook egress policy and delivery/outbox schema | slice 2 plus allowlist/egress decision |
| 6 | Broker-local ACL helper, backend non-root, Docker socket removal | slice 1 clean-stack CI |
| 7 | Duplicate map route plus chosen completeness contract, shared parsers/error boundary, operator session | slice 4 visibility/map contract |
| 8 | Graceful lifecycle coordinator | queue/batch fixtures plus core metrics |
| 9 | Analysis leases, legacy orphan cleanup, additive job-state migration | slice 2 migration/backup foundation |
| 10 | Terrain acquisition/result types, legacy empty-row repair, backfill fix | worker-image tests |
| 11 | Tactical live-viewshed cap, then one queue-v3/viewshed protocol with bounded DLQ and database fencing | slices 2 and 10 plus essential alerts |
| 12 | Ingest write consolidation and historical-migration/backfill runbook | database integration fixtures |
| 13 | Stats aggregates/shadow reads and cache bounds | visibility predicate plus verified restore |
| 14 | Frontend broken-feature regressions, runtime kill switch, and fixes | scoped privacy-safe API fixtures |
| 15 | Scoped stores, polling/cache utilities, browser resource bounds | slice 14 |
| 16 | Accessibility primitives and component/CSS decomposition | stable behavior from slices 14–15 |
| 17 | Metrics/exporter expansion, Alloy migration, alerts, dashboards | stable queue/job semantics |
| 18 | Signed/pinned images, schema-compatible immutable deploy, automated backup/restore, safe maintenance | clean build plus core metrics |
| 19 | Retention/compression activation | aggregate cutover plus successful restore drill |
| 20 | Owner alert UI, observer workflow, job admin, watchlist/SEO/API docs | foundational slices complete |

Where migration number conflicts arise, select the next unused number at
implementation time and preserve the semantic filename. Never create a second
new migration with a number already present.

## 18. Definition of done

The improvement program is complete when:

- All P1 items are fixed and backed by regression tests; all P2 items are fixed
  or have an explicit owner/date/accepted-risk record.
- The seven security findings are revalidated as closed, public privacy
  contracts pass across REST/WS/export/cache/derived data, and no public backend
  has host Docker authority.
- A fresh checkout can build and start the documented stack without hidden
  local files.
- Accepted packets and jobs have deterministic acknowledgement, drain, lease,
  retry, terminal, cleanup, and recovery behavior.
- Database migrations are locked/checksummed and contain no unbounded historical
  rewrite; backfills are resumable and observable.
- Map/stats queries and RF/browser workloads meet baselined budgets without
  correctness or privacy drift.
- Frontend primary flows work across desktop/mobile, network switching cannot
  leak stale state, and representative routes pass keyboard and axe checks.
- Metrics, alerts, runbooks, backup restore, and failed-deployment rollback are
  demonstrated in CI or a controlled staging drill.
- Releases use pinned inputs and signed immutable images with SBOM/provenance.
- OpenAPI and operator/user documentation match the implemented runtime.

For handoff, the implementing agent should reference these issue IDs in commits
and PRs, update this file with status and measured before/after results, and
attach the exact test/rollout/rollback evidence for each completed slice.
