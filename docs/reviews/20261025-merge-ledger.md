# 2026-10-25 deployed-tree reconciliation ledger

Worktree: `/home/ben/ukmesh/agent-worktrees/reconcile-20261025`.
Sole writer: reconcile owner; branch `reconcile/main-into-vps-20261025`.
Starting deployed-tree capture: `1c6d95d`; fetched main: `6438089e`;
required security branch: `b7a744b`. No push, deploy, service restart or live DB write.
An untracked pre-reconcile env file was moved, unread, into a mode-0700 private
notes directory outside the repository (file mode 0600); it was never staged.

## Main merge — 52 conflicts

All original conflicted paths are listed below, including add/add and binary
conflicts. “Merged / main hunks” and “Merged / VPS hunks” retain the automatic
three-way merge outside the conflict regions. Decisions follow VPS behavior
plus main fixes. No unresolved paths remain for this merge.

| Path | Type | Chosen side | Reason | Evidence |
| --- | --- | --- | --- | --- |
| `.env.example` | content | Merged / main hunks | Union env keys and add bounded prewarm concurrency. | Key/service-set comparison; Compose CI-overlay config; infra/adoption fixtures |
| `.gitignore` | content | Merged / main hunks | Union existing ignores with main private-evidence exclusions. | Key/service-set comparison; Compose CI-overlay config; infra/adoption fixtures |
| `backend/package.json` | content | Merged / main hunks | Add main owner session revocation command to merged script set. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/api/contracts.ts` | content | Merged / main hunks | Main adds unattributed region contract; retain merged owner and public contracts. | `backend: contract:generate; contract:check` |
| `backend/src/api/routes/misc.ts` | content | Merged / main hunks | Main adds public telemetry DTO and region-scoped pending overlays. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/api/routes/owner.ts` | content | Merged / main hunks | Main rolls feed setting back when broker ACL reconciliation fails. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/api/routes/ownerPacketSharing.test.ts` | add/add | Merged / main hunks | Retain authentication and owned-node regression coverage. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/api/routes/regions.test.ts` | add/add | Merged / main hunks | Main adds unattributed shaping and route tests to regional coverage. | Region DTO/route/unit tests; full backend suite |
| `backend/src/api/routes/regions.ts` | add/add | Merged / main hunks | Main is a superset adding privacy-filtered unattributed geographic clusters. | Region DTO/route/unit tests; full backend suite |
| `backend/src/db/index.ts` | content | Merged / main hunks | Main dedicated ingest pool, transaction wrapper, regional reads and public payload sanitation; keep auto-merged VPS features. | `packetBatch.test.ts`, migration tests and full backend suite |
| `backend/src/db/packetBatch.integration.test.ts` | content | Merged / main hunks | Assert reviewed VPS migrations exist and complete loader-derived order across merged migrations. | `packetBatch.test.ts`, migration tests and full backend suite |
| `backend/src/db/packetBatch.test.ts` | content | Merged / VPS hunks | 27 columns per observation, including deployed durable forwarding payload, gives 54 values for two rows. | `packetBatch.test.ts`, migration tests and full backend suite |
| `backend/src/db/packetBatch.ts` | content | Merged / VPS hunks | Retain permanent data error handling and durable forwarding outbox in the atomic packet transaction. | `packetBatch.test.ts`, migration tests and full backend suite |
| `backend/src/db/packetInput.ts` | add/add | Merged / VPS hunks | Retain deployed forwarding envelope serialization for durable egress. | `packetBatch.test.ts`, migration tests and full backend suite |
| `backend/src/health/status.ts` | content | Merged / main hunks | Main unified path diagnostics, vacuum classification and privacy generation health. | `statusIntegrity.test.ts` and full backend suite |
| `backend/src/health/statusIntegrity.test.ts` | content | Merged / main hunks | Keep main privacy generation and vacuum regressions plus existing VPS assertions. | `statusIntegrity.test.ts` and full backend suite |
| `backend/src/index.ts` | content | Superset / manual hunk selection | Keep durable forwarding-outbox lifecycle, bounded repository calls and no duplicate forwarding; add main prewarm concurrency. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/metrics.test.ts` | content | Merged / main hunks | Keep main fail-closed privacy generation metric regression. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/mqtt/client.ts` | content | Superset / manual hunk selection | Main ingest-pool reads combined with VPS durable bounded intake, quarantine, integer validation and forwarding outbox. | `statusTelemetry.test.ts`, durable intake/envelope tests and full backend suite |
| `backend/src/mqtt/statusTelemetry.test.ts` | add/add | Superset / manual hunk selection | Union main firmware-shaped Will regressions with VPS integer-domain and status failure durability tests. | `statusTelemetry.test.ts`, durable intake/envelope tests and full backend suite |
| `backend/src/mqtt/statusTelemetry.ts` | add/add | Merged / VPS hunks | Preserve deployed integer normalization and deterministic data-error acknowledgement; retain main offline-Will guard. | `statusTelemetry.test.ts`, durable intake/envelope tests and full backend suite |
| `backend/src/owner/ownerLastHopPrewarm.ts` | add/add | Merged / main hunks | Main bounded concurrency and progress reporting supersede deployed sequential loop. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/owner/packetShareForwarder.ts` | add/add | Merged / VPS hunks | Keep deployed bounded publisher cleanup and durable enqueue error propagation. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/owner/publicFeedForwarder.test.ts` | add/add | Merged / VPS hunks | Preserve hung publish and cleanup recovery regression. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/owner/publicFeedForwarder.ts` | add/add | Merged / VPS hunks | Keep deployed publish/delivery/cleanup deadlines, durable error propagation and backlog-age metrics. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/owner/publicFeedRepository.ts` | add/add | Merged / VPS hunks | Keep deployed durable backlog count and age query. | Owner/share/feed targeted tests and full backend suite |
| `backend/src/ram/liveRead.ts` | add/add | Merged / main hunks | Keep main regional overlay and committed tag preservation. | RAM overlay tests and full backend suite (DB integration separate) |
| `backend/src/ram/liveStore.test.ts` | add/add | Merged / main hunks | Keep regional pending-overlay regression. | RAM overlay tests and full backend suite (DB integration separate) |
| `backend/src/ram/runtime.integration.test.ts` | add/add | Merged / main hunks | Integration lock observer follows dedicated ingest application name. | RAM overlay tests and full backend suite (DB integration separate) |
| `backend/src/ram/writerRuntime.ts` | add/add | Merged / main hunks | Use dedicated ingest pool for RAM writes. | RAM overlay tests and full backend suite (DB integration separate) |
| `backend/src/repositories/nodes.ts` | content | Merged / main hunks | Use region-filtered node source for link evidence. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/spam/cluster.test.ts` | content | Merged / main hunks | Main adds synthetic rotating burst and human chatter guards. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/spam/cluster.ts` | content | Merged / main hunks | Main bounded analysis and rotating-ID transmission floor improvements. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/spam/config.ts` | content | Merged / main hunks | Main configurable rotating-ID floor and tuned machine-name share. | Full backend suite and typecheck; three-way hunk inspection |
| `backend/src/tools/rebuildRegionParticipation.ts` | add/add | Merged / main hunks | Main adds privacy-filtered unnamed cluster rebuild alongside existing region tables. | Region DTO/route/unit tests; full backend suite |
| `backend/src/workers/alert-receiver.test.ts` | content | Merged / main hunks | Main retry assertion matches pre-security receiver. Security merge will use its durable .queue implementation and revalidate authenticated forwarding. | Alert queue/receiver, privacy remat and full backend suite |
| `backend/src/workers/alertDeliveryQueue.ts` | add/add | Merged / VPS hunks | Keep bounded shutdown drain and admission close state. | Alert queue/receiver, privacy remat and full backend suite |
| `backend/src/workers/privacy-rematerialization.ts` | add/add | Merged / VPS hunks | Retain deployed expiring token-fenced leases; port main generation-gap preservation in completion transaction. | Alert queue/receiver, privacy remat and full backend suite |
| `backend/src/ws/server.ts` | content | Merged / main hunks | Main scopes initial state, pending overlays and cache keys by region. | Full backend suite and typecheck; three-way hunk inspection |
| `backup-receipts/latest.json` | content | VPS | Preserve deployed receipt and its matching signature as one pair. | Deployed receipt/signature retained together byte-for-byte |
| `backup-receipts/latest.json.sig` | binary | VPS | Preserve deployed receipt and its matching signature as one pair. | Deployed receipt/signature retained together byte-for-byte |
| `docker-compose.yml` | content | Merged / main hunks | Union service configuration with main prewarm settings; deployed service set preserved. | Key/service-set comparison; Compose CI-overlay config; infra/adoption fixtures |
| `docs/openapi.yaml` | content | Regenerated from merged contracts | Regenerate from merged contracts using committed generator. | `backend: contract:generate; contract:check` |
| `docs/spam-detection.md` | content | Merged / main hunks | Main documents newer bounded spam analysis and rotating-ID tuning. | Full backend suite and typecheck; three-way hunk inspection |
| `frontend/src/components/Map/RegionsMap.tsx` | add/add | Merged / main hunks | Main regional superset adds unnamed footprints; map reinstall handles style changes; preserve auto-merged VPS features. | `frontend: tsc --noEmit; npm test; npm run build` |
| `frontend/src/components/Map/regionOverlay.test.ts` | add/add | Merged / main hunks | Main regional superset adds unnamed footprints; map reinstall handles style changes; preserve auto-merged VPS features. | `frontend: tsc --noEmit; npm test; npm run build` |
| `frontend/src/components/Map/regionOverlay.ts` | add/add | Merged / main hunks | Main regional superset adds unnamed footprints; map reinstall handles style changes; preserve auto-merged VPS features. | `frontend: tsc --noEmit; npm test; npm run build` |
| `frontend/src/pages/RegionsPage.tsx` | add/add | Merged / main hunks | Main regional superset adds unnamed footprints; map reinstall handles style changes; preserve auto-merged VPS features. | `frontend: tsc --noEmit; npm test; npm run build` |
| `frontend/src/pages/regions-page.css` | add/add | Merged / main hunks | Main regional superset adds unnamed footprints; map reinstall handles style changes; preserve auto-merged VPS features. | `frontend: tsc --noEmit; npm test; npm run build` |
| `frontend/src/pages/regionsModel.ts` | add/add | Merged / main hunks | Main regional superset adds unnamed footprints; map reinstall handles style changes; preserve auto-merged VPS features. | `frontend: tsc --noEmit; npm test; npm run build` |
| `rf-coverage/config.ukmesh.yaml` | content | Merged / VPS hunks | Retain VPS remote GPU stall parking timeout; main has no replacement. | VPS-only stall timeout retained by content comparison |
| `tagger-worker/Dockerfile` | add/add | Merged / main hunks | Main pins the existing Python base image digest. | Main pinned Python digest; final Python syntax check |

## Migration parity and checksum proof

A read-only transaction against the live `schema_migrations` ledger found 62
applied files; every applied filename is in the merged runtime directory and
every SHA-256 matches. No live ledger changes were made or are required for
those applied files. Preserve the historically shipped duplicate prefixes 016,
052 and 055 exactly as allowed by `validateMigrationPrefixes`.

Main's **unapplied** `062_prefix_visibility_statement_fence.sql` collided with
live `062_silent_failure_recovery.sql`. The main file is moved byte-for-byte to
`066_prefix_visibility_statement_fence.sql`; both source and destination hash:
`29e8812c08eb02cdadcede35512640885c3f9dd64060eb3f775bbe7a96c51de7`.
Neither source name nor destination name exists in the live ledger. The applied
062 retains checksum
`074f7bc5b904075a23b4f7121f2ad230e14d478990d14538675dd60874d125dc`.
The loader now accepts the complete chain; a regression exercises the real
file set and rejects any newly introduced 062 collision. Pending additive
files are 063, 064, 065, 066. No migration was executed on production.

The deferred 057 status-column note is corrected: the live ledger proves that
061 already installed that status column; the deferred copy remains outside
the runtime chain. The integration expectation uses the actual loader order,
while independently requiring the reviewed production migration baseline.

## Conflict-free merge corrections

- Removed duplicate metric declarations and duplicate owner UI components
  produced by automatic merging; retained the superset of actual behavior.
- Kept VPS durable forwarding-outbox lifecycle and 27-column packet input,
  bounded feed/publisher operations, MQTT admission queue, deterministic poison
  quarantine, integer status normalization, lease fencing and shutdown drains.
- Combined main privacy-rematerialization generation-gap protection with VPS
  claim-token/expiry fencing. A node-scoped pass cannot certify unrelated drift.
- Restored VPS MQTT queue depth/full alert rules removed by automatic merging;
  retained main worker-heartbeat, vacuum and privacy generation fixes.
- Preserved all deployed Compose services and interpolation keys, including
  every live-supplied key referenced by the deployed Compose file. Main's
  enforce/apply owner defaults and public-channel controls are retained.
- Compose alone intentionally lacks infrastructure definitions. Configuration
  validation uses `docker-compose.ci.yml` and fixture credentials only: 31
  services render successfully; no containers are started by that check.

## Validation checkpoints

- Config/docs/infra resolved first; an initial typecheck attempt found no local
  dependencies. Clean backend/frontend `npm ci --ignore-scripts` then succeeded
  with zero audit findings. Intermediate typechecks before backend/frontend
  conflict resolution reported only unresolved syntax and subsequent merge
  defects; final gates below supersede them.
- Migration tests initially 3/3; real-tree regression added as the fourth.
- Backend DB/MQTT/health/owner/regions targeted cluster: 48/48 pass.
- Main backend `tsc --noEmit` passes after removing the unused forwarding hook
  argument (durable outbox owns that handoff).
- Initial main backend suite: 673 total, 669 pass, 1 failure, 3 DB-gated skips.
  Failure was a VPS assertion looking for a not-yet-wired `.outbox` directory;
  main's matching pre-security retry assertion was retained. Security supplies
  the durable receiver implementation and is validated again after merging.
- Re-run main backend suite after the retry-assertion resolution: **673 total,
  670 pass, 0 fail, 3 DB-gated skips**; posttest receiver smoke also passes.
- Main frontend production build passes. `tsc --noEmit` passes; default suite: 166 total, 156 pass,
  0 fail, 10 opt-in browser tests skipped. Browser validation recorded below.
- OpenAPI regenerated; `npm run contract:check` passes (72 API / 11 operator).
- Frontend topology browser fixture accepts an isolated base URL, matching the
  owner/region fixtures; this avoids using the live website port during tests.
- Infra resolver fixtures pass; Compose adoption fixtures pass 9/9 using fake
  Docker exclusively. Receipt and signature are unchanged from VPS.

## Security merge — 61 conflicts

Security carries an older production synchronization as well as the remediation
commits. The final code retains newer reconciled main/VPS behavior for those
older synchronization differences and ports the remediation itself. In
particular, the merge does not roll back bounded stats queries, credential
verifier freshness, revocation-write failure propagation, status-column writes,
region features, owner diagnostics, or modern frontend/CI/dependency changes.

| Path | Type | Chosen side | Reason | Evidence |
| --- | --- | --- | --- | --- |
| `.env.example` | content | Superset | Preserve all main/VPS keys and document the new alert token and separated receipt trust configuration. | Final env/service parity; fixture-only Compose render; ignore checks |
| `.github/workflows/ci.yml` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `.gitignore` | content | Superset | Preserve main private-evidence ignores; prevent staging alert bearer file, trust directory or pre-reconcile env snapshots. | Final env/service parity; fixture-only Compose render; ignore checks |
| `.gitleaks.toml` | content | Merged / reconciled main-VPS hunks | Retain main narrower explicit public-key allowances; security-side sync allowances are older and unrelated to the 19 fixes. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/package-lock.json` | content | Reconciled main/VPS | Keep newer audited main lock matching merged package manifest. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/contracts.ts` | content | Merged / reconciled main-VPS hunks | Keep newer main unnamed-region contracts and auto-merged security contracts. | Regenerated contract and contract:check |
| `backend/src/api/routes.ts` | content | Merged / security hunks | Fail closed when credential-generation lookup cannot verify a session. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/routes/auditwave.integration.test.ts` | add/add | Merged / reconciled main-VPS hunks | Keep main newer regional/privacy API integration coverage; adapt export generation fixture below. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/routes/auditwave.performance.test.ts` | add/add | Merged / reconciled main-VPS hunks | Keep main newer scoped API performance coverage; adapt export generation fixture below. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/routes/exports.test.ts` | content | Merged / security hunks | Add security export generation mismatch, failure and mid-flight redaction regression tests. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/routes/misc.test.ts` | content | Merged / reconciled main-VPS hunks | Retain main public MQTT DTO and region filtering regressions. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/routes/owner.ts` | content | Merged / reconciled main-VPS hunks | Retain main onboarding diagnostics and transactional feed-setting rollback. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/api/routes/ownerPacketSharing.test.ts` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/api/routes/regions.test.ts` | add/add | Merged / reconciled main-VPS hunks | Keep main newer unattributed-region tests. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/api/routes/regions.ts` | add/add | Merged / reconciled main-VPS hunks | Keep main newer unattributed-region feature absent from old security snapshot. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/db/ownerAuth.ts` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/db/packetBatch.integration.test.ts` | content | Merged / reconciled main-VPS hunks | Retain loader-derived order and actual deployed migration filenames. | 62 live checksums; 66-file loader; no live migration run |
| `backend/src/index.ts` | content | Manual superset | Union live-grant wiring and redacted readiness with VPS durable forwarding lifecycle, bounded repositories and main monitor readiness. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/metrics.ts` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/mqtt/client.ts` | content | Superset | Keep bounded durable intake/outbox and normalized status validation; port envelope stripping and packet domain checks; forward sanitized envelope durably. | Envelope/unverified/status/durable-intake tests; full backend gate |
| `backend/src/mqtt/envelopeSafety.test.ts` | add/add | Union | Keep both security domain/quarantine regressions and VPS strict protocol bound regression. | Envelope/unverified/status/durable-intake tests; full backend gate |
| `backend/src/mqtt/envelopeSafety.ts` | add/add | Superset | Security domain validator and SQLSTATE 22/23 policy plus deployed stricter protocol bounds. | Envelope/unverified/status/durable-intake tests; full backend gate |
| `backend/src/owner/ownerAccess.ts` | content | Superset | Use verified grants only and security error class; retain main credential verifier freshness, safe integer generation checks and throwing revocation writes. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/ownerLastHopPrewarm.test.ts` | modify/delete | Reconciled main/VPS | Retain main bounded-concurrency tests; security branch deletion is from older snapshot. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/ownerLastHopPrewarm.ts` | content | Merged / reconciled main-VPS hunks | Preserve newer main bounded concurrency and progress reporting. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/ownerRepository.ts` | content | Merged / security hunks | Bind owner reads to exact live-granted node ID; remove canonical alias scope expansion. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/packetShareForwarder.test.ts` | content | Merged / security hunks | Wire explicit positive live-grant fixtures under fail-closed forwarding. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/packetShareForwarder.ts` | content | Merged / security hunks | Adopt live grant checks at enqueue and delivery; retain auto-merged VPS deadlines and retry propagation. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/packetShareRepository.integration.test.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve deployed 058 owner-scope migration name. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/packetShareRepository.ts` | content | Merged / security hunks | Filter targets through live owner grants; false dead-letters, unknown defers. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/publicFeedForwarder.test.ts` | add/add | Merged / reconciled main-VPS hunks | Keep VPS hung publish/cleanup recovery regression. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/publicFeedForwarder.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve deployed bounded delivery, outbox error propagation and queue-age metrics. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/owner/publicFeedRepository.ts` | add/add | Merged / reconciled main-VPS hunks | Retain deployed queue statistics. | Grant/credential/share/feed regressions; full backend gate |
| `backend/src/path-learning/rebuild.ts` | content | Merged / security hunks | Use canonical packet privacy in training SQL; retain scoped/model improvements outside conflict. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/ram/liveRead.ts` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/repositories/nodes.ts` | content | Merged / reconciled main-VPS hunks | Preserve regional node source; security change is an older unscoped version. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/spam/cluster.test.ts` | content | Merged / reconciled main-VPS hunks | Retain newer main rotating-burst and bounded-analysis regressions. | Canonical privacy and bounded analysis regressions; full backend gate |
| `backend/src/spam/cluster.ts` | content | Merged / reconciled main-VPS hunks | Retain newer main rotating-ID analysis. | Canonical privacy and bounded analysis regressions; full backend gate |
| `backend/src/spam/config.ts` | content | Merged / reconciled main-VPS hunks | Retain newer main bounded analysis configuration. | Canonical privacy and bounded analysis regressions; full backend gate |
| `backend/src/spam/repository.ts` | content | Superset | Canonical packet privacy at ingestion and public incident/timeline reads plus main truncation budgets and additional name/content opt-outs. | Canonical privacy and bounded analysis regressions; full backend gate |
| `backend/src/tools/rebuildRegionParticipation.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main unnamed-cluster rebuild. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backend/src/workers/alert-receiver.ts` | content | Security durable receiver + main/VPS protections | Use authenticated fsynced queue; retain archive-chain recovery, graceful drain, configuration diagnostics and successful-delivery readiness gate. | Authenticated local receiver and durable queue tests; full backend gate |
| `backend/src/workers/synthetic-monitor.ts` | content | Merged / reconciled main-VPS hunks | Keep VPS durable independent alert evaluator/archive timeout; add security receiver bearer token to its send function. | Authenticated local receiver and durable queue tests; full backend gate |
| `backend/src/ws/server.ts` | content | Merged / reconciled main-VPS hunks | Main already handles malformed scope including region errors; add explicit security InvalidPublicNetworkScopeError catch without losing either existing catch. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `backup-receipts/latest.json` | content | VPS receipt pair | Keep deployed signed receipt and signature together. | Receipt and signature match deployed commit byte-for-byte |
| `backup-receipts/latest.json.sig` | binary | VPS receipt pair | Keep deployed signed receipt and signature together. | Receipt and signature match deployed commit byte-for-byte |
| `docker-compose.yml` | content | Merged / reconciled main-VPS hunks | Preserve main prewarm keys and union auto-merged receiver token/trust-anchor settings. | Final env/service parity; fixture-only Compose render; ignore checks |
| `docs/openapi.yaml` | content | Reconciled main/VPS | Regenerate from final merged contracts; preserve main unnamed-region schema as starting point. | Regenerated contract and contract:check |
| `docs/operations.md` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `docs/spam-detection.md` | content | Merged / reconciled main-VPS hunks | Retain newer main analysis bounds and tuning documentation. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `frontend/playwright.config.ts` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/components/Map/RegionsMap.tsx` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/components/Map/regionOverlay.test.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/components/Map/regionOverlay.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/pages/RegionsPage.tsx` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/pages/TopologyPage.browser.test.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/pages/regions-page.css` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/src/pages/regionsModel.ts` | add/add | Merged / reconciled main-VPS hunks | Preserve newer main/VPS frontend superset and isolated browser fixture. | Retained newer main implementation; frontend typecheck/default tests/build |
| `frontend/test/e2e/dashboard.spec.ts` | content | Whole reconciled main/VPS | Retain newer reconciled behavior; security-side difference is older snapshot synchronization outside its 19-finding patch set. | Retained newer main implementation; frontend typecheck/default tests/build |
| `tagger-worker/Dockerfile` | add/add | Merged / reconciled main-VPS hunks | Keep main pinned Python digest; security changes concern tagger runtime, merged separately. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |
| `vacuum-compressed-chunks.sh` | content | Merged / reconciled main-VPS hunks | Retain production-proven/main tested infra Compose resolution; old security variant is superseded. | Three-way review, final typecheck/tests and relevant 19-finding evidence below |

### Additional security reconciliation evidence

The security branch reintroduced three historical migration names. Each is an
**exact SHA-256 match** for an already-applied file under the VPS ledger name;
the redundant names are excluded from the runtime tree to avoid replaying SQL.
No applied migration was renamed and no live ledger UPDATE was performed.

| Security historical name | Retained applied name | Exact SHA-256 on both files |
| --- | --- | --- |
| `047_message_tags.sql` | `055_message_tags.sql` | `15ff8c67d33496d9de329388828944ea802c7ca6e925f20e2c188e1d9f7ae97c` |
| `060_region_scope_participation.sql` | `057_region_scope_participation.sql` | `5268a785e585ac574a4d6c8cd4557d29655501278c9ea6b6e4def88b2d0745d4` |
| `061_packet_share_owner_scope_and_jwt.sql` | `058_packet_share_owner_scope_and_jwt.sql` | `2b421801c0957873dfc2f04786061d44c4fd3ff884b394361760c5441e0a6e43` |

The security validator checks the **normalized values actually inserted** for
status telemetry. Firmware fractional uptime/airtime remains valid, whole
seconds go into integer columns and raw stats preserve precision. Packet
protocol bounds remain stricter than the SQL integer domain; non-finite radio
values are rejected. Durable forwarding receives the same stripped upstream
envelope as live consumers, so `_summary`/`decrypted` cannot bypass sanitization
through the outbox. SQLSTATE 22/23 failures use the security permanent-error
classification; retryable operational failures still withhold acknowledgement.

Main's credential verifier and throwing generation-bump writes supersede the
older security snapshot's cached verifier and swallowed bump errors. Read
failures still deny sessions; verified grants are the sole portal authority.
The receiver combines security's authenticated fsynced queue with main/VPS's
successful-forward readiness gate, archive-chain recovery and graceful drain.
Its fixture now authenticates real local HTTP requests, checks durable dead
letters, and proves missing/wrong/unconfigured credentials cannot archive work.
The onboarding fixture now supplies explicit confirmations for approved grants
and separately proves broker log discovery cannot create an unconfirmed grant.

The extra monitoring run found a stale fixture: it expected a health-worker
alert at 200 seconds even though main correctly uses a 900-second threshold.
The stopped-worker input is corrected to 1,000 seconds. A focused replay passes;
the final complete rules-suite result is recorded below.

### 19-finding remediation mapping

IDs below enumerate the 19 audit keys recorded in remediation commit bodies
`18e6c96` through `0b6a352`. They are distinct from the older SEC-01..SEC-16
catalog in `docs/security-closure.md`. `722075e` is the position-prior follow-up
to the same path-learning privacy finding. No finding is dropped.

| ID | Original audit key / finding | Disposition | Merged implementation and evidence |
| --- | --- | --- | --- |
| S01 | observer-heard-set-node-scope-missing-publicPacketPrivacySql | ported | `api/utils/networkFilters.ts` and `db/index.ts` both embed canonical packet privacy in observer heard sets; network-filter/DB scope tests. |
| S02 | spam-incident-publication-bypasses-packet-privacy | ported | `spam/repository.ts` gates message/observer selection and list/detail/timeline member reads; `repository.privacy.test.ts` plus existing bounded-analysis tests. |
| S03 | observerHealthRows-public-packet-privacy-omitted | ported | `repositories/productFeatures.ts` gates packet health counts; public observer health uses private/no-store in `routes/productFeatures.ts`. |
| S04 | training-packet-selection-omits-packet-privacy | ported | `path-learning/rebuild.ts` gates network/training selection and publication of prefix/prev-prefix priors. `pathingRepository.ts` and `path-lazy/lazyResolver.ts` filter stored prefixes, including position priors; `rebuild.privacy.test.ts`. |
| S05 | tag_message.decrypted-reread-without-visibility-or-generation-fence | ported | Tagger re-reads exact hash/time/receiver/network with canonical privacy and suppresses no-longer-public retries; Python syntax plus source inspection; no live tagger invocation. |
| S06 | GET-readyz.publishes-owner-authorization-and-analysis-internals | ported | `index.ts` emits only database and MQTT state publicly; retains main drain, ACL and connection-monitor readiness conditions internally. |
| S07 | ws.verifyclient.invalid-network-scope-uncaught-exception | ported | `ws/server.ts` catches InvalidPublicNetworkScopeError and returns 400; keeps main ApiInputError/region scope handling and admission metric. |
| S08 | POST-alerts:unauthenticated-alert-ingest-to-operator-webhook | ported | Constant-time bearer check before parsing/archiving; synthetic sender and Alertmanager carry token. Local HTTP regressions cover 401/503 and successful authenticated forwarding. |
| S09 | includePrivacy-false-totals-published-to-anonymous-charts | ported | All totalFilters substitutions from security are ported onto main stats code; preserves timeout/cancellation, bounded rows, fixed snapshot and per-network aggregate switches. |
| S10 | owner-credential-generation-revocation | already-in-main | Main already throws on generation read/write failures and uses a fresh credential verifier. Retain those stronger controls; security session failure projection/error type is ported. Credential-verifier and owner authority tests. |
| S11 | shadow-fallback-regrants-revoked-acl-scope | ported | `ownerAccess.ts` uses only verifiedOwnerGrantSource in enforce and shadow modes; empty grants remain empty. `ownerAccess.test.ts` and `ownerAuthorityScope.test.ts`. |
| S12 | newuser-broker-log-node-key-promoted-to-operator-config-grant | ported | `scripts/newuser.sh` requires exact explicit --confirm-owner-grant key set; fake-Docker fixture covers no-confirm explicit/discovery flows, confirmed concurrent updates and rollback. |
| S13 | packet-share/forward-authority-not-bound-to-owner-grant | ported | Repository and forwarder check the same injected live grant at target selection, enqueue and delivery. False dead-letters; unknown defers. index.ts wires both layers; ownerAuthorityScope/forwarder tests. |
| S14 | fetchOwnerLiveData:canonical-alias-expansion-unbound | ported | `ownerRepository.ts` binds exact granted node and removes canonical/alias expansion; retains main optimized indexed query structure. `ownerAuthorityScope.test.ts`. |
| S15 | numeric-envelope-int-cast-ingest-livelock | ported | Security integer/float validator plus VPS stricter protocol bounds and normalized status writer. Permanent SQLSTATE classes 22/23 quarantine; operational errors retry. Envelope/status/durable-handler tests. |
| S16 | unverified-envelope-as-decoded-content | ported | `buildPersistedPacketPayload` strips publisher-derived fields unless decode is verified; live and durable egress receive stripped envelopes. `unverifiedPayload.test.ts`. |
| S17 | nodes-export:public-cache-not-keyed-to-visibility-generation | ported | `routes/exports.ts` adds generation read/re-read, ETag/Vary, 412/no-store on requested generation mismatch and 503/no-store on unavailable authority. `exports.test.ts`. |
| S18 | keyless-signing-identity-not-bound-to-release-ref | ported | Release signing is removed from image matrix and restricted to refs/tags/v* in a release-signing environment/job with an in-script ref guard; all 8 image/sign components match. Source audit only: GitHub environment protections and workflow execution are not claimed. |
| S19 | restore-receipt-verify-key-published-with-receipt | ported | Verifier and restore drill require a separate trust directory plus out-of-band SHA-256 pin and reject co-located/symlinked keys; Compose separates mount. `backup/receipt.test.ts`, lifecycle tests and shell syntax checks. |

## Final validation and handoff

Required final gates are clean:

| Gate | Final result |
| --- | --- |
| Backend `npx tsc --noEmit` and `npm run build` | Pass |
| Backend `npm test` | **708 total; 705 pass; 0 fail; 3 DB-gated skips**, plus authenticated receiver smoke pass |
| Frontend `npx tsc --noEmit` and `npm run build` | Pass (existing bundle-size advisory only) |
| Frontend default `npm test` | **166 total; 156 pass; 0 fail; 10 opt-in browser skips** |
| `npm run contract:check` | Pass; 72 API and 11 operator routes |
| Migration loader / applied-file parity | 66 files accepted; all 62 applied files match live SHA-256; no unapproved prefix duplicates |
| Onboarding fake-Docker regression | Explicit confirmation, unconfirmed passive discovery, concurrency and all rollback stages pass |
| Compose render / fixture guards | 31 services; token/trust wiring verified; infra resolver pass; adoption fixtures 9/9 |
| Python and shell syntax | Tagger compile and changed shell scripts pass |

Merge hashes and cleanup audit are recorded below. The full Prometheus rules
suite completed successfully after the heartbeat fixture correction. Evidence logs are outside the repository in
`/home/ben/.review-notes/ukmesh-merge-20261025/`.

Before any future staging/deploy (outside this task): provision a shared
`ALERT_RECEIVER_TOKEN` and matching ignored `logging/alert_receiver_token`, and
provision the receipt verify key in `RESTORE_RECEIPT_TRUST_HOST_DIR` with
`DATA_LIFECYCLE_RECEIPT_VERIFY_KEY_SHA256`. No secret values are recorded here.
The deployed receipt/signature pair remains unchanged; the new verifier will
fail closed until the separate trust anchor is supplied.

Optional browser validation limitation: the default frontend suite intentionally
skips 10 opt-in browser cases. Additional local attempts initially lacked host
libraries; privately extracted libraries restored Chromium launch. Authenticated
owner/map browser cases then timed out under software WebGL and were stopped.
A stale unnamed-region mock response was corrected, and topology can select an
isolated base URL. No browser visual parity or restored-database integration is
claimed by this reconciliation. The required default frontend/DB-gated backend
gates are reported separately from those optional attempts.

### Completion audit

- Main merge `882777e` has parents `1c6d95d` and `6438089`.
- Security merge `078b417` has parents `882777e` and `b7a744b`.
- Both requested refs are ancestors of the reconcile branch. The main merge
  message explicitly names all 52 conflicted paths and this ledger.
- Parsed ledger counts are exactly 52 main conflict rows, 61 security conflict
  rows and 19 finding dispositions; no finding is dropped.
- A fresh final live-ledger SELECT in a read-only transaction confirms all 62
  applied filenames and SHA-256 values still match; 063–066 remain pending.
- Every original deployed env-example key and Compose interpolation/service
  entry remains present. The receipt/signature pair remains byte-identical.
- `git log --oneline HEAD -- .env.pre-reconcile-20261020` is empty, the forbidden
  file is not tracked, and `.env.example` is the only tracked env-named file.
  Runtime alert tokens, trust material and pre-reconcile snapshots are ignored.
- No conflict markers or unresolved index entries remain. Required typechecks,
  builds, default test suites and contract checks are green at the security
  merge's source content; only this final documentation audit changes later.
- Isolated Vite test servers and task-owned Chromium processes were stopped.
  The final monitoring replay also completed successfully; no production
  services were restarted or deployed.
- No live checkout files, database rows or deployment configuration were
  modified. No push was performed. Both merge commits checkpoint the work
  well before 07:15 GMT.

Full monitoring validation: `promtool test rules logging/rules/meshcore.test.yml`
returned **SUCCESS**, exit 0. No task test/server process remains running.

Reconciliation completed and final ledger checkpointed at 2026-10-25 04:44:29 UTC.
The remaining handoff is an independent isolated staging/browser review and
provisioning of the documented alert token and receipt trust anchor before
any separately authorized deployment. This branch has not been pushed.
