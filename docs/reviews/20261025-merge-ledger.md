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

## Security merge and final gates

Pending security merge; this section is updated after its resolution and final
validation. Main merge decisions above remain the complete 52-file ledger.
