# Upgrading to UKMesh 2.0

Status: candidate preparation. Publication, production migration, cutover and
cleanup require a separately authorized release. Local branch qualification
does not authorize those operations.

## Release identity

The backend, frontend and contributor scripts identify version `2.0.0`, including
the root metadata in their lockfiles. Dependency versions are unchanged by this
identity bump. Run `node scripts/check-release-identity.mjs` from the repository
root to check package/lock consistency and the eight owned Dockerfile defaults.

Local images report `org.opencontainers.image.version=2.0.0` and the supplied
`SOURCE_REVISION` through their OCI labels. Build them with the actual clean
candidate SHA. The release workflow adds `io.ukmesh.release.version=2.0.0` to all
bundle images; Docker metadata supplies the published tag/SHA version label.
The vendored HopReach build keeps its own upstream `VERSION` argument. A label
alone proves neither the image contents nor a signature: inspect the actual
local image and retain its immutable ID, then verify published digests and
provenance only during the authorized release.

See the [candidate changelog](../CHANGELOG.md) for user and operator changes.

## Identify the starting deployment

Record the actual deployed source revision, immutable image digests for every
service, migration ledger, owner-auth schema/session layout and sanitized
configuration before selecting an upgrade path. A package version of `1.0.0`
does not identify that deployment. The checked-in website manifests dated
2026-08-13 are historical evidence and must not be regenerated as current state.

Bind the rehearsal and eventual release evidence to one candidate SHA. Keep
the pre-upgrade backup, image set, owner authorization inventory, old hashed
assets and rollback receipts for the entire rollback window.

## Schema and identity

The migration chain separates canonical metadata into `core`, packet and
observation data into `mesh`, read models into `analytics`, and privacy and
maintenance state into `ops`. Application/tool writes must address the base
relations, including `core.nodes`, `core.node_links`,
`core.node_status_samples` and `mesh.packets`. The `public` compatibility names
are projections for legacy consumers; they are not truncation targets and do
not establish compatibility with every historical write statement.

Run the explicit migration tool before starting candidate application images.
Application startup does not bootstrap or migrate either database. Bootstrap
backend and reader roles before the schema/grant migrations. Never edit an
already shipped migration checksum to make an upgrade pass.

The runner handles the post-move 074 case only when catalog checks prove that
the privacy tables moved and migration 134 can reassert the prefix fence.
Migration 135 records honest per-chunk identity-backfill progress. A fresh
`base.sql` fixture needs the runner's exact, shape-verified bootstrap approval;
that approval is not a general permission to create a ledger on an unknown
nonempty deployment. See the [migration contract](../backend/src/db/migrations/README.md).

Historical packet identity and multibyte/path read models need explicit bounded
backfill jobs. Inspect their completion counts, unresolved/mismatch reasons,
compressed-chunk disposition, source windows and privacy generation. Do not
interpret a process exit, an empty chart, or a `packets_partial` state as a
completed backfill. The identity tool currently implements the `packets` phase;
`paths` and `aliases` are not independently implemented CLI phases.

Privacy publication must remain fenced while materialization is incomplete.
Use the [privacy convergence runbook](privacy-convergence.md) and retain the
fresh generation checks before and after computation/publication. Cache keys
alone cannot revoke an already-fresh browser/CDN response.

Region snapshots and stored spam geometry now carry a privacy generation (migration
136). Existing rows remain uncertified and are withheld until their producer rebuilds
them. Start `region-snapshot-worker` from the same candidate backend image after
migrations and privacy preparation. It repairs the certificate and refreshes it
in the background; verify `meshcore_region_snapshot_healthy=1` and review its
[capacity settings](operations.md#region-snapshot-worker). Manual repair remains
available through `npm run regions:rebuild`. The spam analyzer publishes its own
certified incidents. A privacy change during a rebuild rejects its
publication. Public derived reads fail closed while privacy rematerialization is
pending and require HTTP revalidation, including conditional requests.

## Owner authorization and configuration

Preserve the separate owner-auth database unless the deployment's audited
consolidation procedure explicitly changes it. Provision its schema before
startup, retain active grant inventory, and verify the intended role DSNs.
Server-side sessions require expiry and revocation checks; cookie handling must
not bypass them. Follow the [owner-auth runbook](runbook-owner-auth.md).

Backend/ingest/worker/migrate/owner DSNs are separate configuration inputs.
Direct processes read `MESHCORE_HTTP_DATABASE_URL`, `MESHCORE_INGEST_DATABASE_URL`,
`MESHCORE_WORKER_DATABASE_URL`, `MESHCORE_MIGRATE_DATABASE_URL` and
`OWNER_DATABASE_URL`. Without `DATABASE_ROLE_ENFORCEMENT`, they can use the
legacy `DATABASE_URL` fallback; the owner service derives its separate database
name when that explicit URL is absent. Enabling enforcement requires the DSN
for every role that process opens. Main Compose constructs container DSNs
explicitly, so separate credentials also need reviewed environment wiring.
Use `scripts/bootstrap-backend-roles.sql` and
`scripts/bootstrap-reader-roles.sql` before the applicable grant migrations.
Review `.env.example`, broker ACL paths, the public-feed opt-in state and any
external forwarding destinations. Never copy production `.env` or owner grants
into a local benchmark fixture. Node-based contributor tooling now follows
Node 26 in `.nvmrc`, matching the container runtime major.

## Rehearsal order

On an owned isolated copy of the identified prior deployment:

1. Verify a complete signed backup and restore it into a separate disposable
   environment. Record checksums, row counts, role grants and owner inventory.
2. Build/load candidate images locally without publishing a registry artifact.
   Apply the complete pending migration chain with the explicit runner.
3. Run the bounded identity/read-model jobs and verify privacy convergence,
   external-reader projections, owner sessions and actual lifecycle state.
4. Start the candidate and qualify HTTP/WebSocket data, owner revocation,
   request scope, browser assets, queues and readiness.
5. Start the exact prior image set against the migrated copy and test the same
   contracts. Record any required search-path/configuration compatibility and
   any statement or session format that prevents image rollback.
   Stop candidate-only workers, including `region-snapshot-worker`, before
   rehearsing the prior service set; do not mix writer versions accidentally.
6. Separately restore the signed pre-upgrade backup into another disposable
   environment and verify its receipt and data. This proves disaster restore;
   it does not prove old-image compatibility with the new schema.

Do not declare the candidate ready until both rehearsal outcomes and all
rollback limits are recorded. Never attempt a destructive down-migration.
The production sequence uses the existing [release/rollback runbook](runbook-release-rollback.md)
and [backup/restore runbook](runbook-backup-restore.md) after release authorization.

## API compatibility and deprecations

Existing default public DTOs remain the compatibility contract. Any field or
section selection is opt-in, scope-isolated and documented in the generated
[OpenAPI contract](openapi.yaml). `/api/runtime-config` remains `no-store` so
privacy-generation changes and feature kill switches remain authoritative.

Retain the existing `410 Gone` legacy coverage/planned-coverage contracts and
inactive rollback helpers through the 2.0 rollback window. Removal is a later
reviewed change, after deployment verification and expiry of that window.
Do not silently reactivate their queues or revive legacy workers.

## Browser and lifecycle rollback limits

Retain prior immutable assets and the service worker's seven-day rollback
window. Rehearse an already-open old tab, waiting/activation and rollback on
an isolated local origin before any live cache invalidation. Prepare mutable
HTML/worker/metadata invalidation separately from hashed immutable assets.

The candidate uses service-worker namespace `v10`, retains `v9`, and stores its
metadata at `/__meshcore_sw_metadata_v10__`. Waiting workers require activation;
an already-open old client can load cached old hashed chunks through the retained
namespace when its network request fails. That fallback does not copy old entries
into the new cache or extend their seven-day retention. Stable worker `.mjs` URLs
continue to bypass the immutable app-asset cache.

Run `npm --prefix frontend run test:sw-cutover` for the isolated native Chromium
protocol check. It uses invented HTML/hashed assets and a temporary browser
profile; production website journeys remain a separate quality gate. Supply
`--previous-runtime /absolute/path/to/archived-sw-runtime.js` to rehearse the
actual recorded prior source, and `--output /absolute/ignored/evidence-directory`
to retain runtime hashes and checks. Without that input, CI uses a previous-
namespace fixture. The check covers an old tab, a waiting update, activation,
offline lazy loading, online/offline rollback and persisted seven-day expiry
after a worker restart. Its simulated expiry affects only the temporary origin.

For an authorized cutover, invalidate mutable HTML, `/sw.js` and `/sw-runtime.js`
as required by the deployed edge policy. The metadata key belongs to browser
CacheStorage; its new namespace creates fresh local metadata. Keep old hashed
assets on the origin for at least seven days and through any longer agreed
rollback window; a cached copy cannot recover an old chunk that a client never
loaded. Hashed immutable assets receive new URLs and need no blanket purge.
No live cache purge or asset deletion is part of local candidate qualification.

Record actual Timescale jobs and compression/retention settings after the
schema moves. A migration declaration does not prove a policy is active.
Disabled policies are not permission to activate destructive lifecycle work;
the existing signed-receipt and table allowlist gates still apply.

## Qualification evidence

The reproducible contributor stack and fixture profiles are described in the
[local runbook](../scripts/local-sample/README.md). Candidate evidence must
include the actual prior deployment inventory, candidate SHA, migration and
rollback results, security/SBOM checks, sustained nonduplicate soak, public
mobile/desktop quality checks and explicit unmet gates. Local/private evidence
belongs under ignored `plans/measurements/`, never in tracked sample fixtures.

The current-schema synthetic disaster-restore rehearsal verified signed receipts,
Timescale restore-mode exit and disabled drill jobs in both databases, complete
packet/path/ledger row fingerprints, Redis stream pending state and a retained
MQTT message. Its owner tables were empty. That evidence does not establish
production role grants, nonempty owner/session recovery, off-host durability,
production recovery objectives or compatibility with the actual previous images.
Those remain part of the deployment-specific rehearsal above.
