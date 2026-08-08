# Review: single-repeater RF coverage plan

Review date: 2026-08-02. Repository and running containers were inspected on the UKMesh VM. The review is read-only; no service, container, configuration, or output was changed.

## Verdict

**Revise before implementation.** The plan is directionally sound: a single-repeater raster can reuse HopReach's terrain-aware propagation path, and the existing frontend raster protocol can display an additional tile dataset. The per-scope block is a good mathematical template, but the proposed change is not a mechanical drop-in in the deployed configuration.

Four plan claims need correction before coding:

- In production, progressive mode leaves the whole-region DEM grid nil. The scope path gets a whole grid only because scope observation enables it; a copied `coverage.Raster(..., grid, ...)` call would therefore be unsafe in the current configuration. A node path should use the chunked DEM path or explicitly load a bounded grid.
- `coverage.WriteTiles` writes directly below its `outputDir`. `WriteTiles(cfg.outputDir, "coverage-node-...", ...)` would create `/data/output/coverage-node-...png`, not an Nginx-served `tiles/...` URL.
- Both the checked-in and running Nginx configuration allow only `tiles/standard|precision/<row>-<col>.png`; arbitrary `coverage-node-*` paths are not served.
- The standard-area and timing estimates are not supported by the geometry or live run. The current precision run is still only 27/54 tiles after several hours, so a benchmark is required before promising “tens of minutes.”

The recommendation is to add a bounded, host-only/on-demand node job backed by a `--node` CLI primitive, with a unique node checkpoint/tier identity, explicit freshness/signature rules, and a separate node manifest or tightly bounded metadata index. Do not make the browser a compute trigger and do not add a nightly batch of all repeaters in the first release.

## Validation findings (file:line)

### 1. Per-scope coverage block and the single-site question

The claimed template is present in `third_party/hopreach/cmd/hopreach/run.go:714-775`:

- `run.go:727-731` gates on `scopeObservationEnabled`, then iterates known scopes.
- `run.go:733-743` filters `selected` into `scopeSites` and `scopePoints` using `repeaterInScope`, and skips an empty subset.
- `run.go:744-746` applies the existing freshness check for that scope.
- `run.go:748` calls `coverage.RasterBounds(scopePoints, rangeKm)`.
- `run.go:752` calls `coverage.Raster(engine, grid, scopeSites, scopeBounds, cfg.coverageImageWidth, cfg.propagation, cfg.coverageMaxAlpha, nil)`.
- `run.go:753` calls `coverage.WriteTiles(cfg.outputDir, "coverage-scope-"+scopeSlug(scopeName), scopeRaster, scopeBounds)`.
- `run.go:759-763` builds coverage metadata and assigns it to `m.ScopeCoverage[scopeName]`; `run.go:764-768` writes `meta.json`.

The raster math is therefore mechanically reusable for one selected site: make `scopeSites` and `scopePoints` contain one validated repeater, then use the same bounds, raster, tile, and metadata operations. The production implementation is not mechanically identical, however:

- The scope block is conditional on scope observation, while the requested node mode must work with `scope_observation.enabled: false` (`rf-coverage/config.ukmesh.yaml:23-28`).
- The whole-grid setup is conditional at `run.go:421-437`. With progressive coverage enabled and scope observation/calibration disabled, `needWholeStandardGrid` is false and `grid` remains nil. The CPU/GPU `Raster` path eventually dereferences the grid (`third_party/hopreach/internal/compute/compute.go:156-162,204-205`). The node path must use `RasterProgressiveChunked`/`MarginsChunked` or explicitly load a suitable grid.
- The node path needs its own freshness, checkpoint, metadata, and publication identity; it must not reuse the global `standard` or `precision` checkpoint names.

### 2. Coverage and propagation signatures

The relevant interfaces are:

- `third_party/hopreach/internal/coverage/coverage.go:31-52`: `RasterBounds(points []Point, rangeKm float64) (propagation.Bounds, bool)` computes a padded geographic rectangle from the selected point set.
- `coverage.go:90-102`: `Raster(engine *compute.Engine, grid *demgrid.Grid, sites []propagation.Site, bounds propagation.Bounds, imageWidth int, p propagation.Params, maxAlpha uint8, progress func(done,total int)) *image.NRGBA`.
- `coverage.go:145-171`: `RasterSupersampledChunked` uses the chunked engine path, which is the relevant bounded-memory option for progressive node work.
- `coverage.go:190-197`: `WriteTiles(outputDir, baseName string, img *image.NRGBA, bounds propagation.Bounds) ([]Tile, error)`.
- `coverage.go:197-231`: tiles are named `<baseName>-<row>-<col>.png` and written directly under `outputDir`; this helper does not create `tiles/<tier>`.

The best-server behavior is confirmed in `third_party/hopreach/internal/propagation/propagation.go:161-185` and `187-218`. For each output pixel, the code starts with negative infinity, evaluates candidate sites, and retains only the maximum margin. The row-indexed implementation at `propagation.go:196-215` likewise retains one best margin per pixel. Per-transmitter margins are not preserved, so merged whole-network tiles cannot be inverted into a repeater footprint. A single-site raster is valid, but it means “coverage from this transmitter alone,” not the transmitter's contribution to the merged best-server map.

### 3. Configuration, skip logic, lock, and progressive signatures

The deployed configuration is `rf-coverage/config.ukmesh.yaml:23-28,30-33,37-63,75-81`:

- scope observation is disabled;
- the DEM cache is `/data/dem-cache` and the configured standard DEM zoom is 11;
- progressive output is enabled, with standard width 2000, precision width 6000, precision DEM zoom 13, supersample 2, and publication tile size 1024;
- `min_recompute_interval_hours` is 6 and the schedule is `17 2 * * *`.

The global skip happens before fetching repeaters at `third_party/hopreach/cmd/hopreach/run.go:358-365`: if a complete `meta.json` is younger than the configured interval, the run exits unless `forceRecompute` is set. The CLI exposes only `-force`, `-force-all-tiers`, and `-prepare` (`third_party/hopreach/cmd/hopreach/main.go:34-38`); `-force` bypasses the global interval but does not make a node request exist. Tier freshness is separately checked at `run.go:634-644` and `third_party/hopreach/cmd/hopreach/output.go:170-185`; it is a UTC-day check, not an input/signature check.

The singleton lock is acquired before the run at `main.go:52-60` and implemented as a nonblocking exclusive flock at `third_party/hopreach/cmd/hopreach/lock.go:9-33`. A node invocation must either queue behind a running job or return a visible busy result. Starting a second process and relying on a retry loop would create ambiguous user state and unnecessary pressure on the container.

Progressive identity is more restrictive than the plan implies:

- `third_party/hopreach/internal/coverage/progressive.go:24-24` permits tier names matching `^[a-z][a-z0-9_-]{0,31}$`, so a full 64-character pubkey cannot be used as a progressive tier name.
- `progressive.go:88-104` hashes model, bounds, zoom, image dimensions, supersampling, site list, and propagation parameters in `progressiveSignature`.
- `progressive.go:126-145` resets a checkpoint when the run ID, tier, signature, or completion state changes.
- `progressive.go:223-260` stores checkpoints as `checkpoints/<Tier>.json` and tiles below `tiles/<Tier>`.

The signature correctly notices a changed site list, but it is safe for node jobs only if each node has a unique bounded tier/checkpoint namespace and a stable node-specific run ID. A generic `standard`/`precision` node job would collide with global work. A safe short hash or collision-checked slug may be used for paths; retain the normalized full pubkey as the metadata key. The global run ID and global `run.tiers` must not be overwritten by an independent node job.

### 4. Nginx serving contract, including the running image

The checked-in contract in `nginx.app.conf:75-95` is:

- exact aliases for `/rf-coverage/meta.json` and `/rf-coverage/progress.json` (`nginx.app.conf:78-85`);
- one regex location at `nginx.app.conf:88-92` matching only `/rf-coverage/tiles/(standard|precision)/([0-9]+-[0-9]+\.png)` and aliasing to `/rf-coverage-data/output/tiles/$1/$2`;
- a 404 fallback for other `/rf-coverage/` paths at `nginx.app.conf:93-95`.

I also inspected the active app container with `docker exec meshcore-analytics-app-ukmesh-1 nginx -T`. Its effective configuration has the same standard/precision numeric regex and 404 fallback; there is no image-side exception for node tiles. This was a read-only inspection.

Consequently, the plan's “no changes expected” serving statement is wrong. `coverage-node-*` files are not served, and the current `WriteTiles` call would place them in the wrong directory even before Nginx matching is considered. The implementation needs a bounded path contract, for example `tiles/nodes/<safe-id>/<tier>/<row>-<col>.png`, plus a narrowly scoped Nginx regex and alias. The tile writer/metadata must emit exactly that path. Do not replace the allowlist with a broad wildcard: the current rule intentionally limits path shape and prevents arbitrary file exposure.

### 5. Frontend data flow and popup location

The frontend is data-driven for the existing global tiers:

- `frontend/src/hooks/useRfCoverage.ts:37-57` models `meta.coverage` with only `standard` and `precision`; `useRfCoverage.ts:78-100` validates tile paths and derives available tiers.
- `useRfCoverage.ts:114-161` polls `/rf-coverage/meta.json` and progress every three seconds and uses `no-store` fetches.
- `frontend/src/components/Map/RfCoverageOverlay.tsx:21-43` builds URLs from metadata tile paths and selects `meta.coverage[tier]`; `:45-89` registers one generic raster dataset and displays it.
- `frontend/src/components/Map/rfCoverageRasterProtocol.ts:189-223` accepts arbitrary validated tile lists/bounds, but its public tier typing/max-zoom logic is currently only standard/precision.

The natural popup action belongs in `frontend/src/components/Map/NodePopupContent.tsx:135-159`, alongside the existing repeater “Show LOS” action. The dock and popup props are assembled by `frontend/src/components/Map/MapLibreMap.tsx:1069-1100,1194-1279`, so that component must pass the node key and a coverage callback/state. Global overlay state currently lives in `frontend/src/App.tsx:133-136,192-203,623-628`; it will need a selected node dataset/state and a clear way to return to the global tier. `NodePopupContent` should show the action only for a valid repeater with coordinates, and should expose pending/available/stale/error states rather than implying that a click synchronously computes a raster.

### 6. Backend lookup feasibility

The actual file is `backend/src/api/hopreachCompatibility.ts` (not a top-level `api/hopreachCompatibility.ts`). It already has the required patterns:

- internal-only protection is at `hopreachCompatibility.ts:81-94`;
- 64-hex pubkey validation is at `:149-157`;
- the reusable repeater/coordinate predicate is `:159-173`;
- the existing `GET /api/nodes/:pubkey/reach` route is `:305-326` and validates a single key before querying links.

The router is mounted under `/hopreach` in `backend/src/index.ts:224-231`, and the reach route returns links, not a node record. A direct single-node lookup using the same predicate and a `public_key` equality condition would be straightforward if the calculator needs server-side freshness/coordinates. The browser already has the node feature's public key and coordinates (`frontend/src/hooks/useNodes.ts:21-41`, `frontend/src/components/Map/types.ts:17-35`), so a lookup is not necessary merely to draw a button. The internal `/hopreach` router should not be treated as a public browser compute endpoint.

### 7. Live state

The read-only command `docker exec meshcore-analytics-hopreach-1 cat /data/output/meta.json` showed only `coverage.standard` and `coverage.precision`; there is no `scope_coverage` or node-coverage collection. The live values were:

- standard: 6/6 tiles, state `available`;
- precision: 27/54 tiles, state `computing`;
- run total: 33/60 tiles;
- top-level `complete`: `false`;
- fetched repeaters: 7071; in-region: 4380; active: 416; degraded: 592; silent: 3372;
- max search range: 77.53187284381539 km; frequency: 868 MHz.

The precision timestamp in the metadata is not completion; the run state explicitly says `computing`. The live metadata confirms that node metadata and node tiles do not currently exist, and that precision cost cannot be assumed to be negligible.

## Touch points (exact files + what changes)

The following are the expected implementation touch points; this review does not make those changes.

- `third_party/hopreach/cmd/hopreach/main.go:34-66`: add a validated `--node <64-hex-pubkey>` dispatch path. Keep it distinct from the global run and return a machine-readable busy/not-found/stale result.
- `third_party/hopreach/cmd/hopreach/run.go:358-463,528-610,634-775`: add node selection, coordinate/status validation, bounded site setup, node-specific publication, and node job state. Do not call the global skip path or mutate global tier state for a node request. Use a chunked raster path when the configured run does not load a whole DEM grid.
- `third_party/hopreach/cmd/hopreach/output.go:152-185,252-285,343-419`: define node coverage metadata, normalized-key/short-path identity, per-node freshness, retention, and atomic publication. Avoid making every node job rewrite an unbounded historical map in the main global manifest.
- `third_party/hopreach/internal/coverage/coverage.go:190-231`: either add a safe node-aware tile destination helper or use the progressive tile writer with a node-specific namespace. The existing `WriteTiles` output must not be assumed to satisfy the Nginx path contract.
- `third_party/hopreach/internal/coverage/progressive.go:24-35,88-104,126-145,223-305`: namespace node checkpoints/tiers with a <=32-character safe identifier and include the normalized key, node snapshot, and all raster inputs in the signature. Ensure a node job cannot resume or overwrite global `standard`/`precision` state.
- `third_party/hopreach/cmd/hopreach-shareapi/admin.go:37-91` and `.../main.go:234-238`: if an HTTP trigger is retained, extend the existing loopback-only admin control plane into a queue/coalescer for node jobs. Do not expose it through the public API or frontend without authentication, authorization, rate limits, and durable job status.
- `nginx.app.conf:75-95`: add a narrow node tile location/alias matching the chosen canonical path. Update path validation and metadata URLs together; retain the exact meta/progress aliases and 404 fallback.
- `frontend/src/hooks/useRfCoverage.ts:3-161`: model node manifests/status, node-specific revision values, and stale/pending/error states. Do not make the three-second global poll download thousands of repeated tile arrays indefinitely.
- `frontend/src/components/Map/RfCoverageOverlay.tsx:21-122` and `frontend/src/components/Map/rfCoverageRasterProtocol.ts:189-223`: allow one selected node dataset to replace the global source, keep the fixed source/layer IDs coherent, validate node paths, and support node tier/zoom metadata.
- `frontend/src/components/Map/NodePopupContent.tsx:135-159`, `frontend/src/components/Map/MapLibreMap.tsx:1069-1100,1194-1279`, and `frontend/src/App.tsx:133-136,192-203,623-628`: add the repeater action, pass node identity and callbacks, manage selected-node overlay state, and restore the global coverage view.
- `backend/src/api/hopreachCompatibility.ts:149-173,305-326`: optionally add a single-node details route using the existing key validation and repeater/coordinate predicate. Keep the route internal and do not use it as an unauthenticated compute trigger.
- `third_party/hopreach/Dockerfile:7,29,45-51`, `docker-compose.yml:553-580`, `rf-coverage/SOURCE-OFFER.md:3-17`, `rf-coverage/BENCHMARKS.md:1-15`, and `docs/rf-coverage-rollout.md:7-30,96-117`: update the public fork/tag, source/revision/license labels, benchmark evidence, image digest, and deployment/rollback documentation if the fork is changed. The release process requires immutable digest-pinned artifacts, not a mutable local tag.

## Answers to the 6 open questions

### 1. Exact touch points and signature safety

The exact current template is `run.go:714-775`; the common interfaces are `coverage.go:31-52,90-102,190-231`. The signature machinery is conceptually suitable because it includes the site list and raster inputs, but it is not safe to reuse globally. Give each node job a unique safe namespace, include the normalized pubkey and a node-position/status snapshot in the input identity, and use a node-specific run ID/checkpoint. A changed position or site snapshot must invalidate the node output. Never use the global `standard`/`precision` checkpoint files for a node job.

Also correct the grid assumption: in the deployed progressive configuration, the node path should use chunked DEM loading rather than pass the nil whole-region grid into `Raster`.

### 2. Nginx location rule

Yes, Nginx needs a change. The exact rule at `nginx.app.conf:88-92` accepts only `tiles/standard` or `tiles/precision` and a direct numeric filename. It will reject both `coverage-node-*` in the output root and a node subdirectory.

Use a canonical, bounded path such as `tiles/nodes/<safe-id>/<tier>/<row>-<col>.png`, add an exact regex/alias for it, and make the tile writer and metadata emit that path. A full pubkey should remain a metadata key, not an unvalidated URL path. A short ID must be deterministic and collision-checked; do not use an arbitrary user-provided slug or a broad Nginx wildcard.

### 3. On-demand trigger: CLI flag, HTTP endpoint, or queue

Use a hybrid with clear ownership: `--node` is the execution primitive, and a loopback-only internal admin endpoint or operator command submits a job to a bounded queue. The existing `hopreach-shareapi` endpoint (`admin.go:37-91`) is already a local control-plane pattern, but its current `/admin/recompute` starts a full `-force` run, so it should not be copied as an unbounded node launcher.

The queue must coalesce duplicate requests, expose queued/running/available/failed/busy states, and respect the singleton lock. If the global nightly run owns the lock, a node request should queue or return an explicit retryable busy response. Do not trigger computation directly from an unauthenticated browser request. Do not batch all 416 active repeaters nightly in the first version; the live precision job makes the resource risk too high.

### 4. Freshness and silent repeaters

Gate the default public/current-coverage request to repeaters with valid coordinates that are active or degraded under an explicit age policy. Exclude silent repeaters by default: the live snapshot has 3372 silent repeaters, and their positions may be stale. Degraded nodes may be included with a visible stale-data warning and a configurable maximum position age. An operator-only or explicitly labeled historical mode can permit silent nodes.

Record the status/position timestamp used by the computation in the node metadata and include it in the signature. Do not silently return a fresh-looking tile for a stale position.

### 5. `meta.json` growth

Thousands of full node entries are not a good long-term shape for the current `meta.json`: the frontend downloads the whole document every three seconds (`useRfCoverage.ts:114-161`), and progressive writes already rewrite metadata during a run. A 416-entry on-demand map may be acceptable as a bounded first experiment if it contains compact index data and has a TTL/LRU cap, but it should not grow without bound or retain every historical node forever.

Preferred design: keep global `meta.json` stable and publish a compact node index or per-node manifest under an explicitly served node path. If the first release keeps `node_coverage` in `meta.json`, impose a maximum count/age, publish only available tiers and compact tile references, and test payload size and update frequency. Old node tiles also need a retention/garbage-collection policy so metadata never points at missing files and disk usage cannot grow indefinitely.

### 6. Frontend popup location

Add the action in `frontend/src/components/Map/NodePopupContent.tsx:135-159`, next to “Show LOS” for repeater nodes. Wire it through `MapLibreMap.tsx:1069-1100,1194-1279` and manage selected-node coverage in `App.tsx:133-136,192-203,623-628`. Extend the hook and overlay rather than creating a second raster protocol: `useRfCoverage.ts:37-100` needs node metadata, and `RfCoverageOverlay.tsx:37-89` already registers a generic tile list/bounds dataset.

The button should request or select a node job and show pending/available/stale/error states. It should not assume that a click immediately creates a tile, and it should provide a clear “back to network coverage” action because the overlay currently uses one fixed MapLibre source/layer (`RfCoverageOverlay.tsx:45-89`).

## Risks

- **Wrong terrain execution path.** Passing the nil production grid to `coverage.Raster` can fail. Loading a whole precision grid can exceed the container's memory budget; `third_party/hopreach/internal/compute/chunked.go:15-27` documents roughly 1.1 GB per padded zoom-13 tile at UK latitudes and several-GB whole-region behavior. Use chunked computation and benchmark it.
- **Overlapping global and node jobs.** The flock at `lock.go:9-33` is process-wide. A node job must be queued or explicitly rejected while a global job runs, and must not alter global checkpoints, global `meta.run`, or global completion state.
- **Partial publication.** A progressive node job can expose partial tiles. Publish a node manifest only after its required tier is complete, or make partial state explicit and ensure every listed file exists. Atomic metadata replacement and old-version retention are required for rollback.
- **Nginx/path exposure.** Broadening the location to arbitrary subpaths risks exposing files in the shared output volume. Keep node IDs canonical, bounded, and matched by an exact regex.
- **Identifier collisions and length.** A full pubkey is safe as a metadata key but is too long for a progressive tier name (`progressive.go:24` allows at most 32 characters). `coverage-node-<short-id>` also needs a defined collision policy; “short” is not enough. A hash-derived ID with a reverse lookup in metadata is safer.
- **Stale node positions.** The backend's repeater predicate (`hopreachCompatibility.ts:159-173`) checks coordinates and role but does not exclude silent nodes. The UI and metadata must carry freshness rather than treating any coordinate as current.
- **Best-server semantics.** The output is a single-transmitter footprint, not per-transmitter attribution from the existing merged PNG. It should be labeled accordingly so users do not compare it as a contribution layer.
- **Compute estimate.** `RasterBounds` pads by `rangeKm/110.574` in latitude and by longitude at the selected latitude (`coverage.go:31-52`). At 77.53 km the full padded span is about 155 km north-south; “155x110 km” is not the resulting symmetric approximate footprint. The live standard run took about 22 minutes to become available for 4380 repeaters, while precision was still 27/54 after hours. A one-site run may be much cheaper, but “few minutes” and “tens of minutes” need measured 4-CPU/8-GiB benchmarks, including cold and warm DEM-cache cases.
- **Metadata and disk growth.** Per-node tile arrays, repeated three-second polling, partial precision outputs, and retained old files can become a storage and bandwidth problem. Add TTL/LRU, quotas, cleanup, and payload-size tests before batch use.
- **Trigger abuse.** A public “Show coverage” action can become a compute DoS if every click starts work. Require internal authorization, deduplication, per-node cooldowns, and a queue limit.
- **Release and licensing.** A fork change requires the new public tag/source offer and the AGPL-3.0 plus Commons Clause obligations documented in `rf-coverage/SOURCE-OFFER.md:3-17` and `third_party/hopreach/README.md:177-185`. The release must update source/revision OCI labels and publish the corresponding source before exposing the service.
- **Digest-pinned deployment.** The documented rollout requires immutable signed image digests and clean source/revision labels (`docs/rf-coverage-rollout.md:96-117`, `docs/runbook-release-rollback.md:1-19`). A local `:local` compose build or mutable tag is not sufficient for the production change. Preserve the existing global tile contract and keep old artifacts for rollback.

## Recommended trigger mode

Implement `--node <pubkey>` as a one-shot calculator primitive, invoked by a small loopback-only queue/control endpoint in `hopreach-shareapi` or by an operator-side job submitter. Keep the nightly schedule for the global standard/precision run. The queue should enforce one HopReach process at a time, coalesce duplicate node requests, exclude silent nodes by default, use unique node checkpoint/manifest identities, and return a status that the popup can poll.

This gives the frontend a safe workflow—request, wait, display—without exposing a compute endpoint or coupling node work to the global `meta.run`. It also leaves room to add a carefully capped active-node batch later, after benchmark, memory, retention, and release-artifact evidence exists.
