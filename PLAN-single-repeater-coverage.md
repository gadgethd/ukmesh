# PLAN: Single-Repeater RF Coverage on UKMesh

Status: **reviewed (REVIEW-single-repeater-coverage.md, commit 4ec5fd1) + benchmarked (2026-08-03) — standard tier only per Ben**
Date: 2026-08-02
Author: Hermes (assessment from live inspection of 192.168.100.105)
Canonical review output: `REVIEW-single-repeater-coverage.md` (same directory)

## 1. Goal

Expose an individual repeater's RF coverage footprint on the UKMesh app, reusing
the HopReach propagation calculations already deployed on 192.168.100.105 —
without degrading the existing whole-network map.

## 2. Current state (verified live 2026-08-02)

- **HopReach** (Go, vendored `third_party/hopreach`, public fork `gadgethd/hopreach`
  @ tag `v0.1.32-ukmesh.3`, commit 0230702be70a2729c5acc5640401f56ab9d65fd4) runs
  as compose service `hopreach` (4 CPU / 8 GiB, CPU mode), nightly cron `17 2 * * *`.
- Backend serves a private `/hopreach` compatibility router (internal-only, not
  proxied by Nginx): paginated nodes (role=2 repeaters with coordinates, ukmesh
  network) + observed `node_links` (calibration evidence).
- **The raster model is best-server**: `internal/propagation/propagation.go`
  `marginsRowIndexed` computes margins for every candidate site per pixel but
  keeps ONLY the max (`bestMargin`) — per-transmitter margins are discarded.
- Output published to volume `rf_coverage_data`:
  `/data/output/meta.json`, `progress.json`, `tiles/standard/{r}-{c}.png`
  (6 tiles, 2000px, DEM zoom 11), `tiles/precision/{r}-{c}.png` (54 tiles,
  6000px, DEM zoom 13, 2x supersample). Precision was mid-compute at inspection.
  Live meta: 7,071 repeaters fetched, 4,380 in region (416 active / 592 degraded /
  3372 silent), max_search_range_km 77.53, freq 868 MHz.
- Frontend: `RfCoverageOverlay.tsx` is fully **data-driven** — it renders any
  tile set described in `meta.json` (`coverage[<tier>].tiles` = {image, bounds})
  via `/rf-coverage/<path>?revision=…`. Adding a new tile set = meta extension +
  a selector; the raster protocol is generic.

## 3. Key finding

**The current output cannot yield per-repeater coverage** (per-transmitter data is
maxed away at raster time; PNG tiles are merged best-server).

**But the fork already contains the exact machinery needed**: `cmd/hopreach/run.go`
has a per-**scope** coverage path (~line 720): it filters repeaters to a subset,
calls `coverage.Raster(engine, grid, scopeSites, scopeBounds, …)` with the same
engine/DEM-cache/model, writes `coverage-scope-<slug>-*.png` tiles, and registers
them in `meta.json` (`ScopeCoverage`). **A single repeater is a subset of one.**

It is dormant in production: `rf-coverage/config.ukmesh.yaml` has
`scope_observation.enabled: false` (live meta.json contains no scope tier).

## 4. Recommended approach (A): per-repeater tiles from the HopReach fork

> **DECISION (Ben, 2026-08-03): per-repeater coverage uses the STANDARD tier ONLY.**
> Precision is rejected for per-node use — measured 4 min 52 s / 36 tiles vs 6 s / 4 tiles
> (see §4.5 benchmark). Standard-only keeps on-demand compute trivial and the UI snappy.
> The global whole-network map keeps both tiers exactly as today.
>
> **DECISION (Ben, 2026-08-03): ON-DEMAND ONLY.** A repeater's coverage is computed
> only when that specific repeater is requested — never precomputed for every repeater,
> never a nightly batch. Repeat requests for the same node within the freshness window
> are served from cache. meta.json grows only by requested nodes (bounded by LRU/TTL).

### 4.1 Calculator (third_party/hopreach, Go)

- Clone the per-scope loop pattern (run.go `computing_scope_coverage` block) into a
  new **per-repeater** path:
  - Input: a single repeater public key (64-hex) → find its node in the fetched
    node set (`selectRepeaters` output) → `sites = [one Site]`.
  - Bounds: `coverage.RasterBounds([]Point{node}, rangeKm)` (same as scope path).
  - Raster: `coverage.Raster(engine, grid, oneSite, bounds, imageWidth, params, maxAlpha, progress)`.
  - Tiles: `coverage.WriteTiles(outputDir, "coverage-node-<short-id>", raster, bounds)`.
  - Meta: register `meta.node_coverage[pubkey] = buildCoverageMeta(tiles, rangeKm, cfg, note)`
    (mirror `ScopeCoverage` pattern).
- **Trigger mode — DECIDED: on-demand only.** Chosen path (per REVIEW
  recommendation): `--node <pubkey>` one-shot CLI primitive as the execution
  engine, invoked by a small loopback-only internal admin/queue endpoint
  (hopreach-shareapi `/admin` pattern) or an operator-side submitter. The UI
  click → internal endpoint → queued job → status poll. Reuses lock.go +
  `min_recompute_interval_hours: 6` skip-if-fresh: a second request for the same
  node inside the window returns the cached tiles, not a recompute.
- **NO precomputation**: no nightly batch, no compute-all-repeaters path, no
  warming. Only nodes actually requested are ever computed (bounded set; see
  meta growth policy §7 Q5).
- **Tier scope: STANDARD only** (2000 px, DEM zoom 11) — the precision tier must
  NOT be computed for node jobs (see decision banner). The node path therefore
  needs far fewer DEM tiles (zoom 11 only), less memory, and no supersampling.
- Terrain: reuse the DEM cache (`/data/dem-cache`, zoom-11 tiles are fully warm
  UK-wide — 67 x-columns; zoom 13 is NOT needed for node jobs).

### 4.2 Serving (no changes expected)

- Tiles land in the same `rf_coverage_data` volume; Nginx already serves
  `/rf-coverage/` read-only (meta/progress/numeric tile paths).
- Verify the nginx app config allows arbitrary subpaths under `/rf-coverage/`
  (it currently serves `tiles/standard|precision/*.png` — check the exact location
  rule during review; numeric `{row}-{col}.png` pattern may need widening).

### 4.3 Frontend (frontend/src)

- `hooks/useRfCoverage.ts`: extend `RfCoverageMeta` with
  `node_coverage?: Record<string, RfCoverageTier>`.
- Node popup (map page): add "Show RF coverage" action for repeaters.
- `components/Map/RfCoverageOverlay.tsx`: select the node tile set instead of the
  global tier; existing layer ordering (below roads/labels/nodes) and tier
  availability states are reusable. Tile URL helper is generic
  (`rfCoverageTileUrl`).
- Optional: status/legend text mirroring the existing legend/model details.

### 4.4 Sizing (VALIDATED by benchmark, 2026-08-03)

- Single repeater, **standard tier only**: measured **~6 s** (4 tiles, zoom 11)
  on 4 CPU / 8 GiB with warm DEM cache, running concurrently with the nightly
  UK precision job. On-demand compute cost is negligible.
- Precision for one repeater (measured, for reference only): 4 min 52 s / 36
  tiles — ~50x standard. Rejected for per-node use (§4 decision).
- Memory: standard tier needs zoom-11 DEM only; no supersampling; well under
  the 8 GiB container limit.

### 4.5 Benchmark evidence (2026-08-03, Berwick 55.7708,-2.0058, active repeater)

- Method: production hopreach binary (deployed digest), mock node feed with
  exactly 1 in-region repeater, prod-identical limits (4 CPU / 8 GiB /
  GOMAXPROCS=4), warm DEM cache, run concurrently with prod nightly run.
- Results: standard 6 s (4 tiles, gen 00:15:38Z); precision 4 min 52 s
  (36 tiles, gen 00:20:30Z); total wall 4 min 58 s. Engine mid-run ETA (~5.3
  min) matched. Prod nightly run unaffected (59.5% → 60.3% during test).
- Artefacts on VM: `~/hopreach-bench-output/` (tiles+meta), `~/bench-cache/`
  (warm tile copy). Cold areas add only S3 download time (~65 KB/tile).
- Implications: a "show coverage" click can serve standard almost instantly;
  precision would be a poor trade for per-node views.

## 5. Alternatives

- **B — resurrect old per-node API**: `node_coverage` table still holds 4,578
  per-node polygons (red/amber/green bands; last written 2026-08-02 15:53 UTC).
  `/api/coverage/:nodeId` is tombstoned 410. Zero compute, but: old model that
  HopReach replaced, frozen/stale data, and `docs/rf-coverage-rollout.md`
  explicitly forbids the live app reading `node_coverage` (rollback material,
  one-release window, removal planned). **Rejected as primary option.**
- **C — derive from merged tiles**: impossible (per-transmitter data destroyed).

## 6. Release constraints (from docs/rf-coverage-rollout.md — MUST hold)

- Any fork change requires a new public tag (e.g. `v0.1.32-ukmesh.4`), immutably
  tagged on `gadgethd/hopreach`, referenced by SOURCE-OFFER.md; AGPL-3.0 +
  Commons Clause publication rules apply.
- `scripts/benchmark-hopreach.sh` release gate + `rf-coverage/BENCHMARKS.md`
  update; digest-pinned images (`.env` pins); do NOT enable calibration or reduce
  fidelity; keep `node_coverage` + old images for the rollback window.
- Backend/app rollback safety: per-repeater additions must not change the global
  tier contract.

## 7. Open questions for review

> **Status 2026-08-03:** code-level questions (1–2) are answered with file:line
> evidence in `REVIEW-single-repeater-coverage.md` (commit 4ec5fd1) — that review
> MUST be folded into the implementation (nil-grid chunked path, WriteTiles/nginx
> path contract, per-node checkpoint identity, freshness, meta size). The compute
> question is resolved by the §4.5 benchmark: standard tier only, ~6 s per node.

1. Exact touch points (file:line) for the per-repeater loop; does
   `progressiveSignature`/input-signature invalidation interact safely with
   per-node runs (site list changes)?
2. Nginx location rule for `/rf-coverage/` — does it need widening for
   `coverage-node-*` tile names?
3. On-demand trigger: CLI flag vs HTTP endpoint vs queue — which fits the
   container's lock/schedule model with least risk?
   → **DECIDED**: `--node` CLI primitive + loopback-only internal queue endpoint
   (per REVIEW's hybrid recommendation); no public/browser compute trigger.
4. Should per-repeater runs be gated on node freshness (silent repeaters have
   stale positions) or computed for any in-region repeater?
5. meta.json growth: thousands of `node_coverage` entries — acceptable? (416
   active now; only compute on demand/by request.)
6. Frontend: where exactly is the node popup (which component) to host the
   "Show RF coverage" action?
