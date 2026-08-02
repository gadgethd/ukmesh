# RF-aware coverage: compatibility, capacity, and rollout

Status: implemented and verified locally; not deployed  
Evidence date: 2026-08-02  
Coverage model: `rf_radial_100m`, model version 7

## Scope and HopReach relationship

The UK Mesh implementation was informed by
[HopReach](https://github.com/A13xB0/hopreach) at commit
`61efac0b4678f55496fe08f53eda0c79eb18655b`. It adopts the same core RF ideas:
free-space path loss, a standard 4/3 effective-earth-radius correction,
single-knife-edge diffraction, shared terrain sampling, bounded CPU work, and
a hard range cap.

It is not currently a HopReach runtime integration or a fork. No HopReach code
or repository was changed, and UK Mesh does not consume HopReach YAML, call a
HopReach service, or read its raster/metadata output.

| Boundary | Current state |
| --- | --- |
| Propagation concepts | Aligned, but not guaranteed numerically identical |
| Runtime | UK Mesh Python worker; HopReach Go whole-region raster engine |
| Work unit | One repeater produces three GeoJSON signal bands |
| Configuration | UK Mesh environment variables and calibration state |
| Persistence | PostgreSQL `node_coverage`, with Redis job coordination |
| Map delivery | On-demand `/api/coverage/:nodeId` GeoJSON |
| HopReach import/export | Not implemented |
| Upstream contribution | Not implemented |

The architectures answer different product needs. UK Mesh needs selectable,
per-repeater geometry and existing queue/database lifecycle semantics;
HopReach produces best-server whole-region rasters. If interoperability is
required, treat it as a separate scoped change. The lowest-risk options are a
validated propagation-config translator, or explicit UK Mesh node export plus
HopReach raster import. Do not describe the current worker as HopReach
compatible until one of those contracts and cross-engine fixtures exists.

## What model version 7 changes

The radial solver now batches multiple rays and endpoint prefixes in bounded
NumPy arrays. Exact scalar-versus-batched path-loss parity is covered by tests.
The running terrain-roughness percentile uses a small sorted list, which is
faster at the deliberate 1,000-sample bound, and DTM approximation writes into
its existing float32 raster rather than allocating another full raster.

Although SRTM acquisition still caches complete one-degree source tiles, each
job now reads only the rectangular pixel window enclosing its RF search radius,
plus five pixels of terrain-filter context. Pixel indices are calculated in the
original VRT coordinate space before the integer window offset is subtracted.
That preserves exact full-raster sampling and avoids floating-point boundary
drift. A London full-raster/window comparison produced identical geometry
hashes while reducing elapsed time from 0.927 to 0.276 seconds and peak RSS
from 457.371 to 194.910 MiB.

Version 7 also fixes an output-affecting terrain extent bug. RF rays search up
to `max(20 km, 1.35 * geometric_horizon)`, capped at 100 km. Tile acquisition
now uses that same expanded radius. Earlier models could search outside the
VRT and clamp samples to its edge. Because that can change coverage geometry,
existing version-6 rows must not be presented as version-7 results.
The coverage API enforces that boundary: public list and single-node reads
return only rows whose `model_version` matches the configured active version
and whose `calculation_status` is `computed`. Stale, retryable, and permanent
rows therefore cannot be labelled `ready`.

`RF_PREFIX_RAY_BATCH=8` is the measured CPU/memory compromise. Raising it can
increase memory without improving latency. Keep `NUM_WORKERS=1` for the first
full generation until production RSS is measured.

## Verification evidence

All verification used isolated or read-only resources. No running service was
rebuilt, restarted, or switched to model 7.

- 28 worker tests passed in local image
  `sha256:64a25c84d44127b1610db39858651ba1a7fbba62a07bb4b3b56fb2949848f70f`.
- Prefix benchmark, 24 rays by 1,000 steps: 126.775 ms p50 total, 5.449 ms
  p95 per ray, 14.84x median speedup over the scalar reference, 0.000 dB
  maximum parity error, and 103.016 MiB peak RSS.
- Complete synthetic radial benchmark, 360 rays by 1,000 steps: 1.848 seconds
  p50, 1.860 seconds p95, and 161.777 MiB peak RSS across three repetitions.
- A cold-cache London sample in the final image downloaded two SRTM tiles and
  returned valid green, amber, and red polygons in 2.342 seconds with
  195.695 MiB peak RSS.
- After the in-place and radius-windowed raster changes, the same hot-cache
  sample completed in 0.276 seconds with 194.910 MiB peak RSS. Its three-band
  geometry hash exactly matched the pre-window full-raster result.
- A read-only, network-disabled pilot sampled 12 eligible repeaters across UK
  latitudes. Eleven cached-terrain jobs completed; one correctly returned a
  retryable missing-tile outcome because downloads were disabled. Completed
  jobs had a 0.464 second median, 1.871 second p95, 1.933 second maximum, all
  three signal bands, and 616.160 MiB process peak RSS for the sequential run.
- Frontend tests, typecheck, and production build passed. Backend tests,
  typecheck, and build passed. The map test verifies distinct red, amber, and
  green features and the legacy single-polygon fallback.

The 2026-08-02 read-only inventory contained 4,552 eligible repeaters, zero
computed model-7 rows, 4,265 computed model-6 rows, and 4,224 model-6 rows with
all three bands. On the representative pilot, a conservative single-worker
estimate for rebuilding all model-7 coverage is roughly 1-3 hours plus any
uncached terrain download time. This is a capacity estimate, not a completed
full generation, and should be replaced by actual queue metrics before public
exposure.

A conservative terrain inventory treated every eligible repeater as if it
needed the full 100 km cap. The union was 175 SRTM1 tiles: 135 already present
in the shared cache and 40 absent, for at most about 0.966 GiB of additional
uncompressed terrain and 4.227 GiB total. The host reported about 13.57 GB free
on that filesystem at the evidence date. This is an upper bound; ordinary
height-derived radii request fewer tiles.

## No-deploy gate

The checked-in defaults target model 7, but the current operator `.env` remains
on `COVERAGE_MODEL_VERSION=6`. That file was intentionally not changed. The
running viewshed worker is also an older image with RF generation disabled.
Those facts prevent this work from silently becoming live.

Before any later rollout:

1. Build a release image from the reviewed revision and rerun the worker test
   suite plus both RF benchmarks in that exact image.
2. Run a small network-enabled canary with a separate queue/database target or
   otherwise fenced writes. Confirm missing SRTM tiles download successfully,
   inspect all three polygons, and retain stage timings and RSS.
3. Confirm disk headroom for the SRTM cache and at least 1 GiB memory headroom
   for one worker. Do not increase worker concurrency until measured peak RSS
   on the target host proves it is safe.
4. Set model version 7 and enable only the generation worker. Keep the public
   coverage UI disabled while the backfill runs.
5. Monitor ready, in-flight, retry, dead-letter, deadline, terrain-download,
   job-duration, and RSS metrics. Stop on sustained retries, a growing
   dead-letter queue, memory pressure, or geometry validation failures.
6. Count only eligible rows where `model_version = 7` and
   `calculation_status = 'computed'`. Require every non-permanent eligible
   repeater to have valid green, amber, and red geometry; investigate every
   permanent result.
7. Back up the coverage table, then enable the backend and frontend coverage
   feature flags and rebuild the app. Verify representative desktop and mobile
   map interactions before directing public traffic to it.

Until these gates pass, do not enable model-7 generation against the live
queue and do not expose the RF overlay publicly.
