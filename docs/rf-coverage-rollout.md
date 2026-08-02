# HopReach RF coverage rollout and recovery

Status: implemented and verified locally; production rollout is not performed
by this change.

Evidence date: 2026-08-02

Canonical model: the public UK Mesh HopReach tag
[`v0.1.32-ukmesh.1`](https://github.com/gadgethd/hopreach/tree/v0.1.32-ukmesh.1)
at commit `9ab79011dcd6537e3d90bbdbf5a52bc273b9b2da`, based directly on upstream
v0.1.32 commit `61efac0b4678f55496fe08f53eda0c79eb18655b`. The tagged tree is vendored
at `third_party/hopreach`.

## Release invariants

- Publish the exact integration release revision before exposing any
  HopReach-derived image. `rf-coverage/SOURCE-OFFER.md`, the public fork/tag,
  the vendored license, and both recorded revisions must be reachable without
  operator access.
- Use digest-pinned backend, HopReach, and app images built by the signed
  release workflow. Confirm the backend/app revision label is the integration
  release commit and the HopReach revision label is
  `9ab79011dcd6537e3d90bbdbf5a52bc273b9b2da`.
- Do not enable calibrated variants. The production profile deliberately has
  `calibration.enabled: false`; evidence validation is a separate rollout.
- Do not reduce range, node count, terrain zoom, supersampling, or RF fidelity
  to meet a runtime target. `scripts/benchmark-hopreach.sh` is the release
  gate and `rf-coverage/BENCHMARKS.md` records the accepted local measurement.
- Keep `node_coverage`, old RF images, and the prior signed release for one
  release as rollback material. The live app and calculator must never read
  them.

## Runtime shape

The backend mounts a private `/hopreach` compatibility router. It supplies
only UK-visible positioned repeaters and genuinely observed `node_links`; it
does not read predicted `node_coverage`. Nodes are paginated at 500, bulk
calibration is bounded at 5,000 public keys and 250,000 returned rows, and a
per-node fallback remains available. Short bounded caches and in-flight
coalescing protect cold-start pagination and concurrent evidence loads. The
router rejects public or forwarded traffic and is not exposed by Nginx.

HopReach uses the immutable `uk-operational-v1.geojson` boundary, containing
Great Britain, Northern Ireland, the Isle of Man, Jersey, and Guernsey. It
publishes into the dedicated `rf_coverage_data` volume:

```text
/data/output/meta.json
/data/output/progress.json
/data/output/tiles/standard/{row}-{column}.png
/data/output/tiles/precision/{row}-{column}.png
```

Standard is computed first at 2,000 pixels and terrain zoom 11. Precision may
start only after Standard is live, the free-disk gate passes, and then uses
6,000 pixels, terrain zoom 13, and 2x supersampling. Each 1,024-pixel
publication tile computes against every transmitter within the unchanged
link-budget maximum range. Tiles and checkpoints are atomically replaced.

The calculator owns a singleton lock, nightly schedule, persistent DEM cache,
input signature, and resumable checkpoint. During recomputation the app serves
the previous last-known-good set plus newly completed current tiles. A failed
or restarted run therefore cannot blank the layer.

## Preflight

From a clean reviewed revision:

```bash
docker compose config --quiet
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/work" -w /work/third_party/hopreach \
  golang:1.23-bookworm go test ./...
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD:/work" -w /work golang:1.23-bookworm \
  /work/scripts/benchmark-hopreach.sh
```

Also require backend tests/typecheck/build, frontend unit tests/build, and the
desktop/mobile Playwright RF journey. The compatibility load test is the test
named `handles UK pagination and coalesces concurrent calibration loads`; it
uses 4,600 rows, ten pages, 32 concurrent requests, a cold pass, and verified
cache hits.

Before production, inspect capacity without deleting anything:

```bash
docker volume inspect meshcore-analytics_rf_coverage_data
docker system df
docker compose config --images
```

The default calculator limit is four CPUs, `GOMAXPROCS=4`, and 8 GiB memory.
Precision additionally requires the configured 8 GiB free inside `/data`.

## Deployment order

1. Verify the release revision and corresponding source are public, then
   verify signatures, attestations, image digests, and revision labels using
   `docs/runbook-release-rollback.md`.
2. Deploy migrations if the release has any, then deploy `backend`. From the
   calculator network namespace, confirm `GET /hopreach/healthz`, node
   pagination, and one bounded bulk-link request. Confirm the same paths are
   unavailable through the public app origin.
3. Deploy `hopreach`. Watch its logs and `/data/output/progress.json`; do not
   wait for the whole UK before proceeding. Confirm a valid `meta.json` and at
   least one atomically readable Standard PNG exist.
4. Deploy `app-ukmesh` built with `VITE_RF_COVERAGE_ENABLED=true`. The Nginx
   contract exposes only metadata, progress, and numeric Standard/Precision
   PNG paths from the read-only shared volume.
5. Allow Precision to start only after metadata reports Standard live and the
   resource gate passes. A denied Precision tier is visible as a tier failure;
   it must not remove Standard.

Do not start a `viewshed-worker`; that service no longer exists in Compose.
The separate `link-worker` remains required because it writes observed
`node_links` used as calibration evidence.

## Live verification

On desktop and mobile, verify:

- the first Standard tile appears progressively and new sessions enable RF
  coverage automatically;
- a manual visibility choice persists, Standard/Precision selection appears
  only when available, and the orange-to-green legend/model details are shown;
- stage, backend, percent, ETA, and any last-known-good failure state update;
- coverage remains nearest-neighbour and below roads, place labels, nodes, and
  interactive layers while pan/zoom remains responsive;
- browser network logs contain only `/rf-coverage/meta.json`,
  `/rf-coverage/progress.json`, and valid tile paths, with no request to
  `/api/coverage` or `/api/coverage/planned`;
- `docker compose ps` contains `hopreach` and `link-worker`, but no viewshed
  coverage worker.

After one Standard run, restart only `hopreach`. The same incomplete run ID
must resume, completed tiles must remain readable, and the next checkpoint
must advance without recomputing completed tiles. Repeat once during Precision.

## Failure and rollback

If HopReach fails, leave the app and shared volume in place: metadata exposes
the failure and the UI keeps last-known-good tiles. Preserve logs,
`meta.json`, `progress.json`, and the checkpoint before restarting the single
calculator. Do not delete the DEM cache or output volume as a recovery step.

If the native UI is defective, roll back only `app-ukmesh` to its previous
verified digest; HopReach can continue safely in the background. If the
calculator or compatibility contract is defective, stop `hopreach`, roll back
backend and app together, and retain its volume for diagnosis. Returning to
the old viewshed product requires rolling back the complete prior signed
release; never mix its worker/API/UI with the new release.

At the end of the one-release rollback window, removal of `node_coverage`, old
images, legacy queue code, or caches needs a separate reviewed change with a
fresh backup and exact target inventory.
