# Single-repeater RF coverage implementation report

Date: 2026-08-03

Branch: `feat/single-repeater-coverage`

Base HEAD: `8d218c1`

## What changed

### HopReach one-shot node mode

- Added `--node <64-hex-public-key>` to `cmd/hopreach`, with normalized key validation and a dispatch path separate from the global run.
- Added machine-readable one-shot results for `available`, `busy`, `not_found`, `invalid_coordinates`, `out_of_region`, `stale`, and `failed` outcomes.
- The node run fetches CoreScope repeaters, requires valid in-region coordinates, rejects silent/stale positions, and accepts active/degraded positions with their position status recorded.
- Computes exactly one `propagation.Site` at the configured Standard settings: image width 2000, DEM zoom 11, supersample 1. It uses `RasterProgressiveChunked`; it never passes the production nil whole-region DEM grid to `Raster` and never computes Precision.
- Added the isolated progressive namespace `tiles/nodes/<dataset-id>/<row>-<col>.png` and `checkpoints/nodes/<dataset-id>.json`. The bounded 25-character dataset ID is a deterministic SHA-256-derived value over the normalized full public key, node snapshot, and raster inputs. The full key is also folded into the checkpoint signature.
- Node checkpoint/run identities are separate from global `standard` and `precision`. Node metadata updates replace only the `node_coverage` member of `meta.json`; global `meta.run`, global completion, global progress, and global checkpoints are not modified.
- Added 6-hour skip-if-fresh behavior using `coverage.min_recompute_interval_hours`. Cached metadata is accepted only when the node coordinates/status still match and every advertised tile exists.
- Added a compact node metadata index with computing/available/failed state, position freshness, per-node run/dataset IDs, progress counts, Standard metadata, and failure text. The index is limited to 128 entries and seven days; unreferenced node tile/checkpoint datasets are cleaned after successful publication.
- Global runs preserve the existing node index while updating normal network metadata.

### Tile serving

- Added a narrow Nginx regex/alias for exactly `/rf-coverage/tiles/nodes/n<24-lowercase-hex>/<row>-<col>.png`.
- Kept the exact `meta.json` and `progress.json` aliases, existing Standard/Precision allowlist, and `/rf-coverage/` 404 fallback unchanged.

### Frontend

- Extended RF metadata types and tile validation for the bounded node dataset contract.
- Added derived `pending`, `available`, `stale`, and `error` states. Degraded node positions are displayed as stale even when a raster is present.
- Added **Show RF coverage** beside **Show LOS** for non-redacted repeaters with a full public key, including the current dataset state.
- Wired selected-node RF state through `MapLibreMap` and `App`; selecting a repeater swaps the existing generic raster protocol/source to that node's Standard tile set.
- Added clear **Back to network coverage** / **Network coverage** actions and node-specific status/legend text.
- Added tests for node state derivation and the node tile path contract.

### Backend

- Not changed. The optional single-node lookup was unnecessary because HopReach fetches and validates the authoritative CoreScope repeater record, while the browser already has the public key and coordinates needed to expose the map action.

## Quality gates

All required gates passed on the final worktree:

- `docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:/work" -w /work/third_party/hopreach golang:1.25.7-bookworm go test ./...` — PASS
- `docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:/work" -w /work/third_party/hopreach golang:1.25.7-bookworm go vet ./...` — PASS
- `frontend/: npx tsc --noEmit` — PASS
- `frontend/: npm test` — PASS (57 tests)
- `frontend/: npm run lint:css` — PASS
- `frontend/: npm run build` — PASS (Vite emitted only its existing large-chunk advisory)
- `git diff --check` — PASS

The backend gate was not required because no backend file was touched.

## Deviations and constraints

- No public or unauthenticated browser compute endpoint was added. The popup selects and polls node metadata; the on-demand execution primitive is `hopreach --node <pubkey>`, intended for the plan's internal/operator-side submitter. This follows the review's requirement not to turn a browser click into an unbounded unauthenticated compute trigger.
- The seven-day/128-entry retention limits are fixed implementation bounds because the current configuration schema only defines the six-hour recompute interval.
- No live node raster smoke test was run: the task explicitly prohibited running the production HopReach binary or touching the running container and `/data` output. Computation/publication behavior is covered by Go unit tests plus the full Go test/vet gates.
- No image, source-offer, digest pin, deployment, service restart, or benchmark artifact was changed. Those remain release/deployment work after the fork is published under the documented licensing and immutable-image process.
