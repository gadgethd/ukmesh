# New Pathing System Notes

This file captures the current beta pathing implementation and the next planned phases.

## Current live implementation

The active beta path resolver now uses packet-derived first-hop neighbor affinity as a scoring signal.

Implemented and live:

- Added `backend/src/path-beta/affinity.ts`.
- Rebuilt and recreated `meshcore-analytics-backend-1`.
- Recreated `meshcore-analytics-path-history-worker-1`.
- Verified `/healthz` returns OK.
- Verified the live backend container includes `dist/path-beta/affinity.js`.
- Verified backend logs show active `path-beta` and `path-beta-multi` requests after restart.

The resolver now builds an in-memory affinity map from recent packet history and uses it in beta path scoring:

- Direct ADVERT observations with no path create `src_node_id <-> rx_node_id` affinity.
- ADVERT path-prefix evidence can create `src_node_id <-> first path hash` affinity when the prefix resolves to exactly one known node.
- Any packet with path hashes can create `rx_node_id <-> last path hash` affinity when the prefix resolves to exactly one known node.
- Shared-neighbor affinity is derived from the resolved affinity graph using a bounded Jaccard-style boost.

Guardrails:

- Affinity does not write to `node_links`.
- Affinity does not bypass `context.linkPairs`; it only boosts ranking/confidence after an existing viable-link gate passes.
- Ambiguous prefixes are ignored instead of guessed.
- Scores are capped so affinity cannot dominate stronger confirmed evidence.

Environment knobs:

- `PATH_BETA_AFFINITY_WINDOW_DAYS`, default `14`
- `PATH_BETA_AFFINITY_MIN_OBSERVATIONS`, default `3`
- `PATH_BETA_AFFINITY_MAX_EDGES`, default `20000`

## Current scope versus full parity

This is not full feature parity with the larger pathing model we discussed.

What we implemented:

- Backend beta-path affinity scoring.
- In-memory neighbor affinity from packets.
- Shared-neighbor boost.
- Live use in active beta pathing and path history worker.

What we do not yet have:

- Persisted `neighbor_edges` or equivalent table.
- Persisted `resolved_path` on every observation.
- One-time historical backfill for resolved paths.
- Frontend preference for server-resolved path fields.
- REST affinity API endpoints.
- Show Neighbors powered by affinity.
- Node detail neighbors section.
- Affinity debugging tools.
- Neighbor graph visualization.
- Candidate inspector for ambiguous path prefixes.

## Recommended next phase

If we want full parity, the next phase should be persistence, not more resolver tweaks.

Suggested order:

1. Add a persisted neighbor-affinity table separate from `node_links`.
2. Add an incremental builder/backfill job for historical packet observations.
3. Add a `resolved_path` storage strategy for packet observations or path history records.
4. Update beta resolver context to load persisted affinity instead of rebuilding from packets on context refresh.
5. Add diagnostics/API endpoints for inspecting affinity edges per node and per ambiguous prefix.
6. Add frontend surfaces only after the persisted model is stable.

## Important implementation note

The current implementation is intentionally conservative because our pathing system already has ITM viability, observed links, learned transition priors, motif priors, edge priors, and fallback logic. Treating packet-derived affinity as a gate would risk creating false certainty from noisy or ambiguous path hashes.

For now, affinity should remain a ranking/confidence prior unless we add a persisted model with clear diagnostics and backfill validation.
