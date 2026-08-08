# Path Core Extract Summary

## What moved

`backend/src/path-core/decoder.ts` now owns the lazy resolver's database-free
decode mechanics: observer-region candidate bounding/indexing, per-column
candidate capping, prefix/ML/anchor emissions, distance and learned-prior
transitions, the unresolved candidate, forward/backward max-product traversal,
and per-position margin/ambiguity calculation.

`backend/src/path-lazy/lazyResolver.ts` remains responsible for packet/node/prior
queries, privacy and network scoping, observer grouping, evidence-map creation,
and shaping the existing `LazyPathResult` DTO. It calls `decodePath` once per
canonical observer group.

## Evidence interface

`PathDecoderEvidence` supplies candidates and direct anchors; the shared weight
table and thresholds; and callbacks for prefix probabilities, ML prefix scores,
directed edge/transition/motif priors, and observed links. Optional callbacks
reserve the Phase-4 corridor interpolation, position-conditional transition,
and ITM-viability evidence without coupling the decoder to storage.

## Behavior notes

Existing score values, geographic gates, candidate ordering/caps, unresolved
baseline, and ambiguity threshold are unchanged. The new score slots
`corridorInterpolation`, `positionConditionalTransition`, and `itmViability`
default to `0`, so they are inert until later phases load evidence and explicitly
enable them. The ML evidence load threshold is also centralized in
`path-shared/scoring.ts`. No changes were needed in `path-lazy/evaluate.ts`.

## Verification

- `npm run build`: passed.
- `npm test`: passed (245/245).
- `evaluate.ts ukmesh 200`: completed without errors; reported Viterbi metrics
  were unchanged to displayed precision. A fixed 32-packet old/new comparison
  returned identical DTOs for every packet (`mismatches=0`).
