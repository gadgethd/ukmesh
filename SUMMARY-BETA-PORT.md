# Beta shared-decoder port summary

## Status

Phases 2 and 3 from `PLAN-unify-resolvers.md` are implemented on
`pathing/beta-port`.

## What changed

- Added `backend/src/path-beta/sharedDecoder.ts`, a beta evidence adapter for
  the shared `path-core/decoder.ts` Viterbi decoder and the sole production
  `path-shared/scoring.ts` weight table.
- Ported the single-observer `resolve` job to the shared decoder. Observer
  bounds, direct-receiver anchors, learned prefix/edge/transition/motif priors,
  ML prefix scores, observed links, and decayed sticky assignments are supplied
  as decoder evidence.
- Ported `resolveMulti` to lazy-style prefix-compatible grouping. Each group is
  decoded once, and every observer result is projected from that canonical
  decode rather than solved independently and stitched.
- Derived beta confidence from shared-decoder per-position margins, followed by
  the existing model calibration.
- Removed the legacy inline-weight trellis, solve/retry/suffix cascade,
  permutation enumeration, red fallback path builder, affinity scoring module,
  and their now-unused constants/geometry helpers.
- Removed fallback output mode from the beta resolver. Unresolved decoder
  positions split the rendered blue path into contiguous `purplePath` and
  `extraPurplePaths` segments; no edge is invented across a gap.
- Preserved the worker job types and DTO field shape. Compatibility fields are
  always `permutationCount: 0`, `redPath: null`, `redSegments: []`, and
  `completionPaths: []`; beta results now use only `mode: 'resolved' | 'none'`.
- Added focused tests for canonical grouping, beta evidence flowing through the
  shared decoder, and gap-safe per-observer projection.

## Verification

Run from `backend/`:

```text
npm run build
npm test
```

Results:

- TypeScript build: passed.
- Backend tests: 248 passed, 0 failed.

The production-data accuracy harness was not run in this worktree; its beta
extension is owned by the separate eval-harness phase/branch described in the
plan. This summary therefore makes no new numerical accuracy claim.

## Operational actions

No push, deployment, service restart, or other runtime mutation was performed.
