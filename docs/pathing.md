# Pathing

## Purpose

Pathing resolves packet routes into confident relay chains. The production
pipeline (2026-08) is the Viterbi champion (`vit_src`) from the pathing
experiment: it walks a candidate trellis built from 1-byte hop prefixes and
scores emissions/transitions from seven calibrated signals, including
positional prefix-frequency priors, corridor interpolation, and **ITM
terrain feasibility** (no hop is placed where line-of-sight physics rules it
out). Measured on the gold holdout set: **97.27% route accuracy / 99.24%
hop accuracy**.

## Main layers

- `backend/src/path-beta/`
  - resolver implementation (`resolver.ts`) — Viterbi candidate trellis
  - worker pool
  - geometry helpers (terrain-aware arcs)
  - fallback logic
- `backend/src/pathing/`
  - service/repository orchestration for API-facing pathing endpoints
  - `pathingPublicDto.ts` — the public DTO projector. **Any new resolver
    field must be added here or the API silently drops it.**
- `backend/src/pathing/pathingService.ts`
  - cache and resolver orchestration

## Production behaviour (2026-08-06)

- **Slow-mode resolution**: packets wait out their propagation window before
  the path is finalised (`469c84e`), so late observations don't re-shape a
  settled route.
- **Physics gates + held-path refinement** (`5dd15d9`): candidate hops that
  violate physical feasibility are gated; held paths are refined in place.
- **Canonical path DTO**: `canonicalPath` is the authoritative route in API
  responses; the frontend renders it as blue paths. Path identity colours
  live-path arcs by the resolved packet (`f6f8290`).
- Terrain-aware live paths + airborne-hop markers (`a76ab1e`); live path arcs
  stay above terrain (`7bcabcc`).

## Evidence priorities

In practice the resolver should rank evidence roughly like this:

- physically plausible link support (ITM/LOS)
- multibyte path evidence
- radio-neighbour evidence
- weaker observational hints

## API ownership

- `backend/src/api/routes/pathing.ts`
  - thin HTTP wrapper
- `backend/src/pathing/pathingService.ts`
  - cache and resolver orchestration
- `backend/src/pathing/pathingRepository.ts`
  - DB-backed path history and learning queries

## Contributor rule

If a path looks wrong, first determine whether the issue is:

- evidence weighting
- physical graph quality (ITM feasibility)
- ambiguous short-hash matching
- cache reuse
- a missing field in the public DTO projector (`pathingPublicDto.ts`)

Do not debug path rendering as if it were a frontend-only problem.
