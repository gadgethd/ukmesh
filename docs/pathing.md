# Pathing

## Purpose

Pathing resolves packet routes into higher-confidence `purple` segments and lower-confidence `red` segments.

## Main layers

- `backend/src/path-beta/`
  - resolver implementation
  - worker pool
  - geometry helpers
  - fallback logic
- `backend/src/pathing/`
  - service/repository orchestration for API-facing pathing endpoints

## Evidence priorities

In practice the resolver should rank evidence roughly like this:
- physically plausible link support
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
- physical graph quality
- ambiguous short-hash matching
- cache reuse

Do not debug purple/red rendering as if it were a frontend-only problem.
