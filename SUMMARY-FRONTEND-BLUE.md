# Frontend blue-path migration

## Changes

- Replaced legacy purple/red/completion path state with the canonical multi-observer DTO: `packetHash`, `network`, `canonicalPath`, `observers`, and `confidence`.
- Live overlays now use one canonical route from `resolve-multi`; unresolved hops do not create connecting geometry or a rendered alternate path.
- Added separate blue observer markers, preserving node metadata and click handling.
- Packet-detail maps consume the canonical DTO and no longer render legacy fallback geometry. Lazy paths remain available for the feed view when no multi-observer response is requested.
- Kept confidence-band arc styling and applied per-hop confidence where the DTO provides it.
- Added coverage for canonical aggregation, observer de-duplication, and unresolved-hop gaps.

## Verification

- `cd frontend && npm run build` — passed. Vite emitted the existing large-chunk warning.
- `cd frontend && npm test` — passed: 58 tests, 0 failures.
- `npm ci` was required because `frontend/node_modules` was absent; npm reported existing Node engine/deprecation warnings.

No deploy, push, service restart, or protected-file changes were made. The supplied `PLAN-unify-resolvers.md` remains untracked and was not included in the commit.
