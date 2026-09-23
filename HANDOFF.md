# Mission B4 — small batch

## Status

Complete on `fix/ukmesh-b4-small`, based on `origin/main` at `2fa80fa`.

## Item disposition

- **#301 — fixed.** Added the retired-workload policy for `path-history`, re-exported `normalizeRetiredAnalysisState` from `runState.ts`, normalized retired state in readiness output, and retained the original failure under `retirement.historicalState`. New leases for the retired workload are rejected. Coverage exercises normalization, audit retention, active-run visibility, and rejection in both scopes.
- **#303 — fixed with measured rows (option a).** A `ResizeObserver` records rendered border-box heights by scope and packet hash. Prefix offsets drive both spacers and the visible range; unseen rows use 76px only as an initial estimate. Scroll anchoring preserves the first visible packet as measurements and packet order change. Row content remains unclipped.
- **#269 / #213 — triaged as expected denials.** Keep the `/raw` denials. The owner ACL renderer and backend topic parser accept the four supported suffixes; raw packet bytes are the `raw` JSON field on `/packets`, and the backend has no `/raw` topic consumer. Documented the log evidence and ruling in `docs/operations.md`; added a parser assertion that `/raw` remains unsupported. No ACL renderer change is needed.

## Mission commits

- #301: `d834ba8` — `fix(analysis): restore retired run-state policy (#301)`
- #303: `078e21b` — `fix(feed): measure UK feed virtual row heights (#303)`
- #269 / #213: `6c47b7f` — `docs(mqtt): document denied raw publish disposition (#269 #213)`

## Tests and typechecks

- `cd backend && node --import tsx --test $(find src -name '*.test.ts' ! -name '*.integration.test.ts' -print)` — **328 passed, 0 failed**.
- `cd frontend && npm test` — **100 passed, 0 failed**.
- `cd frontend && node --import tsx --test src/hooks/measuredVirtualRows.test.ts` — **3 passed, 0 failed**.
- `cd backend && npm run typecheck` — **passed**.
- `cd frontend && ./node_modules/.bin/tsc --noEmit -p tsconfig.json` — **passed**.

## Gated items and open question

No service/container actions, live database writes, ACL edits or reloads, pushes, or merges were performed. Database integration and browser E2E suites were not run. The supplied log review does not identify who owns the `meshmonitor-observer EXT` publisher; that does not change the ACL disposition.

**Next:** coordinator/Hermes can relay the local commits. Trace the `meshmonitor-observer EXT` publisher only if stopping its unsupported `/raw` attempts is desired.
