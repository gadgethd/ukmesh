# UKMesh data handoff — 2026-10-23

Branch: `burn/ukmesh-data-20261023`, based on fetched `origin/main`
`73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.
Scope: HermesMind #332 (MQTT Will telemetry) and #631 (former node names).

## Findings and changes

### #332 — verify the fix already on main

The reported fake-sample behavior does **not** reproduce on this checkout.
PR #98, merge `74d3ff156e06fdc5cc93d89f66629cb09494a3c7`, already installed
the correct discrimination. The ingest path is `mqtt/client.ts`'s `status`
branch → `persistStatusTelemetrySample` → `extractStatusTelemetry` →
`insertNodeStatusSample`. Extraction rejects a trimmed, case-insensitive
top-level `offline` status **before** considering numeric fields or either
raw-statistics fallback. Migration `061_node_status_sample_status.sql` already
preserves the top-level status for accepted samples. Neither needed duplicating.

The added sanitized JSON fixture follows the real firmware's envelope and
boot-time statistics shape; it is not a production payload capture. The pinned
[firmware Will builder](https://github.com/gadgethd/MeshCore-MQTT/blob/f7f4b9f1a32a8406149f91185ed63ee0805fa805/examples/simple_repeater/MqttReporter.cpp#L990)
and its [LWT configuration](https://github.com/gadgethd/MeshCore-MQTT/blob/f7f4b9f1a32a8406149f91185ed63ee0805fa805/examples/simple_repeater/MqttReporter.cpp#L1303)
confirm that offline statistics are captured when configuring the MQTT client.
The regression tests exercise public/test ingest, case/whitespace, owner-only
and arbitrary test stats, and the same statistics with online or absent status.
The SQL fixture proves zero inserted Will rows and one unchanged online sample,
including zero counters and the raw statistics object. Live delivery and
retained replay use the same extractor; no broker retain flag is required.

### #631 — retain and resolve former names

New additive migration `064_node_name_history.sql` creates a history table,
function and `AFTER INSERT OR UPDATE OF name` trigger. It captures both old and
new nonblank names atomically with the node write, including the old name on
an existing node's first post-install rename. It covers both `upsertNode`
paths, MQTT status, RF adverts and direct SQL renames. Unchanged names do not
write history. A compact SHA-256 key deduplicates names without imposing the
B-tree tuple-size limit on previously accepted long MQTT origins; it needs no
extension. History survives node cleanup and rediscovery.

No existing table or historical packets are scanned or rewritten. Timestamps
record first retention and the latest naming transition, not a reconstructed
historical period of use. Names lost before this migration cannot be recovered
by it.

`GET /api/nodes/map?q=...` now accepts a literal, case-insensitive substring
(maximum 160 characters) against current names, former names, node IDs and
IATA. It returns the existing paginated map envelope and **unchanged node DTO**
with the current representative name. History follows current identity aliases;
a shared name alone does not merge nodes. Network/observer, privacy, role,
coordinates, freshness, cursor, row and byte limits remain in force. Private
historical names and currently private/test-only alias sources cannot expose
public results. Searches use `Cache-Control: no-store`.

Both the map's desktop/mobile search and public repeater search supplement
their immediate local results with this lookup. Requests debounce for 200 ms,
cancel on query/scope changes and guard results by query, scope and privacy
generation. Current server names take precedence over duplicate local results.
No history array or new field is exposed in public node responses. The API
contract and generated `docs/openapi.yaml` document `q`.

## Verification

All commands below ran in this worktree. PGlite is the existing ignored,
worktree-local installation (`@electric-sql/pglite` 0.5.8); the SQL test creates
an isolated in-memory database, with no server or database URL. It executes
the actual migration in a transaction, the current identity views from
migrations 036/037, both production upsert paths, the sample writer and the
real Express route/repository. PostgreSQL pool methods are redirected solely
to that fixture for the test.

From `backend/`:

```sh
TEST_NODE_HISTORY_PGLITE_MODULE=file:///home/ben/worktrees/ukmesh/burn-vacuum-20261022/.ukmesh-tools/pglite/node_modules/@electric-sql/pglite/dist/index.js node --import tsx --test --test-concurrency=1 src/db/nodeNameHistory.integration.test.ts src/mqtt/statusTelemetry.test.ts src/nodes/publicMap.test.ts src/repositories/nodes.test.ts src/db/migrations.test.ts src/db/nodeIdentity.test.ts src/api/contracts.test.ts src/api/routes/nodeLinks.test.ts
npm run build
npm run contract:generate
npm run contract:check
```

Results: **37 tests passed, 0 failed, 0 skipped**; build passed; contract
generation/check passed (71 API routes and 11 operator routes). The integration
checks cover first rename without a backfill, both upsert paths, repeated
renames, rollback, blank names, ordinary observer touches, literal `%`/`_`,
network/observer scope, private names, stale nodes, identity aliases, shared
names, pagination, invalid queries, long/Unicode names and rediscovery.

From `frontend/`:

```sh
npm ci --cache ../.ukmesh-tools/npm-cache --no-audit --no-fund
npm test
npm run build
PLAYWRIGHT_PORT_BASE=4783 npm run test:e2e -- --grep 'former node names' --workers=1
```

Results: **149 unit tests passed, 0 failed, 6 pre-existing opt-in browser tests
skipped**; production build passed (existing bundle-size warning); **3 browser
checks passed** (public repeater, desktop map, mobile map). Browser tests verify
old-name lookup displays/selects the current name and that a delayed old query
cannot restore stale map results. Initial test-only failures were corrected:
the key is intentionally displayed shortened, the pagination fixture needed
two eligible nodes, and the map test needed its own runtime budget.

From the repository root, `git diff --check` passed. Ignored evidence logs:
`.ukmesh-tools/ukmesh-data-backend-tests.log`,
`.ukmesh-tools/ukmesh-data-frontend-tests.log`,
`.ukmesh-tools/ukmesh-data-frontend-build.log`, and
`.ukmesh-tools/ukmesh-data-e2e.log`.

## Deliberately left out, open questions and remaining work

- No deployment, live migration, VPS access, service restart, live configuration
  change, secret access, retained-message clearing or historical-row deletion.
  Only the new migration was applied to a disposable local fixture. No existing
  migration file changed. The full Timescale migration chain was not exercised.
- No packet-history backfill: it would require separate bounded operator work
  and cannot reliably reconstruct names no longer present. Historical pollution
  predating PR #98 is likewise untouched.
- The existing MQTT status handler still refreshes node metadata/presence for
  an offline envelope through `upsertNode`; rejecting the telemetry sample does
  not change that separate liveness policy. If undesirable, track it as a
  follow-up instead of folding it into this sample-quality fix.
- Migration 063 must precede deploying code that queries name history. No live
  action was required to implement or verify these acceptance gates. Operator
  review, merge and any later rollout remain outside this lane. No branch push
  or PR publication was performed.

PROOF: targeted backend tests green + `docs/reviews/20261023-ukmesh-data.md` committed at sha c51cc744284e04bce766e82d8d649cdbee281c2e
