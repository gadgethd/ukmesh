# Item #585 — regions Phase B

Branch: `burn/ukmesh-regions-20261023`, based on fetched `origin/main`
`73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.
Implementation commits: `43e9a43728df3c96436612f87f978659a404830b` and
`55cb9b3732bb7f02f7ec16ebaa51633547ac2086`.

## What changed and why

- Added `PUBLIC_FEATURE_REGION_SCOPING_ENABLED`, default **false** in code,
  Compose and `.env.example`. Runtime configuration exposes `regionScoping`;
  older/missing configuration keeps the selector disabled. With no selection,
  the existing network/observer queries, payloads and request URLs remain in use.
- Added an optional `region` filter to `/api/nodes`, `/api/nodes/map`,
  `/api/nodes/:id/links`, `/api/packets/recent` (aggregated and raw),
  `/api/path-beta/multibyte-paths`, and `/ws`. Names are case-sensitive ASCII
  public names of 1–64 characters: letters/digits followed by letters, digits,
  underscores or hyphens. A leading `#` is accepted. Ambiguous/invalid input
  returns 400 before reads; an explicit selection while disabled returns
  `FEATURE_DISABLED`. A valid unknown region yields an empty regional view.
- Respected Phase A's representations: membership names are bare; packet
  transport scopes have a leading `#`. Packet filtering happens before both
  representative selection and observer/count aggregation, including RAM
  pending observations and initial message history. Existing privacy predicates
  and network/observer intersections remain active.
- Added migration `063_packet_region_scope_index.sql`: a partial
  `(region_scope, time DESC)` index. Queries use exact parameterized equality
  within the existing five-minute recent-packet / 24-hour message/raw windows.
  Nodes use the Phase A `(region, node_id)` key plus indexed alias/node lookups.
  The regional node source mirrors the canonical view's representative and
  aggregate semantics without materializing the entire global node view.
- Added an accessible desktop/mobile selector populated from Phase A's summary.
  Selection is shareable as `#region=eng-ne`, preserves search parameters and
  other hash parameters, and supports reload, Back/Forward and “All regions.”
  The disabled feature ignores region hashes and makes no summary/filter requests.
- Scoped WebSocket snapshots, caches and broadcasts. Region changes clear node,
  link, pin and path state; old epochs, sockets and requests cannot repopulate
  the new scope. External-store resets run after React commits and before paint,
  avoiding updates to mounted subscribers during render. GPU overlays stay
  mounted while their logical path jobs/caches reset.
- Updated generated OpenAPI documentation and added repeatable region test scripts.

## Verification

All commands ran locally. Package commands below use the indicated directory.

| Directory | Exact command | Result |
| --- | --- | --- |
| `backend` | `npm run test:regions` | 36 passed, 0 failed/skipped |
| `backend` | `TEST_REGIONS_DATABASE_URL=postgres://postgres@127.0.0.1:32880/postgres TEST_REGIONS_REDIS_URL=redis://127.0.0.1:32881 npm run test:regions-integration` | 2 passed, 0 failed/skipped |
| `backend` | `node --import tsx --test --test-concurrency=4 $(find src -name '*.test.ts' ! -name '*.integration.test.ts' -print)` | 579 passed, 3 existing skips, 0 failed |
| `backend` | `npm run build` | Passed |
| `backend` | `npm run contract:check` | Passed: 71 API and 11 operator contracts |
| `frontend` | `npm test` | 153 passed, 6 existing skips, 0 failed |
| `frontend` | `npm run build` | Passed; existing large-vendor-chunk warning |
| `frontend` | `PLAYWRIGHT_PORT_BASE=4293 npx playwright test regions.spec.ts --project=dashboard-desktop --project=dashboard-mobile --workers=1 --repeat-each=2` | 12 passed; no captured render errors/warnings |
| worktree root | `git diff origin/main --check` | Passed |

The SQL/WS integration tests create and drop their own databases and refuse
non-loopback fixture URLs. Fixtures used fresh disposable containers from
`timescale/timescaledb:2.25.1-pg16` and `redis:7-alpine`, with random ports bound
only to `127.0.0.1`. No application database was used.

Real SQL coverage includes 12,000 background nodes, 24,000 background packets,
canonical aliases, private nodes, other networks, unknown regions, and the same
RF hash observed under two region scopes. It executes both durable and RAM
overlap SQL. `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` asserts indexed regional
paths and no sequential scans of nodes, membership or packet chunks in the
tested selective-region plans; packet plans use the new region/time index.
These are fixture plan checks, not production latency measurements.

Real WebSockets verify default-off admission, separate global/regional/unknown
snapshots, cache isolation, initial messages, regional live packets, and relay
membership that cannot grow from an out-of-region packet endpoint. Browser tests
verify URL state, default requests, scope changes, pinned state, late socket/HTTP
responses, runtime disable, and a selector that fits the mobile viewport.
Desktop/mobile screenshots were inspected locally.

### Existing failures outside this change

- `frontend: npm run lint:css` fails with seven duplicate selector groups and
  one 9px text declaration. Running the same checker against an archived copy
  of the base commit reproduces the exact eight findings in untouched files.
- `frontend: npm run budget` fails on the existing MapLibre size/duplicate
  chunk match, missing legacy Recharts chunk name, and aggregate JS/CSS limits.
  A separate build of the archived base commit reproduces all four failed
  categories. The App chunk passes: base 142.1 KiB raw / 40.4 KiB gzip;
  this change 144.7 KiB raw / 41.2 KiB gzip, against 900 / 260 KiB limits.

## Deliberately left out, questions and remaining work

- No deployment, VPS access, service restart, live migration/backfill, live
  configuration change, merge, push or PR publication. Only the two local test
  containers were created and removed. The feature remains default-off.
- Network summary counters remain UK Mesh totals; the selector explains this.
  Historical link counts retain their existing lifetime meaning while endpoints
  are restricted to region members. Per-packet detail/path resolution remains
  keyed by packet identity and network.
- Phase A attribution/rebuild logic is unchanged. Existing regional sockets use
  the membership captured in their snapshot; a reconnect or a new selection
  obtains refreshed membership. Periodic membership refresh is a possible
  follow-up, not a new background scan added here.
- No implementation blocker remains. Before any separately authorized rollout,
  verify migration 063 and Phase A participation data, confirm ingest probe-name
  coverage and case for the intended regions, then decide when to enable the
  flag. This session intentionally gathered no live-data evidence.
- Separate follow-ups can reconcile the existing CSS and bundle budget failures.
