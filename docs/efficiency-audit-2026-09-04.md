# MeshCore Analytics efficiency audit

Date: 2026-09-04. Original VPS source baseline: `1eefb50`.
Publication base: GitHub `main` at `2c2b9d9`.

## Scope and reversibility

This was a structural audit of Analytics' backend, frontend, workers, build
configuration, dependencies, and deployment boundaries, followed by targeted
changes and measurements. The sibling infrastructure, Beacon, Discord, and
health-check projects were inspected for context. This is not a claim that
every line of the vendored RF engine or every sibling application was reviewed.

Changes were prepared in isolated Git worktrees and published as independent
pull requests. The operational checkout, local staged/untracked work,
configuration, live containers, data, and retained backups have not been
changed. Nothing has been deployed. Database measurements used bounded, read-only transactions;
integration tests used disposable PostgreSQL/Redis instances. No migrations,
retention changes, sampling, reduced RF precision, or slower refresh intervals
are involved.

## Implemented improvements

| Area | Change | Evidence / expected benefit |
| --- | --- | --- |
| Shared backend cache | Skip expiry scans until an entry could actually expire | Eliminates a full cache scan on every write; particularly relevant to the 50,000-entry packet invalidation cache |
| Health path diagnostics | Combine two packet scans and repeated path-array traversals into one query | Three-run mean execution time: 2,695 ms → 1,041 ms, about 61% lower |
| Observer ingest health | Derive public/latest/test timestamps in one grouped scan | Three-run mean: 1,053 ms → 834 ms, about 21% lower |
| WebSocket lifecycle | Register close/error cleanup before awaiting initial-state queries | Failed initial states and clients disconnecting during loading now release per-client state and update the client gauge |
| Browser live feed | Only copy, index, sort, and publish the chat-message array when a batch contains chat | Advert/other packet traffic preserves the `useMessages()` snapshot and does not trigger chat-only subscribers to render |
| Frontend build | Emit only MapLibre's production worker and its shared module | Removes 2,878,846 bytes (2.75 MiB) of duplicate/development assets per build |
| Dependency cleanup | Remove three unused runtime dependencies and one unused type dependency | Eight packages disappear from the lockfile, with no changes to retained package versions or metadata |
| Dead source | Remove an unused packet-panel re-export and an unused frame scheduler with its dedicated test | No references from runtime, build, CLI, or worker entrypoints; the actual packet panel and active map scheduling remain |

The MapLibre saving is deployed artifact size, not a claimed reduction in
normal browser downloads: these unused files were generally not requested.
The dependency cleanup similarly reduces installation/build contents; the
bundler already excluded unused imports from browser JavaScript.

### Cache measurements

Same Node v22.22.1 process, warmed once, fixed clock, unit-sized values,
identical count/byte/TTL limits and metrics in both implementations. Each
sample fills the cache, then overwrites 2,000 entries. These are synthetic
operation timings, not an end-to-end server throughput claim.
The larger fixtures exercise configured capacities, not measured production
occupancy; their speedups must not be treated as whole-system savings.

| Entries | Fill before | Fill after | 2,000 updates before | 2,000 updates after |
| ---: | ---: | ---: | ---: | ---: |
| 4,096 | 109.54 ms | 5.40 ms | 103.50 ms | 5.20 ms |
| 20,000 | 2,999.13 ms | 32.69 ms | 660.10 ms | 6.55 ms |
| 50,000 | 24,398.52 ms | 82.88 ms | 1,978.65 ms | 9.99 ms |

The new expiry deadline is a conservative lower bound. Replacements, deletes,
LRU reads, clock movement, cold expiry, count/weight limits, and metrics retain
their existing semantics. A due sweep still visits the cache; ordinary fresh
writes no longer do so.

### Database measurements and parity

`benchmarkHealthQueries.ts` compares the original SQL with the new SQL inside
one repeatable-read, read-only transaction. It pins the time/data snapshot,
checks both directions with `EXCEPT ALL`, alternates query order, and takes
three timings with `EXPLAIN (ANALYZE, TIMING OFF)`. Individual statements have
a 30-second timeout. The output contains aggregate parity/timings only.

| Query group | Before, runs 1 / 2 / 3 (ms) | After, runs 1 / 2 / 3 (ms) | Different result rows |
| --- | --- | --- | ---: |
| Ingest | 908.416 / 1,215.896 / 1,033.783 | 643.162 / 1,076.594 / 783.260 | 0 |
| Paths | 2,369.023 / 2,688.763 / 3,026.867 | 976.155 / 1,022.783 / 1,123.384 | 0 |

Combined mean execution time for these groups fell from approximately 3.75 s
to 1.88 s. Health refreshes are scheduled every minute. This reduces recurring
database work, but it does **not** establish a 50% reduction in total server CPU.
There was other live workload during measurements; these are not controlled
whole-system benchmarks.

The query continues to count actual hash lengths, including malformed and
mixed-width historical arrays. It does not infer lengths from the packet's
declared width. Public/test exclusions, null handling, time boundaries,
per-packet multibyte counts, and global last-ingest behavior are retained.

## Dead-code findings

Removed direct dependencies:

- `@deck.gl/extensions`
- `d3-force`
- `polygon-clipping`
- `@types/d3-force`

The four additional removed transitive packages are `d3-dispatch`,
`d3-quadtree`, `robust-predicates`, and `splaytree`.
`react-is` has no direct application import but is required by Recharts as a
peer dependency, so it stays. Active Deck heatmaps/arcs also stay.

Removed source:

- `frontend/src/components/PacketDetailPanel.tsx`: unused two-line re-export;
  callers already import `pages/ukmesh/PacketDetailPanel.tsx`.
- `frontend/src/utils/frameSnapshotScheduler.ts` and its sole test: no
  runtime/build consumer remains after the topology changes.

Retained intentionally:

- `backend/src/api/routes/coveragePagination.ts`, dormant planner state, old
  coverage producer/worker code, and legacy `410` API tombstones: documented
  rollback/compatibility retention in `docs/architecture.md`.
- `path-lazy/lazyResolverLegacy.ts`: used by the offline evaluation tool.
- `cache/policyRegistry.ts`: used by the cache audit test.
- Type declarations, worker entrypoints, backfill tools, vendored source,
  database migrations, operational scripts, and backups: absence of an import
  from the main application does not make these removable.

## Next opportunities, in priority order

1. **Measure the remaining database workload over a representative period.**
   There is no representative ranked query-cost history from this audit.
   The existing application histogram aggregates by pool/outcome,
   not query family. Add bounded query-family timing or arrange query-statistics
   collection before selecting the next expensive SQL rewrite. Do not infer
   per-minute rates from cumulative table-scan counters.

2. **Correct resolver-context memory accounting with heap measurements.**
   `path-beta/resolver.ts` stores several large Maps in `contextCache` while
   the default cache weight uses `JSON.stringify`. Map contents serialize as
   `{}`, so nodes, links, and model Maps are undercounted. The repeater array
   is counted, but it does not represent the whole context. An explicit
   estimator and realistic heap fixture would make the 128 MiB limit useful.
   Tightening this blindly could cause expensive context rebuilds, so validate
   memory and cache hit rates together before choosing the weights.

3. **Profile remaining raw-history statistics and chart regeneration.**
   `statsRepository.ts` still has seven-day distinct/grouped packet scans,
   including observer-region summaries. Aggregate reads are enabled in the
   current backend, but that does not eliminate every raw-history query.
   Extending durable aggregates requires exact counting, observer/region scope,
   privacy-generation invalidation, and late-arrival parity. Increasing TTLs or
   dropping detail would change behavior and was not used here.

4. **Resolve frontend performance-gate drift, then target browser bundles.**
   Both baseline and changed builds fail the existing MapLibre/CSS budgets.
   The Recharts matcher still expects `generateCategoricalChart-*` while the
   current build emits `CartesianChart-*`; MapLibre emits a tiny CSS-import
   wrapper plus its main chunk, conflicting with an exactly-one assertion.
   The all-JavaScript check also omits `.mjs` worker assets. Fix accounting and
   collect route-specific loading evidence before adjusting chunks. Splitting
   a file solely to satisfy its name-based limit would not reduce browser work.

5. **Retire the documented old coverage implementation after closing its
   rollback window.** This would remove substantially more source than the
   confirmed dead modules removed here. The Python link worker shares parts
   of that implementation and must keep observed-link/RF calculations intact.
   Keep a release image/source reference, prove the link-only dependency graph,
   and preserve the public tombstones before removing shared code.

6. **Treat sibling projects separately.** The shared infrastructure project
   intentionally owns persistent TimescaleDB/Redis/MQTT resources. Beacon's
   background tasks have separate schedules and materialized-view refreshes;
   the health-check service still performs synchronous JSON persistence;
   Discord's JSON store uses queued atomic writes and fsync. These are candidates
   for measured follow-up, not grounds to merge services, remove durability,
   or delete seemingly duplicate data. Analytics remains the priority.

## Original VPS-baseline verification

- Backend typecheck and build: pass.
- Backend unit suite: 321 passed after registering the benchmark's local cache
  in the existing cache-policy inventory.
- An additional deterministic 10,000-operation cache comparison matched a
  reference LRU/TTL model, including backwards clock movement.
- Frontend unit suite: 97 passed; build passes. The obsolete scheduler's test is removed
  together with that module, and a message-snapshot regression test is added.
- PostgreSQL integration: empty data, null/empty/malformed/mixed-width arrays,
  time boundaries, public/test transitions, and timestamp ties pass.
- WebSocket integration: an initial-state database failure closes with 1013
  and the server client count returns to zero.
- Production-build browser smoke: one map canvas, production worker/shared
  `.mjs` requests both return 200, no page errors.
- Docker Compose configuration: pass with the existing operational environment
  read only for variable resolution.
- Lockfile audit: eight packages removed; retained entries unchanged.
- Existing `npm run budget`: fails on both baseline and changed build. Main
  MapLibre remains approximately 958 KiB raw / 245 KiB gzip; CSS remains
  240.5 KiB raw / 42.3 KiB gzip. No limits were raised.
- Existing `npm run contract:check`: fails on both checkouts because
  `docs/openapi.yaml` is already stale. No API contract was changed here.
- Full browser suite in the matching Playwright container: 15 passed, 17 failed.
  Failures included loading/axe timeouts and dashboard navigation receiving
  `Connection header did not include 'upgrade'` from the development server.
  Two representative dashboard failures were reproduced in a separate clean
  checkout of `1eefb50` with its own locked dependencies. The remaining failures
  have not all been classified; a clean full browser run is still required
  before release. No failing assertions or timeouts were weakened.
- A 30-minute staging browser soak and deployment verification are still
  release steps; no staging or production deployment was performed.

## Publication verification against current GitHub main

All implementation changes were combined in a separate integration checkout
based on `2c2b9d9`, then split into five independent, single-commit PRs with the
same base. The PRs preserve the VPS implementation changes while retaining
already-merged GitHub fixes. No incoming GitHub changes were pulled into the
operational VPS checkout, and no main branch or release tag was pushed.

- Fresh locked installs, backend typecheck, and both production builds pass.
- Backend: 325 unit tests pass. Frontend: 97 unit tests pass (422 total).
- OpenAPI contract check passes; the newer GitHub base already contains the
  contract documentation repair that was absent from the original VPS baseline.
- Both added PostgreSQL/WebSocket integration tests pass again against empty,
  disposable services. No production database was used for publication tests.
- Production-build browser smoke passes with MapLibre 6.5.0: one map canvas,
  worker/shared modules return HTTP 200, and no page errors. APIs, WebSocket
  input, and external basemap data were fixtures, not a full live-service test.
- Dependency cleanup removes exactly eight package entries and changes no
  retained lockfile entry from GitHub main. Its MapLibre/Vite dependency
  updates remain intact. With MapLibre 6.5.0 the four omitted assets total
  2,897,888 bytes, compared with 2,878,846 bytes in the original 6.4.0 audit.
- Full browser suite: 21 passed, 11 failed. Dashboard failures again include
  an HTTP response containing `Connection header did not include 'upgrade'`;
  a topology accessibility test also failed. Not all failures are classified.
  Frontend PRs #87 and #89 remain drafts pending a clean browser gate.
- The existing frontend size-budget check still fails (MapLibre, CSS, and
  chunk-name matching). No budget limits or test assertions were weakened.

The staged VPS vacuum-script fix is already byte-identical to GitHub's #78,
so it does not need another PR. Older untracked tests requiring absent
implementations, operational audit logs/reports, screenshots, credentials,
configuration, backups, and local historical branches were not published.
The original local files and original efficiency branch remain available.
Separate sibling repositories were not modified or pushed as part of this
Analytics publication.

## Reproduce and roll back

From a checkout containing the relevant implementation PRs, in the backend directory:

```sh
npm ci
npm run typecheck
npm test
npm run build
node --import tsx src/tools/benchmarkBoundedCache.ts
node --import tsx src/tools/benchmarkHealthQueries.ts DB_CONTAINER 1eefb50
```

The SQL benchmark requires a container with `psql`, `POSTGRES_USER`, and
`POSTGRES_DB`, and deliberately performs no writes. To compare cache revisions,
run the same benchmark driver against the old/new `BoundedTtlMap` implementations
in isolated checkouts with the same dependencies and clock.

For the integration tests, point `TEST_DATABASE_URL` to a disposable PostgreSQL
database, and point `WS_LIFECYCLE_TEST_DATABASE_URL` and
`WS_LIFECYCLE_TEST_REDIS_URL` to an empty disposable database and Redis instance:

```sh
node --import tsx --test src/health/packetDiagnostics.integration.test.ts
node --import tsx --test src/ws/server.integration.test.ts
```

For frontend checks, run `npm ci`, `npm test`, `npm run build`, `npm run budget`,
and `npm run test:e2e`. Use a matching Playwright container when the host lacks
the required browser libraries.

The operational checkout has not adopted these PRs, so there is currently
no live rollback to perform. Review each PR and adopt the desired changes
into a clean release checkout. The implementation is split
into separate cache, SQL, WebSocket, chat, and cleanup commits so each can be reverted
with `git revert COMMIT` without discarding later work. There are no database
down-migrations. Any later deployment should retain the previous immutable
images and follow `docs/runbook-release-rollback.md`.

Implementation commits:

| PR | Published commit | Scope |
| --- | --- | --- |
| [#86](https://github.com/gadgethd/ukmesh/pull/86) | `0f8a98f` | Cache expiry scans, regression coverage, benchmark |
| [#88](https://github.com/gadgethd/ukmesh/pull/88) | `e77c82a` | Health SQL, parity benchmark, PostgreSQL fixture |
| [#90](https://github.com/gadgethd/ukmesh/pull/90) | `01c5c34` | WebSocket cleanup and failure-path integration test |
| [#87](https://github.com/gadgethd/ukmesh/pull/87) | `3772c62` | Chat snapshot efficiency |
| [#89](https://github.com/gadgethd/ukmesh/pull/89) | `1a40fd6` | Unused dependencies, source, and build assets |

To undo the complete implementation after adopting these commits, use their
hashes in reverse order in the release checkout:

```sh
git revert 1a40fd6 3772c62 01c5c34 e77c82a 0f8a98f
```

This preserves later Git history. If the changes were squash-merged or
cherry-picked and the hashes changed, use the corresponding commits from the
release checkout instead. Before merge, closing a PR leaves main unchanged.
