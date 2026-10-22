# `/api/mqtt-nodes` performance review — 2026-10-22

Branch: `burn/mqtt-nodes-perf-20261022`
Base: `bec753b5e95893acf159d0ef08ce79057d98c6f8` (`main`)
Implementation and evidence commit: `eef1244eca884b4f03c12775c2e2e9fcb47895a8`
PR: https://github.com/gadgethd/ukmesh/pull/142

The proposed handler meets the <1 second p50 target on the real anonymous
UKMesh scope: **548.563 ms HTTP p50**, including a reused SSH transport to the
production database. SQL execution p50 fell from **6,893.302 ms to 203.435 ms**.
This is a branch implementation measured through a local handler harness, not
a deployment or a measurement of a changed public production endpoint.

## Change and rationale

The packet subquery now counts observations by raw `rx_node_id` first, then
canonicalizes those receiver groups and sums their counts. The measured plan
reads 129,755 observations but canonicalizes just 26 receiver groups, instead
of applying the stable SQL function while grouping every observation.
`SUM(bigint)` returns PostgreSQL `numeric`, so the final sum is explicitly
cast back to `bigint`, preserving the original `COUNT(*)` result type and
string serialization. Each observation still counts, including repeated packet
hashes, and multiple raw aliases still contribute to one canonical receiver.

The identity views, representative-name selection, uptime-first status ordering,
15-minute/24-hour windows, network parameters, hidden-name exclusion, zero-count
filter and public DTO remain unchanged. Anonymous callers and legacy network
labels expand to `['ukmesh','northeast','teesside']`; test traffic stays separate.

The source change is 12 added / 5 removed SQL lines. Other files contain
regressions, the original query as an independent oracle, an opt-in read-only
benchmark and review evidence. The existing PostgreSQL integration command now
includes the new regression. No package dependency or lockfile changed.

## Measurements and alternatives

All VPS database commands used `default_transaction_read_only=on`, explicit
`BEGIN READ ONLY`, a 30-second statement timeout and a 2-second lock timeout.
The equality and interleaved comparison ran in repeatable-read transactions,
so both variants saw the same data and `NOW()`. Five SQL samples per variant
were interleaved, reversing variant order on alternate rounds. No cache flush,
restart, database object creation, migration or data modification was used.
These are small, mostly warm-cache samples under concurrent production load;
they do not establish p95 or behavior at a substantially larger future scope.

| Approach | Measured result | Decision |
| --- | --- | --- |
| Original query | SQL execution p50 **6,893.302 ms**; HTTP p50 **8,775.144 ms** | Baseline. |
| Aggregate raw receivers before canonicalization | SQL execution p50 **203.435 ms**; HTTP p50 **548.563 ms** | Chosen: meets target with fresh reads and a small SQL change. |
| Also replace the identity view with name-only representative selection | SQL execution p50 **112.795 ms** | Rejected: saves about 91 ms but duplicates central ranking rules and creates another place to maintain identity semantics. Live snapshot rows matched, but the extra complexity is unnecessary for this target. |
| Precomputed canonical-count rollup, read-path proxy | SQL execution p50 **136.331 ms**, with 26 counts frozen into `VALUES` | Rejected: only about 67 ms below the chosen query, before ingest/refresh/expiry/alias-update maintenance costs. This is a read-path lower bound, not a maintained-rollup benchmark. No materialized view or rollup table was created. |
| 15-second endpoint response-cache prototype over the original query | Warm-hit p50 **4.796 ms**; cold request **8,798.319 ms**; 1 DB query for 6 requests | Rejected: cold latency persists and TTL expiry would allow stale status/counts/names for up to 15 seconds without invalidation. The query rewrite meets the target on every sampled uncached request. |

For the rollup proxy, counts were obtained read-only and represented as constant
rows for the endpoint's read phase. The repeatable reproduction in
`rollup-proxy.sql` takes a fresh snapshot and uses PostgreSQL `format('%L', ...)`
to quote values before EXPLAINing the read phase. It measured **177.284 ms** on
that separate reproduction. Refresh and backfill costs remain unmeasured;
that limitation is part of the reason not to select it.

The first HTTP pass opened a new SSH connection per query: before p50
**12,322.603 ms**, after **1,169.728 ms**. This measures transport setup in
addition to the handler and database, so both that result and the reused-SSH
result are retained. With connection reuse, the after samples were
**657.984, 589.490, 422.218, 548.563, 490.278 ms**, all below one second.
The real Express route, scope resolution, filtering and DTO run locally; only
its database adapter uses remote read-only psql. Bigint fields are explicitly
transported as strings. No remote HTTP server or application process was started.
This does not substitute for a normal pooled-connection check after an operator
chooses to deploy.

Full evidence is in [docs/performance/mqtt-nodes-20261022](docs/performance/mqtt-nodes-20261022):

- `before-plan.txt` and `after-plan.txt`: full initial EXPLAIN ANALYZE/BUFFERS plans.
- `before.sql` and `after.sql`: the exact queries, expanded to production scope.
- `sql-timings.txt`: all five comparison timings per variant and medians.
- `names-only.sql`, `names-only-plan.txt`, `rollup-proxy.sql` and `rollup-proxy-plan.txt`: alternative evidence.
- `http-new-ssh.jsonl` and `http-reused-ssh.jsonl`: verbatim HTTP benchmark output.
- `equality.txt`: live read-only comparison; **23 rows before, 23 after, 0 differences** in either direction using `EXCEPT ALL`; the name-only alternative also had 0 differences.
- `verification.txt`: local test summaries. Plans retain their textual content with trailing whitespace trimmed.

## Cache invalidation semantics

No application, browser or shared-response cache was added. No new
`Cache-Control`, TTL, invalidation hook, background refresh or stale-on-error
behavior exists in the shipped handler. Each request reads the current database
snapshot, so new observations, canonical alias changes and hidden-name changes
are reflected on the next request, subject to normal transaction visibility.
The isolated PostgreSQL test exercises all three changes through the actual
HTTP route. The 15-second cache exists only inside the opt-in benchmark process
for repeated requests to its single fixed anonymous URL; it is not wired into
the application.

## Exact verification commands and output

Initial and final backend checks, from `backend/`:

```sh
npm ci --cache ../.mqttperf-evidence/npm-cache --no-audit --no-fund
npm test
TMPDIR="$PWD/../.mqttperf-evidence/tmp" npm test
npm run typecheck
npm run build
npm run contract:check
node_modules/.bin/tsc --ignoreConfig --noEmit --target ES2022 --module NodeNext --moduleResolution NodeNext --esModuleInterop --skipLibCheck --strict src/api/routes/misc.test.ts src/api/routes/mqttNodes.integration.test.ts
TMPDIR="$PWD/../.mqttperf-evidence/tmp" node --import tsx --test src/api/routes/misc.test.ts
```

The initial suite passed **556/559**, with 3 skipped and 0 failures. The final
suite, including the added contract test, printed:

```text
# tests 560
# suites 0
# pass 557
# fail 0
# cancelled 0
# skipped 3
# todo 0
# duration_ms 91463.570492
Synthetic firing and recovery reached the bounded test receiver.
```

The three skips are pre-existing opt-in live RF/history/export EXPLAIN tests,
not the new regression. Source typecheck, strict test-file typecheck and build
passed. Contract output:

```text
OpenAPI contract current: 71 API and 11 operator routes
```

The focused HTTP tests printed **3 tests, 3 passed, 0 failed/skipped**.
Coverage includes exact public fields, omitted private/future fields, null
counters, count strings above JavaScript's safe-integer range, zero-count
filtering and anonymous/legacy/test request parameters.

The actual PostgreSQL regression command is:

```sh
# From backend; TEST_INGEST_DATABASE_URL must identify a disposable loopback DB.
node --import tsx --test src/api/routes/mqttNodes.integration.test.ts
```

It ran with a fresh loopback-only embedded PostgreSQL **18.4.0-beta.17** fixture
under `.mqttperf-evidence/`, a random test password and worktree-local `TMPDIR`.
The exact local launcher command was:

```sh
TMPDIR="$PWD/.mqttperf-evidence/tmp" node .mqttperf-evidence/run-postgres.mjs
```

The launcher/tooling is ignored local infrastructure, not a runtime dependency.
The test is committed and uses the existing disposable PostgreSQL service in CI
via `npm run test:ingest-integration`; it creates a unique schema in one
transaction and rolls it back. It refuses non-loopback URLs. It uses the real
canonical function and status view from migration 036, fixture names for the
unchanged node view, and the frozen original endpoint SQL as an oracle.

```text
# tests 5
# suites 0
# pass 5
# fail 0
# cancelled 0
# skipped 0
# todo 0
```

These checks cover aliases with case/whitespace normalization, multiple aliases
merging into a canonical receiver, all production labels, isolated test traffic,
24-hour/15-minute windows, null/empty receiver observations, duplicate hashes,
missing names/counters, hidden/stale/zero-count nodes, uptime/time ordering,
`bigint` SQL type, immediate updates and an empty result.

Initial EXPLAIN commands, from the worktree root:

```sh
ssh -p 32720 -o BatchMode=yes -o ConnectTimeout=10 -o ProxyJump=none ben@57.129.131.35 'docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000" meshcore-infra-timescaledb-1 psql -X -U meshcore -d meshcore -v ON_ERROR_STOP=1' < .mqttperf-evidence/before-explain.sql > .mqttperf-evidence/before-plan.txt
ssh -p 32720 -o BatchMode=yes -o ConnectTimeout=10 -o ProxyJump=none ben@57.129.131.35 'docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000" meshcore-infra-timescaledb-1 psql -X -U meshcore -d meshcore -v ON_ERROR_STOP=1' < .mqttperf-evidence/counts-first-explain.sql > .mqttperf-evidence/counts-first-plan.txt
```

The inputs wrap the committed `before.sql`/`after.sql` with `BEGIN READ ONLY;`,
`EXPLAIN (ANALYZE, BUFFERS, SETTINGS, SUMMARY, TIMING OFF)` and `ROLLBACK;`.
The initial output lines, quoted verbatim, were:

```text
 Planning Time: 50.469 ms
 Execution Time: 6607.320 ms
 Planning Time: 59.431 ms
 Execution Time: 207.755 ms
```

The five-round comparison used the same command with input
`.mqttperf-evidence/repeats.sql` and output `.mqttperf-evidence/repeats-plan.txt`.
That input emits each of the committed before/after/name-only queries after
EXPLAIN inside one repeatable-read read-only transaction; the full extracted
timings are committed in `sql-timings.txt`.

The committed rollup-proxy reproduction was run exactly as follows:

```sh
ssh -p 32720 -o BatchMode=yes -o ConnectTimeout=10 -o ProxyJump=none ben@57.129.131.35 'docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000" meshcore-infra-timescaledb-1 psql -X -U meshcore -d meshcore -v ON_ERROR_STOP=1' < docs/performance/mqtt-nodes-20261022/rollup-proxy.sql > .mqttperf-evidence/rollup-proxy-reproduction.txt
```

The HTTP comparison command, from `backend/`, was:

```sh
MQTT_PERF_SSH_TARGET=ben@57.129.131.35 MQTT_PERF_SSH_PORT=32720 MQTT_PERF_SSH_CONTROL_PATH="$PWD/../.mqttperf-evidence/s" node --import tsx src/tools/benchmarkMqttNodes.ts > ../.mqttperf-evidence/http-reused-ssh.txt 2>&1
ssh -p 32720 -o ProxyJump=none -S ../.mqttperf-evidence/s -O exit ben@57.129.131.35
```

Before/after output, quoted verbatim:

```json
{"mode":"before","scope":["ukmesh","northeast","teesside"],"samples_ms":[8775.144,9018.247,8495.012,9025.393,7880.597],"p50_ms":8775.144,"database_queries":5}
{"mode":"after","scope":["ukmesh","northeast","teesside"],"samples_ms":[657.984,589.49,422.218,548.563,490.278],"p50_ms":548.563,"database_queries":5}
```

`git diff --check` and a comparison of the final route SQL against measured
`after.sql` (ignoring comments and whitespace) also passed.

## Continuation completion audit — 15:53 BST

The continuation audited pushed HEAD `f95ad42020c7858830667faf9c545c079f38c41e`
against the brief, original handler, measured queries, fixture oracle and PR.
The original oracle and before measurement match the base handler; the after
measurement matches the current handler after parameter substitution and
removal of comments/whitespace. The response fields, bigint count type,
network/window predicates, alias semantics and ordering are preserved.
No further application change was needed.

[CI run 39785176256](https://github.com/gadgethd/ukmesh/actions/runs/39785176256)
completed successfully at that exact HEAD: Backend, Frontend, Workers and
Compose, and Secret scan. The actual Backend job log confirms that the new
PostgreSQL regression and all four subtests ran and passed, rather than skipping.
The existing PR has no review comments or requested changes. The remote branch
and PR head both matched the local commit when inspected.

The following commands were rerun in the continuation, using the same local
fixture and read-only remote safeguards described above:

```sh
# From the worktree root:
TMPDIR="$PWD/.mqttperf-evidence/tmp" node .mqttperf-evidence/run-postgres.mjs > .mqttperf-evidence/resume-sql-tests.txt 2>&1
# From backend/:
TMPDIR="$PWD/../.mqttperf-evidence/tmp" node --import tsx --test src/api/routes/misc.test.ts > ../.mqttperf-evidence/resume-route-tests.txt 2>&1
TMPDIR="$PWD/../.mqttperf-evidence/tmp" MQTT_PERF_SSH_TARGET=ben@57.129.131.35 MQTT_PERF_SSH_PORT=32720 MQTT_PERF_SSH_CONTROL_PATH="$PWD/../.mqttperf-evidence/s" node --import tsx src/tools/benchmarkMqttNodes.ts > ../.mqttperf-evidence/resume-http-reused-ssh.txt 2>&1
# From the worktree root, repeat the original snapshot comparison, then close the local SSH master:
ssh -p 32720 -o BatchMode=yes -o ConnectTimeout=10 -o ProxyJump=none -S .mqttperf-evidence/s ben@57.129.131.35 'docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000" meshcore-infra-timescaledb-1 psql -X -U meshcore -d meshcore -v ON_ERROR_STOP=1' < .mqttperf-evidence/equality.sql > .mqttperf-evidence/resume-equality.txt
ssh -p 32720 -o ProxyJump=none -S .mqttperf-evidence/s -O exit ben@57.129.131.35
```

The PostgreSQL regression again passed **5/5, zero skips**; focused HTTP tests
passed **3/3, zero skips**. The fresh live comparison again returned **23 rows
before, 23 after, zero differences**. Verbatim fresh HTTP before/after output:

```json
{"mode":"before","scope":["ukmesh","northeast","teesside"],"samples_ms":[10450.707,10538.907,7790.226,8564.191,9159.034],"p50_ms":9159.034,"database_queries":5}
{"mode":"after","scope":["ukmesh","northeast","teesside"],"samples_ms":[777.87,761.479,924.97,784.056,617.875],"p50_ms":777.87,"database_queries":5}
```

All five fresh uncached after samples were below one second. These are another
small sample through the same local handler/remote read-only adapter, with the
same transport and production-deployment limitations stated above. Committed
`resume-http-reused-ssh.jsonl`, `resume-equality.txt` and
`resume-verification.txt` retain this evidence. The continuation changes only
the review and evidence; the application and tests remain at implementation
commit `eef1244eca884b4f03c12775c2e2e9fcb47895a8`.

## Deliberately left out, open questions and rollback

No identity-view rewrite, materialized count table, schema migration, HTTP
cache, invalidation bus, refresh worker, index or new runtime dependency was
added. No unrelated endpoint or frontend behavior was changed. No agent was
spawned, no live service restarted, no live config edited, and no live database
write or migration was performed. Only this worktree's branch was pushed; no
merge, release or deployment was requested.

The operator still needs to review/merge and decide whether to deploy. After a
normal deployment, confirm the actual pooled-connection endpoint latency and
continued SQL-plan shape under the live request rate. The new PostgreSQL
regression is wired into CI; current PR checks are authoritative for remote
status. No claim is made that every unrelated repository integration suite was
run locally. Future packet-volume growth can justify revisiting rollups; ingest
maintenance, moving-window expiry and alias-update backfills need separate
measurement if that happens.

Rollback is an application-code revert of the implementation commit, through a
separate reviewed PR. There is no migration or database object to roll back and
no rollback SQL is required. Reverting restores the original query and its
latency; it does not modify data. This session performs no rollback or deploy.

The previous root review covered unrelated historical work; it remains in git
history at the base commit. This file now describes the requested performance
branch. The following documentation commit records the PR URL and this review;
its exact final HEAD is reported in the handoff and verified against the remote.

## Honest mistake inventory

- The first HTTP pass included a new SSH connection for every query; its
  1,169.728 ms after-p50 cannot establish a sub-second HTTP result. It is
  retained alongside the connection-reuse measurement, with transport scope
  stated explicitly.
- The first multiplexing attempt exceeded the Unix socket pathname limit and
  failed before issuing a query. The benchmark now normalizes the pathname,
  rejects overlong paths and was rerun with a short socket path.
- Optional strict checking of test files found implicit-any parameters in the
  new generic mock and an existing mock. Both are explicitly typed; that check
  and the focused tests pass.
- The initial baseline suite inherited the default OS temporary directory for
  its disposable fixtures. Later suite and PostgreSQL fixture runs explicitly
  use a worktree-local `TMPDIR`; no live configuration or data was edited.
- A few read-only evidence inspections used the wrong working-directory-relative
  paths, and the first staged whitespace check found an extra blank line in a
  captured plan. The paths were corrected and the final artifact whitespace was
  fixed without changing any measurements.

PROOF: SQL p50 6893.302 ms -> 203.435 ms; HTTP p50 8775.144 ms -> 548.563 ms on anonymous production scope; 557 unit passes/3 pre-existing skips, 3 focused HTTP passes, 5 PostgreSQL passes, typechecks/build/71+11-route contract PASS; implementation SHA eef1244eca884b4f03c12775c2e2e9fcb47895a8; PR https://github.com/gadgethd/ukmesh/pull/142.
