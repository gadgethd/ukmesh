# Database lookup qualification

Lookup changes must preserve the existing response, identity and privacy rules.
The latency targets are 100 ms p95 on the retained sample and 250 ms p95 at
10× volume for bounded interactive responses, with eight active HTTP requests.
Large aggregates and exports retain their existing deadlines and have a 10%
regression ceiling. SQL execution plans are diagnostics, not HTTP percentiles.

## Read paths

Most nodes have one stored identity row. The public node projection reads those
rows directly and uses the complete canonical ranking and aggregation for
aliases and normalization collisions. Sparse candidate sets use indexed point
lookups; broad sets use the bulk projection. Candidate selection never replaces
the final scope, visibility or literal substring predicates.

The SQL decoder uses indexed receiver and prefix candidates. It still counts
all canonical matches, rejects ambiguous/repeated hops and applies the existing
receiver trimming rule. The original canonical views and public compatibility
views remain available. Changes to their columns or representative rules must
also update the lookup projection; native parity tests compare every field.

Observer filtering first discovers sources, then requires an eligible public
observation for each source. Repeated receptions do not require repeated
privacy proofs. Private-only sources remain excluded. Connection pools,
timeouts, caches, privacy-generation fences and public limits are unchanged.

Normalized identity and observer/source indexes support equality reads. Literal
search candidates use escaped `LIKE` with a shared trigram index, then the original
substring check. Map pages apply their limit to each disjoint identity branch
after filtering. The normalized index also provides map order; statistics on effective presence
avoid sorting every visible node before returning the first page. Normalization
collisions are found through legacy keys; a collision must include at least one
key whose stored and normalized forms differ. The final index set avoids
duplicate included IDs and separate search/map trees to control WAL per packet.

`meshcore_db_pool_wait_seconds` separates connection acquisition from
`meshcore_db_execution_seconds`. Operation labels use a fixed allowlist and
never contain SQL, bind values, node identities or actor identifiers. Existing
total-duration metrics remain available.

## Isolated fixtures

Run from a source checkout with Node/npm and Docker. The normal DEV services
use ports 5433/6380/9002/3100; lookup qualification uses a separate database on
5434, Redis on 6381, MQTT on 9003 and backend on 3101. Commands do not read `.env`.
Docker must be available to the invoking shell.

```bash
scripts/benchmark-lookups-local.sh prepare
scripts/benchmark-lookups-local.sh measure baseline before
scripts/benchmark-lookups-local.sh measure candidate after
```

Preparation refuses an existing lookup database container. It archives the
baseline backend at `5c780e6`, uses the pinned Timescale image with 2 CPU / 2 GiB,
loads a shifted/redacted copy of the retained local archive, and creates a
separate 10× clone. The source CSVs remain untouched. The 1× fixture has 6,176
nodes and 157,751 observations; 10× has 61,760 and 1,577,510. Extra identities
and text are invented. Six shards exercise recent windows and four add older
history. Observation counts and distinct packet counts remain separate.

Bulk fixture loading keeps node privacy triggers active. Packet bulk loading
uses the existing sample-loader approach and invalidates certification until
normal convergence finishes. Statistics on newly created path tables and
refreshed identities are analysed before backfills. Scheduled fixture
lifecycle jobs stay disabled; this is not a production policy change.

The benchmark uses the real public node and packet handlers, real PostgreSQL
queries, configured pool sizes and production rate limits. It rotates real
loopback client addresses while bounding active requests at one or eight.
Fixture preparation completes before timing. The API benchmark no longer
rewrites recent timestamps during measurement.

Enabled regions need a separate run with
`PUBLIC_FEATURE_REGION_SCOPING_ENABLED=true` for both revisions. An optional
large published-member fixture can be created with:

```bash
DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_10x \
  npm --prefix backend run sample:lookup-scope -- --owned-fixture
```

That invented scope stresses member reads independently of RF classification;
it is not evidence about classifier accuracy. The ordinary fixture also runs
the actual region rebuild. Regional search is checked against the canonical
view because the previous regional projection omitted history source IDs.

## Correctness and sustained load

The native `nodeLookup.integration.test.ts` compares complete projections with
the existing canonical view and decoder. Cases include absent canonical rows,
alias chains, mixed case, whitespace, Unicode normalization expansion,
ambiguous prefixes, literal searches, private-only receptions, repeated
observations, pagination and network/observer scopes. Retained-fixture parity
also runs old and new repositories in one read-only repeatable-read transaction:

```bash
DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_10x \
  npm --prefix backend run bench:lookup-parity -- \
  --baseline-source-root "$PWD/plans/database-lookups/baseline-source/backend/src" \
  --output "$PWD/plans/database-lookups/parity-10x"
```

Ingest comparisons require fresh, independent databases and a restarted owned
backend/broker/Redis between runs. Keep the same invented source, resource
limits and settings. Use explicit lookup endpoints when calling `bench:ingest`:
`--broker ws://127.0.0.1:9003 --metrics-url http://127.0.0.1:9093/metrics`.
Its defaults belong to the normal sample stack and must not be used here.

The final mixed soak publishes 72,000 distinct invented ACKs at 30/second for
40 minutes. Eight HTTP slots include map/search, observer/history, packet/feed
and periodic background charts. Qualification checks persistence and queue drain,
packet/path/fact/ledger counts, live delivery, cache bounds, memory and latency.
Generate a fresh synthetic directory; `--sequence-start` selects a disjoint ACK
range without changing normal generator behaviour. Run a short smoke before
the full qualification.

```bash
scripts/lookup-stack-local.sh services
npm --prefix backend run build
docker exec ukmesh-lookup-db createdb -U meshcore -T lookup_10x lookup_soak
npm --prefix backend run sample:generate -- \
  --data-dir "$PWD/plans/database-lookups/soak-source" \
  --count 72000 --sequence-start 1100000
DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_soak \
  npm --prefix backend run sample:load -- \
  --data-dir "$PWD/plans/database-lookups/soak-source" --omit-packets --fast
DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_soak \
  npm --prefix backend run sample:prepare
scripts/lookup-stack-local.sh start lookup_soak candidate
DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_soak \
  npm --prefix backend run bench:lookup-soak -- \
  --data-dir "$PWD/plans/database-lookups/soak-source" \
  --output "$PWD/plans/database-lookups/soak-final"
scripts/lookup-stack-local.sh stop
```

Build the selected backend first, provision the separate lookup Redis/MQTT and
owner-auth database, and prepare synthetic nodes before starting the soak.
The clone command refuses an existing database. The generator also refuses
existing files; use a new directory and disjoint sequence for another replay.
The stack launcher uses only owned ports and PID markers. Stopping it retains
fixture data and leaves the normal DEV stack running.

## Evidence and rollout

Raw plans, hashes, timings, failures, resource settings and soak results stay
under ignored `plans/database-lookups/`. Do not commit private archive rows,
names, messages, topics or screenshots. HTTP data hashes exclude only the
request snapshot echo; online-state changes between different snapshots remain
real differences and require the native same-snapshot parity check.

Apply additive migrations before starting the new backend. DEV index builds
are transactional and may briefly block writers; failures roll back atomically.
Production online rollout and F2 remain deferred. Do not enable retention,
shorten history, expand stale windows, increase pools or drop response fields to
meet a performance target.
