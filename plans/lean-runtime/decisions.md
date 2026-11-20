# Lean runtime — decisions and evidence

Approved baseline: 2b4613a81a439cdbe8b1dae8352f2c9ee063a857.
User approved the full-stack lean-default plan on 2026-11-14, including fixture
switching, local external-service sinks, built frontend assets and continuous
untangle reviews. All services must fit this VM; no production deployment.

## Runtime policy has one owner

- Hidden link: production, fixture and companion configurations repeat resource
  and database-pool defaults without demonstrating their combined footprint.
- Needless link: dev-all currently couples companion build, runtime state,
  environment cleaning and launch sequencing in one shell script.
- Surplus: permanent Vite processes consume memory when nobody is editing.
- Fix: shared main-runtime defaults, explicit local wiring, built web assets,
  and domain-owned launcher/qualification helpers. Preserve hot reload opt-in.

## Full-stack qualification has an explicit lifecycle

- Hidden link: an ACK-only UKMesh soak does not prove companion commits or work
  by classification, health, RF, link, privacy and operational services.
- Hidden link: current privileged fixture credentials hide role-grant failures.
- Special cases: existing benchmark preparation refuses retained containers;
  lifecycle and destructive recovery must never touch original data.
- Fix: owned fixture manifest, verified backups, role-specific probes, native
  service work oracles and measured resource/recovery evidence. Restore original
  targets afterwards. Existing latency, durability and privacy limits stand.

## Stage gates

1. Inventory/backups; resource and pool policy; independent review.
2. Complete local wiring and static frontends; independent review and work smoke.
3. Fixture/access and qualification tooling; independent review and short smoke.
4. Matched trials, sustained soak, recovery; restore and final complete startup.

Each implementation batch must close structural findings before advancing.
Comments/tests cleanup is already authorized; regression checks precede any
replacement of implementation-coupled tests. Record failures without waivers.

## Published configuration and assets identify their consumers

- Hidden link: replacing a file or directory atomically does not update an
  existing container bind mount; successful builds could leave old assets live.
- Needless link: restarting CoreScope previously rebuilt unrelated Beacon code.
- Fix: each companion component owns its source/build revision; web and generated
  configuration revisions are part of their consumer's Compose definition.
  Unchanged generated files retain their inode. Lifecycle helpers orchestrate
  domain builds; they do not infer application schema or privacy rules.
- Surplus: a handwritten inventory could silently omit a newly enabled service.
  Validate its coverage against all four effective Compose service sets.

## Fixture selection also owns cluster-wide login access

- Hidden link: database roles are global, so grants from an earlier selected
  fixture survive a target change unless its CONNECT is withdrawn.
- Fix: validate the dedicated local cluster's complete store inventory and exact
  ownership markers, revoke previous connection rights, then grant the selected
  measurement/authentication pair. Apply the restricted Core reader projection
  in the same fixture bootstrap so new 1x/10x stores need no manual setup.
- Application-specific Beacon relations and maintenance ownership are defined
  inside beacon-server/deploy/ukmesh-dev; root helpers only orchestrate them.

## Publication and restart boundaries

A published artifact owns its revision. The previous static launcher kept its
revision beside an independently renamed directory, so interrupted publication
could pair a new site with an old revision or leave the active path missing.
The companion artifact helper now moves the complete site and its revision
together, restores the previous site after failed/interrupted publication, and
removes partial staging before another build. Behavior tests inject rename and
build failures and inspect the recovered site.

Each component owns source and configuration revisions separately. Targeted
restarts load the other published revisions without reading their repositories,
use one explicit recreation, and preserve the two-process CoreScope build as a
single versioned pair. Configuration-only edits update consumer revisions while
leaving binaries cached. Compiler image/flags and frontend build variables are
included in the owning component's build fingerprint; runtime-only changes and
dependency caches are excluded. Fingerprint errors stop before Docker mutations.

Finite jobs own their completion result. Both Beacon migration restart and
UKMesh link-backfill startup inspect the actual container state after Compose
wait and require exited:0. Failed and still-running jobs cannot report completed
startup. Main image rebuilding remains the explicit dev-all build operation.

Beacon owns its local-development database grants and three derived-relation
maintenance owners in deploy/ukmesh-dev/access.sql within the Beacon repository.
The root helper owns only local container/database guards and invocation. This
keeps application relations out of UKMesh lifecycle orchestration.

The full-stack interface pass found a hidden executor link: analytics chart
backfill uses the worker pool, while spam's run/lease lifecycle uses the HTTP
pool. Their grants now follow those owning executors, with real transactional
behavior coverage and retained denials. This fixes the capability boundary
rather than disabling either background activity.

Required migration evidence must come from the job that actually migrated the
selected store. Main and Beacon controllers now execute named migration jobs,
retain their output and require exited:0 before role provisioning. This avoids
an ephemeral bootstrap followed by a separately tracked dependency job. Normal
Beacon resident targets exclude its already-completed migration job explicitly.

## Broker-owned reload transport replaces shared PID lifetime

The recovery probe exposed a hidden lifecycle link: the reload HTTP service
joined the broker's PID namespace to signal PID1. Restarting the broker killed
that namespace; Docker's independent service restart raced the broker and left
the reloader exited137. Restarting both services in a special recovery sequence
would hide the dependency and weaken the bare-broker restart test.

The local broker now owns a private FIFO and a small lifecycle wrapper that
signals only its own broker child, forwards TERM/INT, reaps its child processes
and preserves the broker exit code. The independent reload HTTP service writes
only the constant reload command through a strictly owned, nonsymlink FIFO with
nonblocking opens/writes. Its authentication, serialized requests and new-log
acknowledgement requirement remain in place. Other integrations retain their
existing PID1 signal transport unless the FIFO setting is explicitly supplied.

The existing local initialization job prepares a1883-owned control volume;
there is no Docker socket, privileged capability, watchdog or additional service.
Native process/HTTP tests cover exact exit status, child reaping, malformed
control input, unavailable/full FIFO, and the same reloader returning204 only
after log acknowledgement before and after broker-child restart. A guarded
real-Mosquitto lifecycle probe is provided for root's reviewed rollout; it has
not been executed during source implementation.

## Recovery faults belong to the synthetic operation that owns them

The 04ba recovery run passed fourteen checks but left the alert receiver unhealthy:
a real MeshIngestSilent alert exhausted its five retries during the mock's global
503 window. The recovery driver enabled that window before Alertmanager's group
wait, and an unrelated pending item could satisfy its unscoped queue oracle.

Scope provider faults to the tag route or one exact owned alert marker. Observe
that marker's actual failed forwarding before restarting the receiver, then require
its successful delivery, a drained queue and zero dead letters. Keep the real
Alertmanager, durable receiver queue, retry limits and readiness contract. Retain
the failed receipt and dead letter; any redrive is a separate reviewed operation.

The reviewed receiver redrive retains original dead letters and appends a durable,
hash-linked successful resolution. Qualification therefore requires zero unresolved
dead letters, while keeping raw dead-letter and resolved-history counts stable
through a new owned fault. Fall back to the old raw counter only when the new
unresolved field is absent; malformed or inconsistent counts fail closed. A raw
historical record is evidence, not an unresolved delivery failure after its actual
successful redrive has been verified by the owning queue.

## Clock-sensitive operation comparisons own matched trials

A frozen database does not freeze time. Read-only preparation found390 retained
observations crossing the summary's24-hour window in three minutes, while chart
responses recompute region-health freshness from the response clock. Statistics
handler, repository, service and relevant policy sources are byte-identical in
the sealed baseline and candidate. Requiring one hash across six sequential
responses rejects valid clock behavior.

Run the two source-selected processes as paired trials on the same owned fixture.
One controller releases each route after both are ready, outside HTTP timing;
retain actual UTC bounds and require start skew at most100ms. Compare exact full
canonical payloads within each corresponding trial, including chart timestamp,
generation, health and summary values. Keep all failed attempts; a pair separated
by a real threshold fails. Share one companion-reader group and one report writer.
No clock overrides, query rewrites, retained timestamp edits or blind normalization.
Require the same eligible persisted chart with at least five minutes remaining
before the first release, then enforce normal freshness on every response.

## Service work proof follows its own delivered alert

The Alertmanager work oracle still consulted a single last-alert marker. A later
unrelated forwarding success could overwrite that marker after the owned alert
had actually arrived. Reuse the provider's bounded successful-probe history,
which is already written only after successful responses, and keep the original
25-second deadline after each awaited HTTP read. A different last marker is
neither success nor failure for this operation; its exact fresh UUID must appear
in the successful history before the deadline. This closes the same hidden
cross-alert link in the ordinary work check. No forwarding or retry policy changes.

The first paired diagnostic preserved all twelve exact payload pairs, but one
scheduled Node timer resumed one millisecond before its wall-clock release.
Recheck the release condition in a loop, retaining the same monotonic barrier
deadline after every await and capping sleep by its remaining budget. A deterministic
early-wakeup test owns this scheduler constraint. The separate21.89% summary
median regression remains unresolved and is not excused by the timer correction.

## Three operation trials retain enough raw samples to measure cached reads

The first real pair retained cold summary times223.359/231.261ms and only two
cached times per source:3.453/3.066ms versus4.209/4.016ms. Its21.89% median
regression is retained. Read-path/runtime/configuration source closure is identical;
those three observations do not identify SQL, pool or transport cost.

Pre-register three trials with twenty summary and twenty chart responses each,
plus one CSV and one GeoJSON:126 raw responses per source. Pair every sample,
retain the first cold response and all failures, and compare the median of each
trial then the median of three with the same10% gate. Eight declared real
loopback actors execute serially (C1), preserving the twelve-chart-per-minute
client limit and thirteen-second export floor. A single shared policy owns counts.
Read existing selected-source database histograms outside HTTP timing, record
allowlisted pool settings and CPU/event-loop/transport evidence. Do not reset
metrics, create another runtime, change application instrumentation or rewrite SQL.

## Candidate cardinality, rather than regional scope, owns sparse projection

The strict1x matrix retained two warm-C8 failures and a Core companion tail
regression. Outside-timing plans identify a concrete regional cost: a50-row page
scans5,451 node keys and filters5,402 rows (30.724ms SQL), because regional
candidate plans force an ordered bulk projection. The scope label hides that its
candidate set is sparse; existing general projection already has a128-candidate
adaptive boundary and preserves complete aliases before privacy/filtering.

Reuse that adaptive projection for ordered candidate plans while retaining the
already-indexed observer strategy. Do not add regional caps, discard aliases,
change scope/privacy/cache rules or alter pagination. The guarded READ ONLY
cardinality peek found1x regions≤54 of5,451 nodes;10x has a21,020-member dense
qualification region among61,760 nodes and ordinary regions≤48. Thus the existing
128 authority handles both observed regimes; no new tuning threshold is needed.

Extend native coverage through the actual repository at sparse/boundary/dense
sizes, retaining aliases, private rows, scopes and pagination against the canonical
view and sealed baseline where available. Record before/after plans and row work
outside timed trials using the actual API login. The unrelated short-search SQL
remains unchanged; CFS/whole-host contention is a separate controlled diagnosis.

Native verification passed168 complete-field/network/cursor/page cases at0,1,127,128,129,500 and all eligible IDs, with the sealed2b repository and an independent canonical-view oracle. Explicit positive controls prove the public alias family without a raw canonical row is returned; a private representative outside region membership is excluded. Two invalid synthetic memberships (lowercase then legacy Unicode) were rejected by the real region constraint before correcting fixture inputs; both failed receipts and successful cleanup remain retained. No schema guard was changed.

The actual restricted meshcore_api TCP session ran READ ONLY against the selected owned1x fixture: all50 returned rows/all12 fields match sealed2b exactly for54 members. Outside-timing EXPLAIN reports5453→54 node-row work and8.331→2.733ms executor time. This establishes the structural cause and privilege compatibility, not a fresh HTTP qualification. TypeScript and diff checks pass; runtime image, targets and resource settings remain unchanged pending independent review and root rollout.

The source-frozen regional-only diagnostic (bc69ed0; runtime image b2/ad8 unchanged) exercised the entire failed C8/warm mode:12 cases×3trials×200 responses, unchanged100ms gate and one companion group. All36 results passed; maximum93.346ms. Search-short stayed unmodified and returned68.820/81.850/74.019ms p95; map-region returned66.507/62.946/69.286ms. This is diagnostic evidence before rollout, not a replacement for full matched qualification. Both companions had72 reads/zero failures; their one-heavy-mode p95 values are retained without treating the previous four-mode aggregate as an equivalent comparison.

The actual overlapping read-only CPU sampler recorded PG1.807 mean cores with210 throttles/+20.178s throttled time across the36.159s enclosing interval while host idle was22.62%; PID and effective2CPU/100ms quota stayed unchanged. No CPU update/recreation occurred. Optional period A/B helpers remain prepared-only, pending parent decision and final lifecycle review; no setting or acceptance gate was changed in response to the passing diagnostic.

## Companion observations stay with their collector

CompanionReads times real HTTP/body/JSON work but discarded the raw observations.
The matrix loop knows each selected source and mode, yet the resulting aggregate
could not explain a tail regression against different background-job windows.
Keep observation identity, clocks, lifecycle and raw retention in CompanionReads;
keep setup/mode/between/teardown transitions in its owning reads loop; verify
raw-file hashes and recompute the all-sample aggregate in the existing read
validator. This adds no application dependency or regional exception. Preserve
two independent actors per application at1Hz, all failures and crossing samples,
the original latency boundary and the same10% comparison. Separate old/current
collectors and raw files remain mandatory; never reuse one stream for both labels.

The shared collector also needlessly depended on benchmarks.py, pulling fixture,
source-identity and inventory ownership into operations/soak read loops. Move
collector, raw-evidence validation and percentile calculation together into
companion_reads.py. Callers pass the existing bounded HTTP function explicitly
and own scope transitions; generic collection imports no fixtures or application
inventory. The matched-read comparison remains responsible for applying its
unchanged budget and regression gates to the audited observations.

## CPU propagation owns equivalence of resolved RF work

Keep the complete selected identity/site list as the publication and checkpoint
input. Within the CPU margin calculation, evaluate each finite, bit-identical
resolved Site only once, preserving the first occurrence and all four values
(latitude, longitude, ground elevation and transmitter height). Raster coverage
is the maximum of independent path margins with shared propagation parameters;
an identical path repeated by another identity cannot change that maximum.

- Hidden link: identity cardinality currently determines repeated terrain-path
  work even when every physical RF input is identical. The owned10x preview has
  38,388 selected identities at3,671 coordinates. Equality must be established
  after tier-specific ground heights are resolved, rather than from coordinates.
- Needless link: reducing duplicate CPU work through selection, GeoJSON, tile
  signatures or SpatialIndex.Query would alter a separate identity/publication
  contract. Keep that index's every-candidate contract and all GPU/remote inputs.
- Surplus: evaluating the same path repeatedly changes neither best margin nor
  coverage, but consumes CPU throughout every raster tile.
- Necessary special case: nonfinite values remain separate; signed zeros and
  any differing IEEE-754 field bits remain distinct. Do not round positions,
  approximate terrain, sample inputs, cap transmitters or change RF parameters.

Capture pre-change optimized full float32 raster and actual PNG hashes before
the production call is changed. Require exact parity afterwards, retain existing
upstream1e-5dB gates, and test differing heights, input ownership, progress, empty
sets and bit-edge values. Benchmark the complete retained duplicate-coordinate
distribution against the pre-change implementation using controlled terrain;
this is a CPU benchmark, not actual fixture coverage or an invented RF deadline.
No product computation, target switch or cache mutation is part of this batch.

Native before/after evidence retains six complete float32/PNG files, all byte
identical across the committed pre-change CPU, a read-only pre-change overlay and
the final candidate. Both propagation/coverage packages pass24 top-level tests;
the pure physics package also passes on the current companion Go toolchain.
The PNG integration fixture belongs to coverage tests, avoiding a new GPU/CGo
dependency in propagation tests. The complete38,388-site controlled-terrain
benchmark's three-run median changes4.164s to0.396s; transient allocation rises
from1.629MB to3.770MB per representative raster. Existing UK4600 Standard and
Precision speed ratios remain0.262/0.259 against their unchanged0.70 limit.
This does not predict whole-region completion or replace whole-VM qualification.

## Publication ownership persists across Docker replacement

Keep the calculator's existing lock pathname and flock semantics, but make its
Docker image resolve that pathname to a root-owned lock inode in the existing
data volume. Never unlink, replace or archive that persistent inode.

- Hidden link: container startup performs an initial calculation, and an expired
  last-good cache can be overwritten before qualification has archived it.
- Hidden link: a lease on the old container's private/tmp inode cannot protect
  a replacement container, even when both use the same pathname.
- Fix: an image-owned/tmp/hopreach.lock symlink to/data/.hopreach.lock. Bridge
  the old private lease through old-container stop while a separate bounded
  volume carrier already owns the persistent lease. Startup must demonstrate
  the existing successful busy skip before any RF write; protected cache bytes
  remain unchanged. Source, config, model, cron and freshness gates stay intact.

The temporary carrier is a one-shot orchestration owner, not a resident service.
Its1200-second lease spans the existing900-second canonical startup allowance;
the owned startup child inherits its stdin lease and survives controller-group
interruption. Expired/foreign leases fail closed before dispatch; failure stops
the owned new RF resident before protection is released. Verify root ownership,
regular nonsymlink target, device+inode equality, private/tmp compatibility, exact
busy0, byte stability, replacement survival and EOF/timeout cleanup on an owned
isolated volume before any guarded live rollout. No production-store or RF job
is authorized by this source change alone.

## Plain loopback transport owns local qualification requests

The Beacon10x failure exposed an observation cost: the probe rejects HTTPS URLs,
yet urllib's default opener constructs an HTTPS handler and TLS context for every
plain HTTP read. The isolated no-network profile measured four-actor opener
p95 at66.27ms versus0.072ms for a fresh HTTP-only opener. This needless TLS/proxy/
FTP dependency consumes VM CPU and obscures the measured server behavior.

Move local URL validation, redirect restrictions and fresh plain-HTTP requests
into loopback_http.py. Keep timing before construction and through network/body/
JSON work, fresh connections, four1Hz actors,15s timeout and8MiB GET limit. The
Health Check POST uses the same owner while retaining its64KiB response limit.
No shared opener, SSL context cache, app/resource change or new matrix method.
Keep every failed429 sample and repeat both scales under the same100/250ms and
10% gates after independent behavioral review.

## Native lookup phases own their observation boundaries

The companion collector preserves every observation and its four caller-owned
mode boundaries, but the native lookup process owns the actual trial, case,
warmup and measured-request intervals. Inferring those intervals from an outer
mode duration or splitting it into thirds would invent missing evidence. This
hidden lifecycle link prevents causal comparison of a companion tail with the
actual request mix and background-service work.

Record bounded in-memory UTC and monotonic start/finish events inside the native
benchmark at those owning boundaries. Flush a separate timeline after cleanup,
including partial failed spans, without per-request events or I/O. Preserve the
existing HTTP timing positions, queries, case order, three trials,20/200 samples,
C1/C8 slots, cache behavior, response verifier, worker budgets and all-sample
100/250ms and10% gates. The caller may join real timestamps to retained raw
companion observations; it must not infer uncollected intervals, trim samples or
rewrite the failed02685ec aggregate. A prospective same-source A/A diagnostic
belongs in a unique run and requires its own reviewed, frozen execution window.

## Clock captures own their uncertainty

The first same-source control stopped invalid before its second label: the
native origin recorded UTC before its first monotonic reading, leaving an
unmeasured initial gap. The first boundary already differed by1.896ms and later
UTC-minus-monotonic differences occupied a0.991ms range. This does not establish
a system clock change. A fixed2ms join guard assumed a capture cost the tool had
never measured; the failed control and original performance failure stay intact.

Bracket every actual UTC read, including origin and final capture, with immediate
monotonic readings. Preserve both endpoints and the unmodified UTC value. Native
Date readings have1ms quantization: for a UTC millisecond W and monotonic bracket
[B,A], a consistent UTC-minus-monotonic offset lies in[W-A,W+1-B). A causal join
requires a nonempty intersection across all captures, ordered monotonic brackets
and nondecreasing actual UTC. Retain bracket uncertainty and crossing intervals;
never shift timestamps, raise a tolerance, infer missing boundaries or omit
samples. HTTP request clocks and every workload/acceptance gate remain unchanged.
A new prospective protocol records this sampling correction before one fresh
same-source control; it cannot retroactively validate the first attempt.

## The read protocol owns matched companion repetitions

Three native trials previously shared one companion observation window per
source. That hidden link made native trial count appear to supply three matched
companion repetitions. The valid same-source control retained312/328 Core reads
but produced7.109/4.891ms p95 without a source change; it neither explains away
nor passes the original regression. Short native trials had only6–10 or26–30
Core observations, so dividing those rows would make p95 too close to a maximum.
Fixed1Hz sampling can also alias periodic work. Neither trimming startup nor
selecting a passing interval repairs these structural problems.

Give one read-protocol profile ownership of three complete matched matrix pairs,
orderedAB/BA/AB prospectively. Each fresh window reuses the existing workload and
lifecycle owner: four modes, three native trials,20/200 samples andC1/C8 slots.
The paired read profile alone uses0.25s per existing actor; operations and soak
retain their1s default. This stays within Beacon's enabled600/minute limit with
120/minute headroom and preserves fresh HTTP construction/body/JSON clocks.

Keep collector/raw ownership separate from protocol scheduling and comparison.
Retain every observation and independent collector/window/pair identity. The
primary companion statistic pools all raw rows by source across all three
windows, with the unchanged nearest-rankp95 and10% gate; pair/mode/trial results
are descriptive. Every current-source native value must meet100/250ms, now nine
values per case/mode. The sealed baseline remains an unbounded reference.
Normal baseline authority never accepts candidate aliases. Same-source aliases
are explicitly diagnostic, use candidate authority six times, and cannot enter
a qualification report. Missing/duplicate pairs, wrong order/cadence, failures
or source/runtime drift fail closed. One fresh same-source three-pair control
with a fixed24-minute collection budget precedes any separately authorized
baseline/current runs; no retries, cadence tuning, weighting or idle padding.

## Permission probes must reach the permission check

The real 10x reader session has no table or column INSERT privilege on the
public packet view or its private source table. The whole-row INSERT probe
nevertheless returns 0A000 because it targets the view's computed payload
column before PostgreSQL checks INSERT permission. The view itself is
insertable; treating it as wholly nonupdatable or accepting any 0A000 would
hide the cause. Preserve the failed 62/63 result and all grants.

Use the existing native reader test's rollback-only INSERT DEFAULT VALUES,
still requiring exactly 42501. Independently require absent effective table
and any-column INSERT privileges in the actual reader session, bound to its
role, database and relation identity. The qualification owner owns this probe;
the application repositories retain ownership of database and privacy rules.

## Reuse deterministic response-validation evidence

The benchmark validates identical response bytes repeatedly: all 7,200 responses
in the current diagnostic and 95,040 inspected V3 responses have one raw digest
per trial and case. Parsing and canonicalizing each copy couples local benchmark
CPU and allocation work to API throughput. The validator is pure in the exact
ArrayBuffer and status, expectedStatus and populated fields. Offline paired
worker trials preserved every output and reduced median process CPU by 54%.
Their higher whole-process RSS remains evidence, not an ignored result.

The validation worker owns a bounded cache of at most eight successful derived
results. Check bounds and status and hash all bytes for every response. Key on
that raw digest, byte length and the complete validation contract; never key on
the normalized digest. Retain no response buffer, string or parsed object, copy
returned evidence, and never cache errors. Preserve worker, queue, timeout,
sampling and request-timing limits. Expose cache counters honestly: repeated
parsing is avoided, while every response is checked for byte identity.

These changes require independent review and fresh qualification. They do not
clear any previous performance, permission-oracle or diagnostic failure.

## Request slots own their sockets and settle before teardown

The incomplete e8 10x comparison failed in the fourth, baseline window with
ECONNRESET during regional reads. No request-level socket receipt exists, so
that connection's exact cause is unproved. The installed Node26 Agent source
nevertheless exposes a concrete lifetime defect: a custom keep-alive agent
with no positive timeout ignores the server's shorter Keep-Alive hint because
its default zero timeout wins. The 128 rotating clients can retain free sockets
past the loopback server's advertised lifetime during slow baseline cases.
The live 43-service stack passed its closing guard; this is not evidence that
an application service restarted or that any performance gate passed.

The request fleet owns a finite free-socket lifetime that honors server hints,
while each active request retains its existing 30-second timeout. Separately,
Promise.all currently lets one failed request unwind the trial and close its
server and database while the other request loops are still active. The batch
owner must stop admitting work on the first error, await every active slot,
and then rethrow that first error before its existing cleanup. No retry,
sample selection, timer/count change or failure-to-success conversion is
allowed. Add equivalent real local HTTP and concurrent-failure coverage before
changing these ownership boundaries. Preserve the failed execution unchanged;
new matched measurements must use the same corrected client for both sources.

## Export repetitions and pacing belong to the measured client

The actual 1x CSV comparison has one observation in each of three trials:
baseline90.42/79.81/82.06ms and current126.81/102.12/69.09ms. Its median
fails the unchanged10% gate. The24 normalized CSV application dependencies are
byte-identical; this does not invalidate the failed observation or prove its
cause. Three singleton samples provide little protection against environmental
variation, unlike the20 samples already used for statistics in each trial.

The existing eight serial clients each have their own source IP and deployed
five-per-minute export bucket. A global13-second delay needlessly couples
independent clients. Use20 export samples per trial, retaining three trials,
the same median rule and every raw result. The next actual source IP owns its
13-second cooldown, shared by CSV and GeoJSON across all trials. Keep one HTTP
request active, eight IPs,30-second deadlines, exact paired clocks/data and
chart-freshness guards. Validate every export identity, not only the first.

Independent review approved this prospective measurement repair before edits.
It requires focused rate/lifecycle/data tests and fresh complete comparisons;
no application code, production limit, threshold or old evidence changes.

## Capacity and equal-demand comparisons answer different questions

The same eight closed-loop request slots generate1.52-1.60 times the request
rate in the faster current implementation. That is a valid capacity experiment:
its saturated companion timing increases remain failed observations. Coupling
those timings to a regression gate for matched external demand hides that the
neighbours receive different background work. Process isolation did not help
in the one paired diagnostic and will not be adopted.

Keep every saturated C1/C8, warm/miss trial and its100/250ms current-source gate.
Keep the saturated companion results and actual throughput in the report. Add
three prospective AB/BA/AB comparisons with identical offered demand for the
companion10% regression question. Both independent reviewers agree this fits
the user's matched-baseline requirement, provided it is never described as
companion nonregression at eight continuously active requests.

Freeze80 requests/second for120 seconds:9600 planned requests per cohort, the
same cyclic12-case mix,128 existing real clients, at most eight active slots,
warm caches and unchanged validation/30-second request deadlines. The rate is
about80% of the measured baseline's95-100 requests/second, chosen before this
new experiment. Retain every planned arrival, actual admission, response,
queue delay, failure and undelivered request. Admission lateness must not exceed
100ms, the existing paired-clock allowance; actual dispatches are at least10ms
apart and the complete dispatch span stays within100ms of its fixed schedule.
No batched catch-up, adaptive rate, dropped samples or passing retry is allowed.
An unsustained schedule is invalid, not matched80RPS.

Use the existing clock/response/collector owners. Compare every companion read
whose start belongs to the predeclared120-second window, including boundary
crossings; retain setup and drain reads with all failures visible. Native
saturation and fixed-demand evidence have separate owners and collection
budgets. Both are required for clean new qualification. All old experiments,
failures and source revisions remain immutable; fresh complete measurements
under the new committed method must precede any result claim.


## Useful-work probes must fit the capability they observe

The 1ba2ab7 preflight retained two actual failures. The app-ukmesh work probe
requested the legacy unbounded `/api/nodes?network=ukmesh` endpoint; an actual
header-only response confirmed200 JSON and12,578,628 bytes on the owned10x
fixture. The8MiB guard correctly rejected that response. The manifest coupled
frontend useful-work proof to a full dataset transfer that grows with scale.
Use the existing bounded `/api/nodes/map?network=ukmesh&limit=20` endpoint and
its `nodes` array for this work proof, retaining the8MiB guard and minimum one
node. No application capability, production endpoint or response cap changes.

The separate link-worker oracle returned an actual zero commits and remains
a required failure until an eligible owned fixture job traverses the existing
queue and the real worker commits it. Preserve path uniqueness, ambiguity and
role checks; do not insert the ledger or substitute idle health for useful work.
Record that input/admission/worker commit independently before repeating the
full work proof under the final source context. Root authorized this narrow
manifest repair and native owned-job preparation before these edits.

## Soak evidence counts the owned workload rather than unrelated history

The first full72,000 attempt stopped before WebSocket setup, HTTP warmup or
publication. Its baseline SELECT combines four full-table counts under the
existing30-second timeout. This couples an exact owned-workload proof to all
retained history; repeated drain polls would repeat that unnecessary work.
The retained failure sent0 ACKs and launched no RF calculation. Original
stores are restored and all43 services pass their guard; the unchanged RF
cache still matches its verified archive.

Count the exact ACK and auxiliary workload keysets using indexed relational
joins, retaining every observation/path/fact/ledger expectation. Declare the
scope and keyset digest; do not substitute estimates or silently call scoped
numbers global totals. Include duplicate hashes/multiple observations correctly
and exclude unrelated writes that could previously mask a missing owned row.
Keep the pool at1, the30-second counter deadline,120-second drain deadline,
72,000 distinct ACK/live-delivery and all other integrity/resource limits.
Validate equivalence and actual owned10x execution plans outside HTTP trials.

Repeated polls use packet-derived observation keys and explicitly exclude
independent orphan UUID rows. Whole qualification still requires a separate,
once-only final audit of all four tables filtered by the complete owned hash
set. This audit includes orphan or mismatched children and must agree with the
parent-derived totals; it is mandatory for native success. It retains the same
30-second statement/logical bound, one connection and exact row multiplicity.
The actual read-only empty72k+aux workset completed in8.429seconds for the
indexed poll and7.215seconds for the independent final audit. This does not
claim a populated or loaded execution bound; no index or DDL was added.

The failure also exposed a teardown dependency: Health/typed/ACK checks each
wait for delivery after a confirmed failure before any publisher can start.
A native phase receipt must conservatively attest that state. Cancel impossible
polls and record explicit failed/not-published snapshots; never turn those
checks into successful skips. Unknown or started-publication failures retain
normal bounded commit and recovery verification. Keep full preparation/privacy
checks and all partial evidence; no timeout increase or passing retry.


## Live ACK evidence uses its actual observer subscription

The c5ef361 attempt completed preparation and its exact baseline counter, but
failed before publication: the network-wide WebSocket snapshot contained34,179
nodes and20,000 links and exceeded the existing4MiB guard. The retained server
branch implies1013; the old helper did not capture that wire code. Zero ACKs
or auxiliary events were sent, no warmup completed, and RF never launched.
The new confirmed-prepublication cleanup correctly retained failed snapshots
without padding a resource window. Keep that failure and the global10x
snapshot limitation explicit; do not report global network WS readiness.

The live-delivery proof depends unnecessarily on all unrelated map history.
All72,000 generated ACK rows have network ukmesh and one valid64hex receiver.
Independent prospective review accepts the existing observer scope for that
owned workload. A single read-only real subscription received its initial
state in691ms/430bytes, then closed normally; it published no traffic.

Own observer derivation and initial-state diagnostics in the existing socket
helper. Before connecting, require every full-stack ACK row to have the same
valid receiver and ukmesh network; retain every distinct expected ACK hash.
Record the exact subscription in evidence and capture actual close code/reason
when available. Preserve standalone unscoped commands. Keep the real15second
initial-state barrier,4MiB limit, all72,000 SQL/live checks, global eight-request
HTTP workload, auxiliary persistence/Health checks and every resource gate.
Bind the socket helper in trusted failure receipts. No retry, skipped initial
state, application/configuration change or raised response cap is introduced.
Validate mismatch rejection, real socket readiness and source-bound cleanup
within this batch, then obtain independent review before another full attempt.

## RF chunk work owns only the current chunk's transmitter data

The actual 72,000-ACK run failed; its RF child exited137 with a cgroup OOM
increment on entering Precision. Its archived metadata records31,540 selected
repeaters and its log records3,936 planned chunks. The pre-pass retains a
`sitesNear` slice for every chunk; each reserves the entire input length even
when no transmitter is nearby. That alone requests about3.97GB of site backing
arrays (32bytes/site), independently of the128MB terrain budget.

- Hidden link: a terrain-only chunk budget does not bound transmitter planning
  allocations, which scale as all transmitters multiplied by all chunks.
- Needless link: computing progress weights retains the same transmitter copies
  until the entire publication tile completes, though each is consumed once.
- Fix: retain scalar work weights in the chunk plan; derive and release only the
  current chunk's transmitter subset when it is computed. Share the geographic
  predicate between counting and selecting so the two cannot drift.
- Preserve range padding, RF equations, output dimensions, supersampling,
  progressive publication, remote/local fallback and weighted progress. A bounded
  offline memory regression must fail against the current implementation before
  changing it; existing numerical/PNG parity remains required. Terrain padding
  and loading remain separate concerns, with no speculative sampler rewrite.

## Beacon startup owns region changes, not timestamp-only rewrites

The original-store check found one protected row mismatch after restoration:
Beacon's `public.regions.updated_at`. The verified backup reproduces the sealed
baseline row exactly, and read-only per-column comparison proves every other
region field remains equal. Startup `config.Seed` calls the region upsert, whose
unconditional conflict UPDATE rewrites that timestamp even for identical input.
This hidden lifecycle link belongs to Beacon's query/store owner, not the root
fixture controller or a retrospective protection-policy exception.

Use a null-safe changed-field predicate for config-owned region values, including
imported-region takeover. An unchanged upsert deliberately returns no row; the
existing slug reader supplies its ID without writing. Keep actual changes and
errors visible, and cover unchanged tuple/timestamp identity, each field change,
null transitions and imported takeover with native PostgreSQL behavior checks.
The strict original comparison and failure evidence remain intact. Any repair of
the single historical timestamp is a separate, guarded operation requiring exact
backup identity and equality of all other columns; it is not part of this patch.

## Database checkout owns one pool-wait clock across execution paths

The failed full-stack run recorded an average45.8ms for the persistence CTE
in one30-second window, but quiet owned10x rollback profiles measured11–29ms
after warming. The prepared statement's generic transition was14.7ms; neither
a generic-plan regression nor an intrinsic45.8ms SQL floor was reproduced.
Retained I/O shows concurrent database and Alloy reads, but does not establish
which pressure caused the delay. Preserve that failed run and uncertainty.

- Hidden link: ingest transactions call pool.connect() outside their execution
  timer and never publish the shared pool-wait metric. A missing series could
  be mistaken for zero wait when comparing SQL and processor latency.
- Needless link: acquisition timing and outcome labels would drift if copied
  independently into each execution path.
- Fix: one database checkout owner measures acquisition success/failure and
  updates the existing bounded pool metrics. General/analytics calls retain
  their existing semantics; ingest transactions use the same acquisition owner.
- Keep lease release/discard, rollback, SQL, privacy locks, transaction order,
  retries, pool sizes and deadlines with their current owners. No per-topic
  concurrency or batching change is justified before the next controlled test.
- Verify delayed acquisition and failed acquisition with behavioral tests that
  preserve the returned client/error, SQL order and release behavior. Obtain
  independent review before commit or another measured runtime.

## Owned soak counters own set membership instead of per-hash chunk traversal

The failed f55 full40 retained a populated counter read timeout; its empty-key
proof was not evidence for populated/load cost. A read-only inactive owned10x
profile of the same72,120 hashes now returns40,852 observations: the first hot
counter takes19.646s and its warm actual plan4.285s. The30s failure does not
reproduce while idle. The plan still visits every packet chunk72,120 times,
spending3.628s and2,709,279 shared-buffer hits in observation lookup alone;
`wanted` is estimated at200 rows but actually contains72,120.

- Hidden link: expressing owned-hash membership as a forced per-hash lateral
  query couples a single exact workset count to workset size multiplied by all
  packet partitions. Empty observations conceal the populated downstream work.
- Fix ownership: let the packet observation query express membership with
  `packet_hash = ANY($1::text[])`, so the existing planner/indexes can scan the
  set together. Keep every matching parent observation, distinct natural path
  keys and distinct UUID/hash/network child keys exactly as before.
- The independent final four-table hash audit uses its own hash membership on
  each table. It must still see child rows without parents; no parent-derived
  shortcut, time restriction, index/DDL, changed multiplicity or missing-row
  allowance is introduced.
- Preserve pool1, statement/query/logical30s, the shared120s drain, input scope,
  privacy, and every qualification gate. Validate old/new exact counts and
  populated EXPLAIN row work after the serialized build slot, plus real
  read-only relational fixtures for duplicates, auxiliary rows, missing/mismatched
  children, nullable observers, shared path keys and repeated observation IDs.
  This is a source-local efficiency fix, not a retrospective full40 pass.

## Effective memory and swap limits come from the cgroup

The failed full-stack soak exposed a collection gap: Docker HostConfig recorded
the intended memory-plus-swap limit, while the sampler never read the effective
`memory.max`, `memory.swap.max`, swap charges or swap events. Local infrastructure
and companion Compose definitions also omit the main stack's no-swap pairing.
Configuration intent alone cannot establish kernel enforcement or attribute the
whole-host swap growth.

Keep cgroup parsing, process accounting and resource acceptance with the existing
resource sampler. Read strict cgroup-v2 values, retain explicit unlimited values,
and check required containers' actual finite memory and zero-swap limits against
Docker intent. Record native SwapPss separately from resident PSS and mark missing
coverage explicitly. Preserve the whole-host growth/churn thresholds and the RAM
working-set calculation; swap attribution never replaces those acceptance gates.

Temporary-file and mocked-inspection tests must demonstrate an effective limit
mismatch despite correct HostConfig, missing/malformed data and cgroup path
escape. Runtime recreation, missing Compose pairs and broker storage belong to
separate reviewed batches after this collection boundary closes.

## Owned counter negative candidate and semijoin follow-up

The first direct-array candidate is retained as negative evidence. It preserves
all counts but plans in1.797s and executes in9.676s versus the old4.7ms planning
and3.981s warm execution: it estimates1,470,830 parent rows instead of the actual
40,852 and broadens child reads. Do not accept or commit that slower candidate.
Restore baseline production SQL while testing a raw materialized workset with
an EXISTS semijoin, which preserves duplicate-input membership without array
constant selectivity or forced per-hash partition traversal. Retain the same
pool/30s/120s and independent orphan audit; accept only with actual populated
counts/row-work evidence. No temporary/public schema or planner-setting change.

## Every local consumer inherits the same memory/swap ceiling

The first effective cgroup capture identified21 mismatches:11 historical main
monitor containers retained unlimited kernel limits despite the current Compose
intent, and10 running local/companion consumers lacked paired swap settings.
Give each formerly unpaired service one YAML memory anchor used for both its RAM
and total RAM-plus-swap limit. Preserve all existing RAM and CPU values, including
finite build/init jobs. Configuration checks cover every resolved service.

Alloy keeps its128MiB hard cap, journal inputs, labels, cursor storage and Loki
forwarding. An overrideable96MiB Go soft runtime budget leaves room for native
journal overhead; it is not an RSS guarantee. Its GC, memory and actual delivery
must be characterized after Root's coordinated recreation. Explicitly recreate
the11 unchanged monitor consumers too: Compose will not infer that their actual
old cgroups differ. Kernel memory.max/swap.max assertions close the rollout.

## Owned counter final bounded batch prototype

The raw-workset EXISTS trial also retains exact counts without improving the
parent counter:14.352s first read and7.107s warm execution versus the retained
3.981s baseline warm plan. Its planner still estimates809,076 observations for
40,852 actual rows and broadens child work. Preserve that negative trial too.

Root prospectively authorizes one final fixed1024-hash array-batch prototype:
DISTINCT the complete input, deterministically order and partition every hash,
and use an indexed lateral ANY lookup for each batch in the same statement.
This separates index-vector width from total workset size without a row cap,
skipped hashes or per-call deadline changes. Existing parent/child identity
scope and the independent four-table orphan audit remain identical; keep the
old audit SQL because no audit rewrite has a demonstrated margin win. There
is no runtime knob, adaptive size search, temporary/public DDL or timer change.
If this is not a clear causal winner, stop shape tuning and retain baseline
SQL while Root addresses measured system pressure before another live trial.

## The local broker owns durable session storage

The shared DEV Mosquitto config also serves legacy lookup tooling. Its previous
`persistence false` meant a broker restart could discard durable subscriber
queues. The canonical broker had no explicit data volume. Keep one shared
configuration, enable persistence in `/mosquitto/data`, and have the existing
finite initialization job prepare a dedicated1883-owned0750 volume directory
without replacing saved data. Preserve queue caps, listeners and reload transport.

The30-second periodic save policy is explicit: PUBACK does not certify per-message
disk fsync or power-loss preservation. An isolated owned-broker test must verify
offline QoS1 sessions and retained data across graceful restart, initialization
byte preservation, and the base image's writable data-volume legacy contract.
Atomic source publication keeps the active broker's old bind inode until Root's
reviewed empty-queue recreation. Generic target transition checks remain separate;
neither persistence nor broker health alone proves fixture backlog is drained.

## Alloy checks the running server without starting another collector binary

Actual inspection shows `/bin/alloy` is450MiB and the pinned image has Bash but
no curl or wget. The health check launches that complete Go executable every
30seconds solely to validate the config file. The existing running HTTP server
answered `HTTP/1.0 200` for `/-/ready`; Grafana documents200 after initial config
load and503 before readiness. Source: https://grafana.com/docs/alloy/latest/reference/http/.

- Needless link: routine runtime readiness depends on a second collector-sized
  process loading configuration; startup already loads the same config.
- Fix: the service's own health check uses existing Bash TCP support to GET the
  local readiness endpoint and require a valid HTTP200 status. Keep the existing
  Docker interval, timeout, retry and start-period settings. Bound status reads;
  refuse malformed/failed/closed/stalled responses.
- No replacement image, dependency, listener, component, journal input or cursor
  is added. Actual log-delivery work proofs remain independent of readiness.
  There is no claim that this alone caused the previously measured I/O or swap.
- Native local HTTP behavior checks must cover success,503, malformed/closed
  responses, connection refusal and a stalled status line. Independently review
  this batch before rollout; final full-stack resources still decide acceptance.

## Counter investigation outcome: retain baseline SQL

None of the three prospective rewrites has a demonstrated overall win. Direct
array membership increases planning/selectivity costs; the raw EXISTS trial
broadens child work; fixed1024 reduces the parent custom-scan loops from72,120
to71 and parent time from3.358s to2.118s, but changes cardinality estimates and
causes broader child reads/spill. Its total5.265s warm execution is worse than
the baseline3.981s. Keep all trials as negative evidence and leave production
`soakWorksetCounts.ts` byte-exact at1d2ad9db; pool1/30s/120s and mandatory orphan
audit remain unchanged. Stop SQL-shape tuning until Root's pressure fixes and
new metrics support a controlled load reproduction. Preserve two independent
relational coverage additions for shared UUIDs across times and same-hash orphan
children. These constant-JSON tests cover semantics, not a performance fix.


## Broker quiescence has one read-only observation owner

The failed full run left durable subscriber backlog. Broker health and database
selection cannot prove that backlog is safe to hand off; persistence protects
queued messages but does not drain them. Copying ad hoc SYS subscriptions into
rollout and target selection would create separate interpretations of the same
broker state.

Keep a broker-only observer under `scripts/local-sample`: one unique clean MQTT
session reads a fixed set of SYS metrics and watches `#` for retained application
data. It never publishes, changes other sessions, clears data, restarts services
or imports database/target/persistence-proof orchestration. Require all metrics,
zero current packet-out queue and store equal to retained count, with three
strictly advancing live uptime epochs inside one45-second bound. Retained uptime
alone is not liveness; unchanged retained SYS values are cached observations,
not fabricated new metric messages. Preserve every observed unsafe condition.
Require zero disconnected durable sessions even when their queues are empty:
Root's protection policy rejects sessions that could later accumulate fixture
traffic and reconnect against an old store. Record bounded counts/digests
instead of topics or payloads.
Missing, malformed, stale/nonadvancing, inconsistent or unsafe evidence and any
client/deadline/cleanup failure refuse the gate. The caller owns source, broker
identity, target locks and independent application drain proofs. Test the owner
with fake clients before Root coordinates an actual broker observation; neither
this observer nor durable storage waives existing recovery/integrity criteria.

## D2: target intent, intake observation and controller leases

The protection failure exposed a metadata-only selection link: changing target
files did not establish what stores the actual MQTT consumers still used, and
broker drain alone did not include active Core/Beacon work. The prospective
private API/decision was recorded before this batch; runtime remains unchanged.

Keep role/ref decoding pure and shared with runtime validation. Each target
intent pins old and requested refs per actual consumer. Fsync that intent before
publishing either settings file; a torn publication resumes only the same owned
request. Mixed known old/requested consumers remain distinct, foreign refs refuse,
and only a complete healthy authoritative service inventory closes the intent.
Unknown stopped changing consumers must resume their retained previous store;
same-store starts can bootstrap missing observability without declaring it empty.

A separate inherited FLOCK lease covers managed publication through drain and
cleanup, and precedes the existing controller lock. Validate actual descriptor
identity/mode, and bind shell re-entry to a live ancestor PID/startticks/argv;
flags alone cannot bypass either owner. Detect known unmanaged native publishers
using actual process cwd as well as absolute source/dist entrypoints. The fixture
owner owns file publication; the controller applies targets, without adding
fixture imports to the safety/observation owners.

Before each changing main or companion subsystem applies, run one fresh combined
45-second broker/application observation after its build work. Reuse the broker's
existing observer; preserve its refusal output. Application adapters use their
own intake statistics, not raw storage queries: Core pending+active/outstanding,
monotonic generation, PID/startticks, freshness and drops; Beacon's existing
loopback admin queue (including active work), process start and drops; UKMesh
queues and a fresh normal delivery-probe success. The Core generation is a
monotonic transition counter, superseding the early proposal's nonce wording.
Concurrent observation waits for the existing normal probe inside the same bound;
there is no forced probe, polling extension or cached proof across lengthy builds.

Keep publisher/role/privacy/resource/response policies unchanged. This is local
controlled quiescence, not an atomic fence against an external broker publisher.
No failed qualification is reclassified; no app stores or targets change in the
source/test stage. Larger-host main Compose overrides are documented separately
from DEV's clean, fixed qualification environment.

Review closure: external same-store controllers also retain a shared lease,
including while an independent managed publisher is active. Inspect desired and
actual refs under SH; if a scope really changes, release SH before acquiring EX
and reread both authorities. Never upgrade a live shared descriptor. Restart
scope follows only the recreated consumers, including both Core processes and
Beacon's migration/DB dependencies; the pending intent still validates all
consumer refs. This lets a stopped requested-side backend resume while another
side remains old, without declaring the overall transition complete or empty.
The public shell boundary also clears caller environment before Python verifies
its entry frame. Preserve only PATH/HOME/LANG and the raw lease/frame strings;
Python validates their descriptor and ancestry before use. A poisoned caller
PYTHONPATH regression covers imports as well as the final controller child.

## Separate delivery-control topics from local business subscriptions

The first strict target-selection gate refused safely because the ordinary
UKMesh QoS1 delivery probe shares its first business topic prefix with Core's
`meshcore/#` subscription. The probe carries a random UUID, not packet JSON.
Core correctly accounts for its accepted callback, advancing generation by3
even though no storage counter changes. The actual read-only correlation at
`probe-noop-correlation-20261115T064733Z/result.json` (SHA
`d149671bce9acb72f19b4b56f8d4705dc0d8cdc6abd89955ebd362193f7096b7`)
binds one normal probe, unchanged original stores/processes and zero writes.

There is no existing independently configurable probe namespace. Add an optional
backend-owned `MQTT_DELIVERY_PROBE_TOPIC_PREFIX`; its unset/blank default remains
the first business prefix, preserving existing topic bytes, ACLs and filters.
DEV selects `ukmesh-local-control`, outside every local business subscription.
When that probe topic is not covered by the existing business filters, subscribe
to that exact topic at QoS1 on the same persistent ingest client. Await every
business/control SUBACK before starting the existing probe; denial cannot
certify delivery. Do not broaden the business subscription or mutate its prefixes.

Keep the nonce, QoS1/nonretained behavior, normal30-second interval,60-second
deadline, recovery cooldown, durable intake and all strict generation/quiescence
checks unchanged. Do not teach Core about UKMesh probes or add a no-op exception.
Test production defaults, configured coverage, invalid prefixes, actual MQTT wire
SUBACK/echo behavior, denied subscriptions and ordinary business dispatch before
review/build. Root will separately verify a fresh normal probe with stable Core
generation after the reviewed local runtime change; no source-stage rollout.

## Compose producers must enter the existing journal pipeline

The actual Core PostgreSQL proof timed out and restored its fixture safely, but
its container was removed before its default json-file logs were archived. The
retained Loki query is empty. Do not infer the missing historical exception or
claim that log history can be recovered. Alloy already reads labelled journal
entries across projects; the missing link belongs to each Compose producer.

Before editing producers, the private decision/proposal recorded the source
hashes and lost-log limitation. Use the existing journald driver, container Name
tag and Compose service/project label options in each owning configuration:
all companion services/tools, the four local-sample services, the local reloader
and alert bootstrap, and the main Health Check data-init job. Keep main Alloy's
bounded10m x3 json-file self logs; feeding the collector's own logs back into
its input is unnecessary. Keep journal inputs, relabels, cursor storage, Loki
retention, every resource cap and the host's existing journal policy unchanged.
No Docker API mount, collector, log vacuum or host policy change is introduced.

Resolve normal, hot and Core-proof Compose merges without daemon access and
assert all required43 service producers have this policy or Alloy's explicit
self-log exception. Check the existing service/project/container relabel path
has no project restriction. Root will later inspect actual driver metadata and
retention after coordinated protected recreation; source-only tests do not
claim actual end-to-end delivery or a measured numeric host journal quota.

## Distinguish projection ownership from an active SQL transaction

The current-source SAMPLE PostgreSQL proof timed out before projection commits.
Retained source/data showed the correct reader policy and unchanged source cursor;
failed application logs were unavailable after the existing cleanup recreated the
pair, so no exact historical application error is claimed. Private reproduction
uses production's root Store projection assignment and deterministically fails
with the new transaction guard. Earlier fixtures held a projection variable but
did not assign that root ownership pointer as main does.

`publicProjection` denotes the policy/provenance owner, not an active transaction.
Steady Batch checks current policy, FULL-journals records and fences ready=0 before
calling its decoder on the root Store. Only `replayTx` denotes a scoped executor
with the writer mutex already held. Remove the mode-pointer nesting rejection;
retain the real transaction guard and FULL/WAL check. Preserve policy, retention,
private revocation, journal, final ready publication and source cursor rules.
Replay pages retain their existing reconstructible NORMAL owner and FULL restore;
public mode still forbids MQTT sources. Correct prototype comments to explain
this actual capability rather than a future refactor.

Before promotion, rooted steady Batch/dedup/privacy/replay/failure/cursor fixtures
failed on current2719 and passed the minimal candidate (18 top-level+11 subcases).
Full CGO ingestor/server suites and independent review precede commit. The failed
actual receipt stays failed; Root must separately rebuild and rerun its unchanged
PG gate before further qualification. D1, queues, limits, deadlines and RF helpers
remain unchanged.

## Reserve the SQLite writer before reading a packet transaction

The current 1x recovery run passed fourteen faults but lost CoreScope packet
observations during the owned SQLite contention fault. Five packet insert errors
returned immediately while a later status write succeeded. The native failure,
journal logs and cleanup proof are retained. The explicit one-second delay in
that fault is not its entire lock duration: publication and count reads also run
before the holder rolls back. Do not relabel the failure or extend its deadline.

The packet transaction reads before writing. A deferred SQLite transaction can
fail that upgrade immediately instead of waiting through the existing busy
handler. Configure `BEGIN IMMEDIATE` in the authoritative writer DSN shared by
the ingestor and migration CLI. Keep the server's separate read-only connection,
FULL/WAL, five-second busy timeout, single writer pool and replay's explicit
durability and privacy fences. This changes lock admission, with no additional
packet pass, retry queue or dummy write.

Admission errors occur before scoped write statistics exist. Count and wrap
them at the observation owner; the buffered MQTT boundary reports its returned
error without counting downstream SQL failures again. The pinned driver can
also return a successful BEGIN after context cancellation, before asynchronous
rollback is observed. Check the context after BEGIN and immediately before the
packet callback, retaining rollback and the original cancellation assertion.

Separate-connection tests first reproduced the packet loss and missing error
reporting. Corrected raw, channel and direct handlers wait for release, keep WAL
readers available and reopen exact durable observations. A fixed twenty-trial
cancellation run and the full affected CGO suites passed before local promotion:
schema 30, migration 1, ingestor 1016 and server 2963 pass events, with nine
explicit conditional skips. The earlier full-suite cancellation failure remains
retained. Buffer-test cleanup is bounded and a duplicate assertion was removed.
Independent review binds the exact four files. New binary integration, recovery
and full-stack load proofs remain required after the guarded rollout.

## Reusable read plans belong to their physical connection

The current9521/b655 full-stack1x matrix failed the unchanged100ms gate:
observer-map113.029/109.003/116.930ms, plus isolated paging/region over100ms.
The failed matrix, its immutable native windows and the passing operations
trials remain retained. Actual API-role read-only plans identify5.874–8.538ms
planning against8.394–9.669ms execution; no disk reads or expensive legacy scan.
The two-CPU100ms setting remains unchanged: a controlled same-allowance10ms
comparison was slower and was restored/rejected.

Fix: opt in to bounded read preparation through the existing query contract.
The prepared-statement owner resolves names only after checkout, reserving at
most32 SQL fingerprints per actual client, retaining no values, and falling
back to unnamed execution when full. Preserve fixed ingest statements and
current transactions, cancellation, session settings and pool sizes.
- Hidden link: unchanged parameterized map shapes are replanned on every read.
- Hidden link: names live on physical connections; unbounded request-global
  naming hides retained client/server memory from its pool owner.
- Needless link: a global pg.query monkeypatch would couple unrelated executors
  to map behavior. Use an explicit frozen bounded-read hint instead.
- Special case retained: ingest owns two fixed write statements; it refuses the
  read hint before dispatch. Named path-beta execution retains its own contract.

A private read-only prototype reused real generic plans for observer/region
reads and reduced observer p95to70.664–90.523ms, but first page105.679ms and
next page106.208ms remained failures. Existing normalized data digests, entry
counts and recorded response lengths matched; raw HTTP bodies differed because
snapshots differ. A separate nullable-cursor split produced much worse generic
plans (308–497ms tails) and is rejected. Keep the original nullable-cursor SQL.
No generic-plan forcing, result cache, response-limit or latency-gate change.

## The response representation owns its already-encoded row bytes

The main-thread CPU profile sampled byte-budget fitting at~7% of non-idle
sampled time and Express stringify at~5.4%; this is sampled attribution, not
whole-process or worker CPU measurement. The current fitter encodes every row
to enforce its exact byte limit, then Express encodes those same rows again.

Fix: preserve the original per-row budget loop, early truncation and error
order; retain the accepted encoded rows as a bounded JSON-array representation.
The map HTTP owner pairs that representation with its unchanged data/page object.
The shared public JSON delivery owner may send the encoded body only after both
existing successful generation reads. Express still owns send, ETag and304.
- Hidden link: budget accounting discards bytes the HTTP serializer needs.
- Needless link avoided: a whole-array speculative fast path could visit later
  invalid rows and allocate beyond the old stopping point. Keep the exact loop.
- External constraint: custom Express JSON replacer/spacing/escaping changes
  encoding. Retain the original value and use res.json when those settings apply.
- Preserve ordinary flat PostgreSQL map rows, coordinates, Unicode, fields,
  pagination,512-byte reserve and1.5MB cap. No serialization shortcut for received
  message content or a different endpoint is implied.

Independent source review and meaningful tests must close this batch before
promotion: bounded names/lifetime/overflow/no captured values; fixed-ingest
refusal and existing cancellation/transaction ownership; real HTTP byte/ETag
parity, privacy failures, custom-format fallback and exact budget boundaries.
Then rebuild the backend and repeat relevant native/performance qualification.

## Batch identical reads only before their SQL starts

The bounded-preparation/encoded-response batch passed type checking,1,022unit
tests (14existing optional skips),8nativeSQL tests with168canonical map cases,
HTTP/SQL privacy fences and all36cold C8results. The warm C8gate still failed
7of36results, up to161.530ms. Cleanup retained exact43/profile/source/backups.
A guarded pool2counterfactual was slower and introduced long-lease capacity
risks; retain HTTP8. A guarded PG2→3→2CPU comparison improved throttling but
still failed100ms. It restored exactly2CPUs; no resource change is promoted.
All failures and source/measurement scopes remain retained.

Fix the duplicate body work at the request owner: group equal public map/history
reads queued in the same dispatch turn. Remove the group BEFORE invoking its
loader, so every joiner arrives before the shared statement snapshot. A later
request, including one arriving during the first SQL, starts a new read. This
keeps read freshness rather than introducing a completed-result cache.
- Hidden link: simultaneous equal reads repeat SQL and row decoding despite
  using the same normalized arguments and visibility generation.
- Hidden link: sharing a body across generations could expose a previous privacy
  projection. Keys include the exact successful initial generation and every
  normalized argument; both authority reads remain separate per HTTP request.
- Hidden link: sharing mutable rows could let a custom JSON formatter affect
  another caller. Clone the raw result for EVERY caller before fitting/encoding.
- Needless link avoided: batch instances belong to each registered map/history
  route runtime, never a module-global DB or source owner.
- Bound at64pending keys per feature; overflow executes normally. No new503,
  TTL, query timeout, HTTP concurrency change or request-snapshot normalization.

A small shared queue owns pending dispatch and closure. Closing settles queued
callers and cancels handles; dispatched work retains existing DB ownership.
No response authority query, active SQL result or completed result is coalesced.
Independent design review3cae0e3b confirms the snapshot/key/lifetime requirements.
Meaningful tests must prove separate scopes/generations, after-dispatch fresh
reads, isolated nested/Date values, failures, overflow and close behavior, plus
actual SQL visibility. Then repeat relevant checks and the unchanged C8gate.

The first dispatch-turn implementation passed1,037unit tests (14existing
optional skips) and9nativeSQL tests, including the held-snapshot/external-commit
oracle. Its cold C8gate passed35of36results; map-page trial1was102.223ms, so
warm was not run. Observed body work remained444map reads per609HTTP reads
(including warmups), rather than full equal-request cohorts. PostgreSQL authority
completions often arrive in different poll turns, before/after an immediate
flush. All failures, exactSource23/backups/43 and cleanup are retained.

Use a fixed nominal1ms asynchronous admission window to collect those arrivals
BEFORE startingSQL. This deliberately adds measured queue time; it is not a
guaranteed wall-time bound under event-loop load and remains inside every
unchanged100/250ms measurement. It introduces no result/active-read cache.
Keep exact key deletion-before-load, clones,64pending/ordinaryoverflow and
privacy reads; clear the matching timer on close. Replace test assumptions
about a particular immediate turn with loader-start barriers, while retaining
the existing real SQL snapshot proof. Independent prospective review8c33a7fc
confirms this scheduling amendment preserves the fresh-point contract.

## Give each queued read one immutable response representation

The nominal1ms batch passed all1,037unit and9nativeSQL tests, all36cold
C8results and34of36warm results. Warm map-page167.751ms and short search
111.468ms still fail100ms. All43/profile/backups stayed exact. Keep this
failure and its independent actual audit; no resource or gate change follows.

Fix response ownership at the existing queued-body boundary: the cohort loader
owns SQL, exact byte fitting, page metadata and encoding once. Share only an
opaque PreparedJson marker and primitive snapshot/complete metadata. No raw
rows or mutable page object escape the shared representation.
- Hidden link: the raw-result batch reduces SQL but each caller still deep-clones,
  byte-fits and encodes equivalent PostgreSQL rows, multiplying allocation and GC.
- Hidden link: a shallow-frozen DTO still exposes nested rows through the marker;
  private fields must own both raw value and immutable encoded body.
- Needless link: ordinary default Express delivery needs only encoded bytes;
  eager deep cloning ties every delivery to mutable custom-format behavior.
- Required special case: custom Express JSON settings can mutate their argument.
  Supply an optional per-delivery clone callback, invoked separately after EACH
  caller's final successful privacy fence. Default delivery uses the wire body.
  Existing two-argument PreparedJson semantics for arbitrary classes/toJSON stay
  unchanged; map/history are the actual cloneable PostgreSQL value domain.
- Keep nominal1ms/64pending/key-before-loader deletion, exact scope/generation
  keys, ordinary overflow, later fresh SQL, per-router ownership and close rules.
  Both privacy reads remain independent for every caller. No result cache.

Independent prospective reviewf219fedf accepts this explicit immutable-projection
amendment. Cover wire/ETag304/headers/privacy/custom mutation/Date isolation,
zero default clones and one encoding per cohort before changing coupled tests.
Then independently review and repeat full source/native/C8checks unchanged.

## Keep SQL admission bounded while collecting more equal callers

The immutable-wire stage passed1,042unit tests (14existing optional skips),
9nativeSQL tests and all36cold results (maxp9589.989ms). Warm results still
failed map-page104.031ms and short-search109.961ms. The independent actual
audit3782f158 closes source/profile/43/backups and helper cleanup; retain all
raw failures. This is not qualification or proof of one cause for the tails.

The current1ms window still dispatched303map and266short-search body reads for
609HTTP callers in each case (three trials plus warmups), about two callers per
cohort. Driver completion times include PostgreSQL, wire, parsing and scheduling;
these aggregates cannot isolate database execution or prove a GC explanation.

Change the single admission owner to a fixed nominal4ms window, before measuring
once with all existing gates. This tests whether collecting more independently
fenced equal requests avoids duplicate body SQL/encoding and their downstream
scheduling work. It adds nominal3ms inside the measured HTTP deadline; event-loop
delay is still counted. This is a production scheduling choice, not a cache TTL.
- Hidden link: independent authority completions and HTTP arrivals can span the
  existing1ms queue window, multiplying identical body work before dispatch.
- Keep the same64pending bound, no timer reset on joins, no active/completed
  result reuse, exact keys, per-request fences and remove-before-loader rule.
- Existing loader-start behavior oracles and real held-SQL/external-commit tests
  cover freshness without coupling tests to one nominal timer duration.
- No pool, CPU quota, SQL, response, fixture, warmup, sample or latency-gate change.

Independent prospective review accepts the fresh-point contract; performance
is unproven until the unchanged full checks and cold/warm C8stage close.


## Keep local broker fixes and builds in the broker component

Record these links before applying the reviewed local runtime changes:

- Transport sockets can exist before MQTT CONNECT creates an identified session.
  Subtracting socket count from identified-session count can underflow or hide an
  offline session. The broker owns both gauges and now partitions the identified
  contexts with its existing connected-state predicate.
- MQTT 5 Clean Start discards the previous session; Session Expiry controls the
  new session's lifetime. Separate client and subscription persistence filters
  used Clean Start as a lifetime rule, losing positive-expiry sessions and
  disagreeing about bridge policy. One broker-owned eligibility predicate serves
  client/message and subscription saves, with the bridge's local policy retained.
- A global UKMesh revision is unrelated to the broker executable. Its narrow
  build context pins the upstream image, source archive, patch and resulting C
  files. Only the broker executable enters the final image. Application builds
  and the broker build keep separate artifact identities.
- The existing launchers must build that component before starting infrastructure
  even when a local tag exists. They orchestrate the build; the component recipe
  owns compiler flags, patches and upstream constraints. Existing URLs, runtime
  settings, volumes, resource caps and finite initialization jobs remain intact.
- Changing a broker executable on the same store differs from changing consumer
  database targets. Preserve the closed store and require fresh connection
  evidence before any maintenance; the strict target protection gate is unchanged.
  The old writer can omit MQTT 5 sessions, so a closed-store backup alone cannot
  establish that arbitrary old in-memory sessions were preserved.

Private evidence is closed: 26 counter/setup phases, 12 restart/replay phases,
three observer epochs, and 18 real persistence cases with 379 assertions pass.
The original writer fails exactly the MQTT 5 fresh-session case and two bridge
policy cases. Original fixture tests still pass byte for byte. Earlier failed
attempts, including test harness setup failures, remain recorded. The canonical
recipe produces the exact private-tested image 96f6be4a and binary 79ea71b5.
Independent reviews d420744a, 35ab1043 and c4b39dad bind the actual results.
These are component proofs; full-stack qualification and shared-broker promotion
remain separate pending work. No service restart or target change occurs here.

## Verify launcher behavior through its commands

Before changing launcher test fixtures, retain their existing migration, backfill,
foreign-process, rollback and interruption coverage. Extend their fake command
boundary only for the new exact broker build command. Add failure injection that
proves a failed broker build prevents infrastructure/application startup and
preserves retained fixture data. Do not replace behavior checks with source-text
or helper-name assertions. Keep comments explaining external constraints.

## Prepare native connections before target protection

Record these links before changing the startup boundary:

- A healthy broker container does not establish that its existing application
  clients have reconnected. Infrastructure reconciliation can therefore leave a
  valid pending fixture switch stopped at the first application readiness read.
  The target entry point supplies a preparation callback only after the existing
  intent, reference and observation eligibility checks.
- A new service readiness owner observes native process and connection state for
  running consumers. It permits backlog and changing CoreScope generations; the
  existing observation owner remains responsible for drain and protection.
  Preparation has a 60-second maximum, followed by the unchanged 45-second
  protection window. Resident service health retains its 240-second wait. The
  selector's existing 1,200-second command wait is not a new global manual-start
  deadline or proof that every pre-existing subprocess is reaped.
- Probe lifecycle belongs to that readiness owner. Hold its session leader
  unreaped with Linux waitid(WNOWAIT) until descendants settle, establish identity
  while cancellation is blocked, and defer the first cancellation through cleanup
  and receipt writing. A cancellation must not produce an authorizing return.
- Native contracts remain with their adapters. Beacon's actual OnConnect logs
  include stderr and supplement its reconnecting-capable SDK status. CoreScope's
  same-document/process adapter preserves its existing intake result, and permits
  an omitted source-status list only when no MQTT source is configured. Schema
  errors are fatal; temporary unavailability is distinct from an empty queue.
- Compose adds three image configuration labels and provenance to the direct
  broker build. Its running platform manifest f09cafc5 has the same four layers
  and executable 79ea71b5 as the privately tested 96f6be4a artifact. Keep those
  identities and their proof scopes explicit; the older direct-build statement
  above does not establish a full Compose image identity.

The existing cold-start, stopped-consumer, partial-transition and fixture guards
remain authoritative. This batch changes no application API, database rule,
resource setting or workload deadline. Validation is recorded with the reviewed
24-case startup batch and the existing target, intake and launcher regressions.

## Compare complete broker statistics updates

Record this observation-owner link before changing the predicate timing:

- Mosquitto publishes uptime first, followed by changed gauges. A fresh stored
  count and an earlier retained count are not a coherent tuple. The controlled
  reconnect correctly recovered all four consumers, but the old observer latched
  store52 versus cached retained2, 78 microseconds before retained52 arrived.
  The failed real attempt and exact eight-packet replay remain recorded.
- The next live uptime closes the preceding producer update. Validate every
  completed tuple, retain three immutable epoch records, and require four live
  markers inside the existing 45-second maximum. Check tail updates through the
  existing own-UNSUBACK barrier, including after the third recorded epoch.
- Positive queued/disconnected counts and retained application data remain
  immediately sticky, including before initialization. Malformed or reordered
  metrics, message/byte bounds, session, subscription and cleanup failures still
  refuse protection. No counter normalization, arbitrary sleep, publisher, reset
  or extra readiness barrier is introduced.
- Preserve the legacy safety/lifecycle tests. Once equivalent behavior coverage
  exists, adapt only their marker schedules. Keep the exact mixed-batch replay,
  split updates, genuine closed mismatch, late retained replay, incomplete epochs
  and final-barrier negatives as a permanent production-path regression file.

This fixes measurement coherence at its owner; it changes no application data,
API, database policy, resource budget, broker binary or workload deadline. Native
reconnect55516 remains failed at its original postcheck. Later protection-only
recovery is separate evidence; a new real controlled check is still required.

Run these observer cases and the existing DEV startup/target/intake regressions
in the backend CI job after its MQTT dependency is installed. This keeps behavior
coverage with the owning code without provisioning or contacting a DEV stack.

## Keep qualification credentials with their capability owner

Record these links before applying the credential-repair batch:

- The common native-tool launcher gives fixture-administrator credentials to
  preparation, semantic oracles and timed route benchmarks. Its read-only helper
  then copies that one URL into every logical pool, hiding HTTP, analytics,
  ingest and owner ownership. Existing service sessions and live HTTP soaks use
  their intended roles; the defect concerns the three native read benchmarks.
- The invocation boundary will distinguish administrative fixture work from
  application read measurement. Read measurements derive credentials and pool
  settings from the actual selected backend consumer, validate the owned target
  and published loopback port, and pass secrets through the child environment.
  Shared subprocess code continues to own inherited target-lease descriptors.
- The existing application role resolver remains authoritative. Apply read-only
  mode independently to each resolved URL, validate all updates before changing
  the environment, and keep the separate owner database. Standalone commands
  accepting only DATABASE_URL retain an explicitly labelled inspector scope;
  that scope cannot qualify application-role performance.
- Replace existing setup read-only checks with authenticated identity checks,
  then collect representative identities and pool metadata after timing. Do not
  pre-open clients, add warmup, query unused pools, expose credentials or claim
  every historical physical connection was observed. Validate these native
  proofs when admitting canonical evidence.

Keep prior native results, their administrator-read-only limitation and the
failed companion comparison. Fresh matched measurements must use the corrected
method for both source trees, with unchanged timing, workloads and acceptance
limits. This tooling change does not itself change the running application or
RF binaries; record actual image provenance separately from the new tool source.

## Pass fixture context at the read-window validation boundary

The read evidence validator serves both in-progress mode checks and complete
window checks. Adding authenticated role validation exposed a hidden link: the
in-progress caller supplied a smaller record without fixture context. Its first
native mode completed, then the Python owner failed with KeyError before the
remaining modes or demand cohorts. Retain that failed attempt.

Pass the already captured context explicitly at the owning call. Keep the
validator free of target discovery and fallback credentials. Add positive
orchestration coverage through the real validator, then exercise a complete
single window on the selected fixture before committing and restarting matched
qualification. Do not change workloads, timing or acceptance limits.


## A test fixture owns its Redis client through cleanup

- Hidden link: F08 registered stream deletion with `t.Cleanup` but closed its
  Redis client in a function defer, which runs first. The ignored deletion
  error left a fixture stream after otherwise passing crash-recovery tests.
- Fix: keep deletion and close under one cleanup owner. Delete with a fresh,
  bounded context, check its error, and close the client afterwards. Keep the
  strict zero-schema/zero-key and exact container-removal checks.
- The first isolated run remains failed (`74eb43a7`); all eight assertions
  passed, but one stream remained. The repaired isolated run (`14d9fc00`) and
  full worktree run (`8faba0f9`) passed with no remaining schemas or keys.
- Full native validation exercised both Go packages: 1,022 ingestor and 2,965
  server passing events. Three standalone worker helpers remain disclosed
  skips and are exercised by passing parents. The original distance-array
  test's exact HTTP 202 skip is covered by the completed-index overlay. Other
  skip reasons are rejected. Independent actual audit: `295cdeb2`.
- CoreScope commit `ec65f8b12e8343b924314ac391be3697bd2a5ebe` contains only the
  reviewed fixture repair; its test bytes match the validated worktree. Its
  application implementation is unchanged. This component evidence does not
  replace full-stack performance or recovery qualification.


## The prepared representation owns its reusable HTTP validator

- Hidden link: equal queued responses already share one immutable encoded body,
  but ordinary Express delivery hashes that body again for every reader. The
  native CPU profile attributes 174 ms to this ETag path.
- Fix: keep one last known built-in generator and validator inside the existing
  opaque representation. Each delivery still owns fresh bytes. No mutable byte
  cache, completed-response cache, global validator cache or copied hash
  algorithm is introduced. Routes, SQL admission and their 4 ms bound stay with
  their existing owners.
- Privacy authority remains independent: both fresh generation reads must pass
  before selecting a validator or delivering a body. Express still handles
  content length, conditional freshness and HEAD/304. Custom serializers,
  application/header hooks and preset headers retain their existing path.
- Two earlier diagnostics remain rejected: an 8 ms admission delay and private
  UTF-8 byte reuse did not show a consistent benefit. The ETag-only ABBA run
  (`9f1f5a7d`) reduced aggregate CPU 6.7%, wall time 7.4% and median trial p95
  7.5%; warm-only median p95 increased 7.1%. All 24 map trials met 100 ms.
- Broader private validation (`2c010548`) passed all 144 lookup trial rows and
  all 9,600 reads over the unchanged 120-second, 80-RPS schedule. The largest
  C8 p95 was 75.5 ms. All 27 HTTP compatibility checks and a bound full backend
  typecheck passed. Independent adoption-readiness audit: `012ecfac`. A tiny
  native scheduler rounding warning remains recorded with its passing actual
  timing checks.
- This supports the exact reviewed implementation and normal repository
  regression checks. The canonical 1x/10x matrices, full load/RF qualification
  and restoration of original targets remain required; the original 115 ms
  failure is retained.
- Normal repository validation passed: 1,067 unit checks, typecheck, the 75 API/
  11 operator route contract, local alert firing/recovery and production build.
  Fourteen pre-existing conditional unit skips retain their separate native or
  optional dispositions (`41263fec`). Both existing real-PostgreSQL ordinary
  and prepared privacy-fence tests then passed without skips (`87ed1abc`), with
  their temporary databases and instance removed; independent audit `ac6ed9a8`.

## Bulk fixture preparation and chart refresh share one admitted boundary

- Fix: explicit fixture preparation joins the existing database heavy-work
  admission before its first mutation and retains the claim through derived
  publication. It keeps two connections: one admission session and one sequential
  work session. SQL, history windows, durability and all timeouts stay unchanged.
  - Hidden link: preparation publishes a visibility generation before its
    path facts finish, while normal chart warmup can independently rebuild the
    same facts. The 0eee949 10x rehearsal recorded overlapping work and an
    82.139-second query against the existing 60-second budget.
  - Needless link: separate setup and runtime owners compete for the same
    analytical resources despite an existing shared admission mechanism.
- Fix: a canonical chart job obtains its current privacy generation after
  admission, and derives its cache key there. Computation, validation, SQL
  publication and caching use that generation under the same claim.
  - Hidden link: caller-supplied generation and cache key can become obsolete
    while a job waits behind another process. Scope single-flight and final
    privacy publication fences remain in force.
- Remove the obsolete count in the admission comment; retain PostgreSQL's
  session ownership and crash-release constraint.
- Regression coverage uses the existing migrated 25-packet fixture, real
  independent chart/preparation owners, exact owned cancellation, whole-window
  coverage, and a privacy change during deferred chart admission. Native waits
  observe live lock relationships rather than SQL formatting.
- The failed 10x rehearsal remains failed (map p95 253.084ms, sustained swap
  65.763s, chart budget failure). Coordination fixes have independent source
  review; native checks and fresh qualification must demonstrate their effect.

## Map suppression owns an exact candidate index

- Fix: build a per-network latitude index inside the existing pure suppression
  function. Use the same Haversine calculation at equal longitude as a
  conservative lower bound, then apply the unchanged full-distance predicate.
  Select the original freshness/node-id rank and preserve stale input order.
  - Surplus: the old stale-by-fresh scan evaluates same-network nodes whose
    latitude alone rules out a match. The actual 34,195-node diagnostic measured
    22.079 seconds of pure computation versus 539 milliseconds selecting input.
  - Hidden link: latitude traversal order must not become winner order. Original
    stable freshness ranks remain the authority, including duplicate IDs.
- Keep coordinate/time filters, networks, configuration, exact radius tolerance,
  privacy SQL and the refresh transaction unchanged. No cache, pool or setting is
  added. Dense latitude bands can still require many comparisons.
- Independent original-loop oracle tests cover order, ties, invalid inputs,
  radius boundaries, poles, antimeridian and custom policies. Private focused
  checks passed 19 tests with no skips; full runtime qualification remains open.

## The region child owns its shared admission and publication session

- Fix: the existing heavy-work owner passes a query-only session capability to
  its task. Regional rebuilds use that same session for shared admission,
  private publisher ownership and the final publication transaction. The second
  connection remains available for sequential reads; the pool stays at two.
  - Hidden link: the region child previously bypassed the shared analytical
    claim and could overlap the same large packet scans as charts and fixture
    preparation. Parent ownership would also disappear before an orphaned child.
  - Needless dependency: a separate publisher checkout plus another admission
    checkout would exhaust the existing pool before publication.
- Keep SQL, history, privacy generation fences and the parent's 180-second child
  deadline unchanged. Acquire the fresh generation after admission. Admission
  wait is included in the existing deadline.
- The admission owner alone releases its session. Failed tasks destroy it, and
  a failed private unlock propagates, preventing nested locks or failed
  transactions from returning to the pool. Cancellation after acquisition is
  checked before dispatch.
- Independent review corrected a stale release expectation and replaced a new
  SQL-text cancellation probe with actual blocker relationships. Five private
  helper tests passed without skips. Disposable PostgreSQL behavior checks and
  renewed 10x qualification remain required; both prior failed runs stay failed.

## Path CPU capacity follows the lean runtime budget

- Fix: set the existing path-resolve worker default to one in the application,
  main Compose configuration and example environment. Keep the generic worker
  pool independent of the application's runtime policy. Larger hosts can set
  the existing override; its one-to-eight bounds remain unchanged.
  - Hidden link: worker count selects both CPU concurrency and the possible
    number of independently retained worker contexts. A CPU-based default alone
    does not account for this VM's memory budget.
  - Hidden link: an absent environment value must select the same application
    capacity as the main configuration. DEV's clean launcher and larger-host
    main Compose overrides are documented with their actual scopes.
- The 29b replay passed delivery, deadlines, foreground latency and log checks,
  but failed headroom (11/12 steady samples) and swap growth (169,590,784 bytes).
  At the low-headroom sample, backend anonymous memory rose from 162,750,464 to
  550,903,808 bytes near slow-path completion activity. Worker context allocation
  is a candidate cause, not a measured attribution: hydration is lazy and LIFO
  dispatch can leave a second worker unused. No per-worker heap/CPU trace exists.
- This is a prospective capacity change. Preserve algorithms, complete graphs,
  history, caches, queues and deadlines. The unchanged full-stack 1,800-packet
  trial must establish throughput and resource behavior before advancing. The
  failed 29b evidence stays failed; no resource threshold or warmup changes.

## Progress serialization belongs to its producer

- Fix: the owner last-hop warmer emits each progress record as one JSON line,
  keeping its existing message and numerical fields together. The existing log
  collector already interprets structured zero counters correctly.
  - Needless link: Node's multiline object display exposed its formatting to
    the strict failure detector. Two recorded `failed: 0,` lines were benign
    counters, while the same older run also contained real timeout failures.
  - Surplus: format-specific exceptions in the collector would couple it to
    one producer. Keep the collector and its criteria unchanged.
- Preserve all warnings, scheduling, concurrency, cache policy and individual
  failure handling. Keep the external owner-dashboard/cache rationale and
  remove comments that restate the implementation.
- Independent source review passed both changes. The original thirteen warmer
  behavior cases plus two structured-progress cases passed privately without
  skips. Full regression checks and the new runtime trial remain required.

## Scheduled regional rebuilds defer before doing work when admission is busy

- Fix: one shared session owner implements blocking admission and a one-shot
  try operation. Only an explicit false lock result means busy. Query, task,
  cancellation and cleanup errors remain failures; malformed results fail closed.
  - Hidden link: a region child's wall deadline includes admission wait, while
    fixture preparation legitimately holds admission much longer. Starting a
    waiting child can leave too little budget for its unchanged SQL work.
  - Needless link: scheduled polling was coupled to blocking manual-command
    behavior. A busy scheduled attempt now ends promptly and the existing poll
    reads the latest generation before its next attempt.
- The background child owns the claim. A fixed internal protocol argument and
  reserved busy exit share one pure protocol module; the parent receives a typed
  deferred outcome. It does not certify publication, report success or alter
  same-generation failure history. The existing next 30-second poll remains.
- Keep the 180-second child timer armed at spawn, including all work. Direct
  manual rebuilds retain blocking admission; the manual CLI itself has no such
  wall timer. Preserve its two-connection pool, private publisher ownership,
  all SQL and history windows, math and final privacy-generation fence.
- The a13 replay passed native delivery/latency and every resource gate but
  failed its logs on a genuine regional deadline and PostgreSQL disconnect.
  Retained timing permits an admission-wait explanation, not an exact acquired
  timestamp or SQL duration. A later certificate (92-day window, generation16,
  finished13:38:52) confirms later publication, not clean prior qualification.

## The admission owner reports cleanup failures explicitly

- Fix: both admission interfaces use the same acquire/task/unlock/reclaim owner.
  Failed tasks or unlocks destroy that session exactly once. A task error plus
  unlock error retains both causes. A successful task cannot conceal an unlock
  failure behind a successful return value.
  - Special case removed: the previous helper logged a shared unlock failure
    and destroyed its session but still returned the task's successful value.
  - Surplus removed: one-shot options omit the blocking retry setting. No
    workflow-specific policy or duplicated lock/protocol constants are added.
- Preserve PostgreSQL's session/crash-release constraint. Independent source
  review passed; focused, real-PostgreSQL and fresh stack checks remain required.

## Reuse bounded preparation for unchanged node history

- Fix: opt the history read into the existing connection-owned, 32-definition
  bounded statement registry. It caches SQL definitions, never response rows.
  - Surplus: the exact history SQL repeatedly incurred planning work even for
    repeated values. A fixed read-only AB/BA/AB diagnostic returned identical
    ordered500-row bytes in all48 calls; prepared wall time fell60.9%, while
    Node CPU rose1.1%. This is diagnostic evidence, not a C8-tail cure.
  - Hidden link: PostgreSQL may reuse a generic plan while aliases and privacy
    generations change. Real disposable-database behavior coverage must prove
    current data and fail-closed fences after actual generic executions.
  - Ownership: retain the existing runtime checkout, per-client preparation
    bound, overflow fallback and connection disposal. SQL, binds, history
    windows, limits, DTO, privacy fences and automatic plan choice stay exact.
- Promote the production hint and new native behavior test first. Keep the old
  mock assertions unchanged until that native coverage passes; then normalize
  QueryConfig at the test boundary and retain every assertion. No test-only
  product factory, injection argument, alias rewrite or second registry.
- Preserve genuine5adc latency failure and all earlier failures. Native tests
  use effective API-role privileges on one real read-only client in a uniquely
  owned database, not an authenticated application-session permission proof.

## Retain existing measured reads for latency diagnosis

- Fix: persist the already collected successful read records with their
  existing start offset, slot and burst ordinal, after timed work finishes.
  - Surplus: records were retained in memory for quantiles but omitted from
    evidence; only failures survived, preventing precise burst attribution.
  - Ownership: the existing workload owns every request and clock. Retain all
    actual measured and drain-tail rows; no sampling, timer, warmup, request,
    acceptance or response change. Fatal failures before final output can
    still lack those rows; do not invent missing observations.

## Bound history rows before display identity hydration

- Fix: apply the existing eligibility predicates and per-direction/global 500-row
  limits before the one-to-one source identity display join. The identity
  mapping primary key guarantees that hydration cannot multiply selected rows.
  - Hidden link: display alias hydration previously ran over eligible history
    before its limit, making custom plans scan and join thousands of rows.
  - Ownership: the repository retains eligibility, raw indexed alias predicates,
    time boundaries, scope, privacy fences, deduplication, DTO order and bounded
    preparation. Only the location of display hydration changes.
- Real read-only AB/BA/AB measurements returned identical ordered bytes in all
  48 calls and reduced query wall time 47.48%. This is a single-client diagnostic.
  Five real disposable-database tests passed after generic plan reuse, including
  alias changes, exact time boundaries, 500-row limits and privacy changes.
  Independent source and actual reviews passed before promotion. Existing mock
  assertions remain in place. Integrated HTTP latency acceptance remains pending.
- Preserve the failed private overlay run. Its runtime module identity/format
  mismatch was corrected with the identical runtime import and explicit ESM
  metadata; no credential fallback or production behavior changed for the test.

## Keep compact bootstrap transport inside one shared contract

- Fix: one generated contract owns a lossless column format. Clients explicitly
  request columns-v1; the frontend restores ordinary rows before its existing
  scope and live-event coordinator. The full synthetic probe decodes the same
  format. Legacy clients retain their current envelope and rejection behavior.
  - Hidden link: an unscoped bootstrap with all eligible nodes and 20,000 links
    exceeds the existing 16 MiB cap. Dropping graph data, narrowing the monitor
    scope or raising the cap would conceal this real application failure.
  - Needless dependency: per-client recursive descriptor and custom-toJSON
    checks validate hypothetical JavaScript inputs rather than the owning
    producer's plain PostgreSQL DTOs. The producer encoder accepts own-data
    records with native Dates and parsed JSON. The wire decoder validates
    untrusted parsed input; neither owns database or privacy policy.
  - Surplus: a second encoded cache, worker framework and full legacy JSON
    clone would add lifecycle and memory ownership. Keep existing cache weights,
    admission, limits, timestamps and final generation fence unchanged.
- Two isolated real-snapshot proofs preserved full legacy serialized bytes and
  reduced the latest 19,216,342-byte envelope to 11,823,442 bytes. The generic
  defensive fast encoder cost 256 ms in that run and has no production speed
  claim. Measure the simpler producer path and integrated latency before
  accepting it. All earlier failed preflights remain failed.
- Generate package-local TypeScript adapters through the existing contract
  generator, so builds keep their current context and runtime dependencies.
  The shared source owns shape/omission rules and strict decoding. No DTO field
  allowlist, nested data rewrite, geometry change or new preference is added.

- The existing synthetic socket checks own their connection, timeout and cleanup
  in a small worker module. The monitor keeps scheduling, settings, persistence
  and alert ownership. Real loopback tests call that same connection path without
  importing the monitor's database and alert initialization.
- Missing or unknown request formats retain legacy behavior, including the
  existing first-query-value convention. Only an exact columns-v1 opt-in uses
  compact delivery. A malformed advertised compact response fails decoding;
  the frontend closes and reconnects before recording or hydrating that frame.

- The first public build exceeded the unchanged total JavaScript budget by
  376 bytes. Preserve that failure. Remove the decoder's redundant recursive
  validation of JavaScript-only nested values: callers immediately parse the
  producer's JSON, while the decoder owns the column structure. Keep every
  envelope, shape, index, width and prototype-safe reconstruction check. Native
  producer stringify converts nonfinite values to null. This contract does not
  promise rejection of exotic non-producer JSON such as 1e400, which JSON.parse
  itself turns into Infinity. The full ordinary producer-wire roundtrip remains
  covered; tests no longer pretend host-JavaScript undefined is parsed wire.
- Remove verbose browser connection console messages. The existing connection
  state and retry timer still own user feedback and recovery. No retry, jitter,
  delay, timestamp, error or server logging rule changes.

- The first owned socket run returned every expected row but exposed a new
  fixture expectation error: canonical identity responses use uppercase IDs,
  while the expected set used lowercase hex. Preserve the 8-pass/2-fail native
  result and correct only that expected set to the established identity rule.
  The new encoder, identity views, caps and privacy policy are unchanged.

## Weigh WebSocket snapshots without a monolithic serialization pause

- Fix: the WebSocket owner prepares the complete legacy JSON byte weight in
  cooperative row slices. A weak map supplies that number to the existing
  bounded cache; an entry without a prepared weight is refused with Infinity.
  - Hidden link: generic cache admission synchronously stringified a 19 MB
    snapshot during a background warmup, despite that snapshot not being sent.
    The retained history miss aligned with a 199 ms event-loop maximum and
    the broad cache insertion. This was a measurement lead, not proof of cause.
  - Shared cause: the raw viable-link cache also serialized its complete value
    before refusing it at its existing 16,777,216-byte bound. Apply the same
    owner-specific counter to both caches; do not change generic cache policy.
  - Surplus removed: neither admission needs a retained full JSON string or
    another encoded cache. The counter keeps ordinary row values and yields
    between rows, targeting 4 ms slices without claiming a hard row bound.
- Twelve same-object real-PG comparisons returned identical full weights for
  both caches, including the overweight raw link value. Initial snapshot
  heartbeat gaps fell from 125–135 ms to 4–6 ms. Raw link gaps also fell, while
  that isolated kernel used more CPU; end-to-end operations remain subject
  to their unchanged deadlines and matched regression limits. Six pure tests
  cover ordinary JSON semantics, full overweight counts, yielding and aborts.
- Preserve the 50,331,648-byte/128-entry/60-second initial cache,
  16,777,216-byte/50-entry/300-second viable cache, row contents, timestamps,
  generation keys and singleflight ownership. Await counting within the
  original fetch signal and before the final fresh privacy-generation check.
  Serialization failure retains Infinity refusal; cancellation reaches the
  fetch owner. Stop the existing warm timers when their server closes.
- The b034 preflight remains failed: history 278.577 ms and sustained host swap
  traffic exceeded unchanged limits. Correctness, logs, companion delivery and
  store preservation passed. A private counterfactual is not C8 qualification.

## Give PostgreSQL a whole-stack memory policy

- Fix: one root PostgreSQL service policy supplies the main defaults. The
  existing local DB extends it and keeps its deployment bindings; no additional
  service is launched. Larger deployments can override the documented GUCs.
  - Hidden link: the image tuned this 2 GiB container in isolation, reserving
    512 MiB shared buffers and 256 MiB maintenance memory while dozens of
    other services share this VM. Container swap is disabled, but that does not
    prevent host daemons from paging during overall pressure.
  - Ownership: the main application resolves an independently managed live
    data plane. Its resolver and external project remain unchanged. CI keeps
    its explicit 4 GiB/300-connection/24-worker settings; Beacon keeps its own
    database rules. Local overrides retain credentials, ports and fixture storage.
- Candidate defaults are shared_buffers=256MB, maintenance_work_mem=64MB and
  autovacuum_work_mem=32MB. Preserve max_connections=100, parallel-read policy,
  work_mem, worker capacity, durability and the existing 2 CPU/2 GiB/no-swap
  limits. No host swappiness, daemon configuration or stored PostgreSQL config
  is changed. CLI GUCs also apply to retained stores without reinitializing data.
- A read-only collector probe increased daemon major faults and host paging,
  but did not reproduce the failed traffic window. It is not proof of cause.
  Smaller shared buffers can shift memory into the kernel file cache and
  increase reads; smaller maintenance budgets can increase spill. Configured
  savings are not claimed as measured resident-memory savings.
- Independent source review preserves all existing config-test assertions and
  adds resolved service-set/default/override/CI-isolation coverage. Require actual
  settings, read/maintenance behavior, whole-VM resources and unchanged acceptance
  gates before qualification. Keep the failed b034 trial and its raw evidence.


## Price PostgreSQL scans for the shared VM

- Fix: the existing root PostgreSQL policy owns random_page_cost=4 and
  effective_cache_size=512MB, with explicit environment overrides. Keep the
  256MB/64MB/32MB memory defaults and every connection, worker, durability and
  resource bound. CI retains its independent database command.
  - Hidden link: the image's isolated-container initializer left random_page_cost
    at 1.1 and effective_cache_size at 1536MB. That cache estimate does not fit
    the same assumptions as the adopted 256MB shared buffers on this shared VM.
    The q18 index scan read 309,907 blocks and spent 44.5 seconds in the scan;
    the rematerialization side was empty and no hash or sort spill occurred.
  - Ownership: declare the planner estimates with the existing main database
    policy, rather than rewrite chart SQL, force a scan, or tune one caller.
    effective_cache_size is a planner estimate, not a memory allocation.
- The bounded same-worker, read-only repeatable-read comparison retained exact
  SQL, binds, generation20 and all eight ordered result rows (684 bytes). The
  direct old-cost call took 52.894 seconds; transaction-local 4/512MB took
  1.616 seconds. Subsequent actual plans chose the existing index at the old
  costs (32.297 seconds) and a sequential scan at 4/512MB (2.619 seconds).
  Fixed A/B order and the bundled settings limit attribution; this is not a
  chart-factory, HTTP or qualification pass. The earlier admission-unavailable
  attempt and all failed preflights remain preserved.
- Do not change SQL, privacy, chart freshness/history, queue budgets or any
  latency criterion. Require the existing config tests, actual adopted settings
  and full required qualification. Larger VPS deployments may explicitly
  override both estimates with the documented variables; no VPS action is part
  of this local change.


## Retain learning winners within each edge group

- The worker exhausted its 256MiB JavaScript heap at 21:29 on the retained
  10x fixture. Runs are serial and their heartbeat timers close; source inspection
  found no global model cache. The concrete surplus was keeping every scored
  adjacency candidate for every edge group until a later publication pass.
- Move the existing stable score sort and eight-choice slice to each group's
  insertion, then emit its stored winners. Every candidate is still scored;
  the Map's insertion/overwrite behavior, ties, equations, limits, privacy checks,
  dataset order, calibration, model hash and publication fences stay identical.
  The existing aggregate-key bound does not account for this temporary expansion.
- Five original-code oracle tests passed. One real all-scope, worker-read-only
  fixture snapshot produced identical 72,488,162-byte ordered outputs and model
  hashes for original512, changed512 and changed256 processes. Matched512 peak
  RSS fell from 444,152 to 412,024KiB; aggregation took 5.200 versus 5.576s
  (7.22% slower in this single diagnostic). Changed256 completed at 374,168KiB
  peak RSS. Heap allowance and RSS are different measurements.
- This proves output parity and current-heap completion for that input. It does
  not reproduce the native crash or certify the complete publication loop.
  Keep the native crash, Lighthouse failures and failed preflights in evidence;
  require successful resident learning publication and integrated qualification.
  No heap/container allowance or input limit is increased.


## Finish fixture preparation with current planner statistics

- Keep the early path/identity analysis that supports the facts backfill. After
  convergence and both backfills finish, analyze packets, packet paths and
  multibyte facts before releasing the existing heavy-work admission claim.
  Convergence can rewrite visibility and advance its generation; facts upsert
  that generation and packet identity backfill modifies packet rows afterward.
  The earlier analysis therefore does not describe the final bulk state.
- The existing real-PG fixture/chart handoff test checks that fact-generation
  statistics describe its fresh ready generation. Preserve all data, privacy,
  coverage, pool2, cancellation, chart SQL and timing assertions.
- This closes a fixture ownership gap. The failed22ddb99 preflight, chart-budget
  error and incomplete ingestion remain failed; fresh statistics alone are not
  claimed to explain every tail or replace required qualification.


## Keep scan estimates with the analytics pool that was measured

- Remove the two database-wide planner overrides. Main backend Compose explicitly
  supplies DATABASE_ANALYTICS_RANDOM_PAGE_COST=4 and
  DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE=512MB to its analytics pool only.
  Without those explicit settings, standalone callers retain server defaults;
  application names do not choose policy. HTTP, ingest and other workers keep
  their existing planner settings, pool counts, deadlines, credentials and schema.
- Validate the two estimates before constructing pools. PostgreSQL connection URL
  options override pg Pool.options, so preserve existing worker URL read-only and
  schema options while adding the explicit analytics estimates. Qualification
  forwards the same actual-consumer keys through its existing invocation owner;
  administrative fixture/oracle tools receive no new estimates from that owner.
- The q18 counterfactual proved identical rows and a faster analytical scan, not
  a safe global policy for ingest or OLTP. The failed22ddb99 run still had late
  ingest around eight packets/second after statistics caught up; global GUC
  causality is unproved. Preserve that failed run and all unchanged acceptance
  limits. Final fixture analysis and pool isolation need their existing native
  tests and fresh qualification; neither is a claimed cure for every tail.
- Keep the adopted256MiB learning bound unchanged. No new SQL, index, scan-force,
  pool, timeout, configuration selector or validation harness is introduced.


## Keep region analytical policy and fixture maintenance with their owners

- Reuse validated analytics connection options for the region tool's existing
  max2 pool. Its main worker explicitly receives4/512MB; HTTP, ingest and other
  workers retain server settings. Preserve URL read-only/schema options, all SQL,
  generation fences, statement120s/child180s and publication/admission ownership.
  A fresh-statistics post-preparation rebuild still exhausted180s with an active
  DataFileRead query and stale certificate. Actual completion remains required.
- Nine Timescale schedulers plus simultaneous midnight history-cleanup jobs
  exhausted16worker slots. The fixture owner assigns only verified owned
  main/owner databases distinct nonzero phases of their existing6hour cycle.
  Original jobs remain unchanged. Supported alter_job changes only initial_start
  and the next aligned future start; preserve complete job JSON, retention,
  retries, runtime, enabled state and fixed scheduling. No job deletion/disable,
  data removal or worker/cap increase is permitted. Repeating an aligned phase
  must not postpone it. Existing fixture CLI repairs retained jobs and applies
  the same owner after future creation. Native proof must execute this owner
  against real owned jobs, alongside meaningful ownership/refusal unit tests.

- Preserve the failed 13-case native run: 11 passed and the admitted-child
  cleanup case plus its parent failed. Child exit does not guarantee an immediate
  server-side session disappearance. PostgreSQL documents that with the default
  [client connection checks disabled](https://www.postgresql.org/docs/17/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL),
  disconnect detection waits for the next socket interaction, and
  [open transactions roll back on termination](https://www.postgresql.org/docs/17/protocol-flow.html#PROTOCOL-FLOW-TERMINATION).
  After releasing the test's blocking transaction, use the existing bounded
  cleanup wait before checking session/claim removal and the unchanged snapshot
  fingerprint. Keep the five-second child deadline and every production bound;
  a fresh successful native run is required.


## Release region rebuild inputs when their consumers finish

- Keep one read-only geometry record per located public node and reuse it across
  unattributed packets. The clustering implementation reads these records and
  constructs its own output; repeated packet membership needs references, not
  duplicate coordinates and names.
- Save packet counts separately, discard raw hex after decoding, and release
  source rows and intermediate maps after their last consumers. This removes the
  link between final publication/log counts and retaining the entire input graph.
  Preserve every SQL statement, read and iteration order, calculation, privacy
  fence, atomic transaction, capacity and deadline.
- The failed05205fa preflight remains failed. This reduces unnecessary retention;
  initial query materialization remains, and memory savings or effects on host
  swap and latency require measurement. No cache, worker or resource allowance is
  changed, and no additional validation harness is introduced.


## Observe operational-check refresh failures at the cache owner

- The health cache can return a stale snapshot while its replacement query runs.
  That detached refresh still needs a rejection observer; otherwise a checkout
  failure can escape the health caller and trigger application shutdown.
- Observe and log refresh rejection without replacing the original promise.
  Fresh reads, the existing stale interval, blocking error propagation and retry
  after settlement retain their current behavior. Keep SQL, privacy, deadlines
  and fallback policy unchanged. Cover these paths through the existing health
  runtime tests, without a new cache API or validation harness.
- Preserve the failed668dd8b read run and resident restart. Its retained stack
  does not identify the exact detached promise or establish the cause of pool
  starvation; closing this ownership gap does not claim to resolve those causes.


## Refresh RF support only in modes that consume it

- The link-only worker runs physical link jobs using its topology and calibration
  snapshots. RF support coordinates and link-capped coverage radii serve coverage
  and planned-coverage jobs, which the link-only queue loop never dispatches.
  Stop refreshing that unused snapshot at link startup and during its idle loop;
  retain both refreshes for viewshed and all mode.
- The support refresh computes complete ordered node/link checksum strings before
  checking whether its inputs changed, and then reloads those inputs on a change.
  The observed link-worker query was waiting on BufFileWrite alongside region
  work. Removing an unused consumer dependency avoids this work; it does not
  establish that this query caused the failed reads or shared pressure.
- Remove the unused support deadline from link idle scheduling as well. Otherwise
  an intentionally uninitialized support timestamp would force a 50ms wakeup.
  Keep topology, calibration, radio-neighbor and five-second lease-reaping
  deadlines, all physical calculations, coverage behavior and resource bounds.
  Exercise the actual mode loop and idle deadlines in existing worker tests;
  Root owns execution and fresh qualification.


## Aggregate region observations before expanding their paths

- The region query expanded each observation into its path hops before sorting
  raw packet payloads and deduplicating observers. Materialize the scoped
  observations once, aggregate raw/times/observers at observation cardinality,
  and aggregate only packet hashes and elements across expanded paths. Join the
  two packet groups in the same statement. Keep the shared network/privacy
  predicate, windows, latest-nonempty raw ordering, path80/observer40 limits and
  packet capacity unchanged. A small region module owns this SQL and row type;
  the tool retains its existing orchestration and atomic generation fences.
- The controlled comparison reduced temporary I/O by about 36% and completed
  faster, but failed raw-row byte parity. Retain that failed result. Its exhaustive
  follow-up compared 70,182 rows per query: no missing rows or nonraw differences;
  all 11 raw differences were observations tied at the latest eligible timestamp.
  The existing time-only ordering leaves those ties unspecified. Actual producer
  keystore/probes and decoder inputs, payload hashes and resulting regions matched
  for every difference. No tie-break rule or required test is waived.
- Raw is an intermediate decoder input, never a published region field; stored
  packet data is untouched. This evidence supports region semantic equivalence,
  not raw-byte equality or an integrated latency claim. Preserve all calculations,
  pools, deadlines, admission/publication ownership and fresh qualification gates.


## Compress full-page WAL images at the main PostgreSQL policy owner

- The failed9d2aa47 short run generated547,995,216 WAL bytes during1,800 ACKs,
  including its auxiliary and background work. A requested checkpoint overlapped
  the map outliers. This motivates reducing disk traffic without raising WAL or
  memory allowances; it does not establish the cause of every slow read.
- PostgreSQL17 documents lossless compression of full-page WAL images and their
  decompression during replay, with extra logging/replay CPU:
  https://www.postgresql.org/docs/17/runtime-config-wal.html#GUC-WAL-COMPRESSION.
  Root's read-only BEGIN/SET LOCAL/ROLLBACK capability receipt at
  final-evidence/read-failure-668dd8b-ttdkqi77/wal-lz4-capability.json confirms
  lz4 is supported by both current PostgreSQL builds. Both returned to off after
  rollback, with fsync, full_page_writes and synchronous_commit on throughout.
- Add wal_compression=lz4 to the existing shared main PostgreSQL command, with
  POSTGRES_WAL_COMPRESSION as an explicit normal-Compose override. DEV's clean
  environment uses the source default. Beacon's separate database command and
  CI's distinct policy remain unchanged. Preserve all pages, backups, durability,
  checkpoint settings, memory, CPU, worker and connection bounds.
- Only full-page images are compressible under this setting, not all measured
  WAL. The I/O benefit and CPU cost require the existing next qualification run;
  no latency or resource pass is claimed. Extend the existing Compose policy
  tests for the default, override isolation and unchanged CI/resources.

## Bound region sort scratch space at the region settings owner

- The 42698d2 short trial passed integrity and resources but failed map and
  observer latency. The unchanged region SQL still spills while reads run.
  Read-only, same-snapshot EXPLAIN evidence in
  final-evidence/region-sort-memory-42698d2-8bfqy38a/native.json measured
  97,381 temporary blocks written at 10,485kB, 64,075 at 16MB and 64,050
  at 32MB, with identical 71,082 output rows. The earlier syntax/container
  diagnostic failures remain retained. This is diagnostic evidence, not an
  HTTP qualification result.
- Give region rebuild sessions a bounded 16MiB work_mem default owned by
  regionRebuildSettings, with an explicit REGION_SCOPE_WORK_MEM_KB override.
  Keep the server, HTTP, ingest and ordinary worker defaults unchanged.
  The existing admission gate still permits one heavy task; the region pool
  remains two connections. work_mem is per operation, not a process cap;
  integrated resource and latency criteria still apply without relaxation.
- Extend the existing analytics startup-option merger to append trusted
  owner-supplied settings once, preserving URL precedence, schema/read-only
  flags, planner estimates and role selection. Avoid duplicating URL parsing
  or changing transaction snapshots. Preserve SQL, results, deadlines and
  publication/privacy fences. Check actual region sessions in the existing
  disposable-database integration test before rollout.

## Union regional paths within their packet rather than globally sorting them

- The 04c70b5 short trial passed all integrity/resource checks and drained in
  4.9s, but map342ms/history270ms failed250ms. Its slow burst coincided with
  PostgreSQL disk traffic and the region source query. The source query expands
  563,825 observations into3,336,436 path rows before sorting. The union belongs
  to each packet; the global expanded sort is surplus work.
- Retain native path arrays alongside each packet's metadata, then unnest and
  union only that packet's paths. Store each observation array in PostgreSQL's
  native text representation inside a text[] aggregate: this permits variable
  lengths and dimensions and round-trips nulls/escaping without adding a schema
  type or JSON conversion. Preserve DISTINCT ordering, the80-element cap,
  observers, latest raw selection, packet order and all privacy/window filters.
- Read-only same-snapshot/role/16MiB comparison in
  final-evidence/region-native-array-paths-04c70b5-fht815y1/native.json measured
  temporary writes64,139→36,655blocks and execution11.00→6.82s. Whole-array
  deduplication, element deduplication and ordered-input alternatives increased
  disk work and are discarded; preserve their diagnostic evidence.
- Raw-byte comparison still fails on the existing time-only latest ties. Its
  exhaustive follow-up in
  final-evidence/region-native-array-semantic-04c70b5-079bm8bm/native.json
  compared71,982 rows each: no missing or nonraw differences; all11 raw
  differences are eligible latest-time ties with identical actual producer
  decoder inputs and region results. Retain the byte failure and the existing
  unspecified tie policy. Raw remains intermediate and stored packets unchanged.
- Keep pools, memory bounds, admission, deadlines and publication fences. Run
  existing backend and real region regressions, independent review and a fresh
  integrated trial; these query diagnostics do not establish qualification.

## Remove the former shared region input's materialization fence

- Per-packet path aggregation leaves scoped_observations with one consumer.
  Its forced MATERIALIZED fence was needed by the former two-branch query,
  and now writes a temporary copy that is never reread. Remove only that fence;
  the PostgreSQL statement snapshot and canonical privacy checks remain intact.
- Read-only same-snapshot actual worker comparison in
  final-evidence/region-single-consumer-9ed7b6d-p38paw9q/native.json passed exact
  ordered byte equality for all71,982rows, including raw. Temporary writes fell
  36,655→18,181blocks; reads stayed18,171. Preserve the previous raw-byte
  failures and their scoped semantic resolutions; do not generalize this exact
  comparison into a new latest-time tie rule or an HTTP performance claim.
- Preserve all query parameters, fields, caps, ordering, memory, admission,
  deadlines and publication/privacy fences. Verify the compiled SQL, existing
  region checks and real producer tests before rollout and fresh qualification.

## Keep the node lookup heap local without another write-heavy index

- The six-window10x read collection on6f362fe failed its final candidate
  C8/warm trial1 map page at565ms p95/813ms p99. All earlier candidate modes
  passed and all13 resource checks passed over1,384.8s. Preserve that failure;
  neither sampling cadence nor acceptance changes. Its timeline shows the
  fixture observer probe reading packet history before the map's cold reads;
  region-worker CPU was idle during this particular failure.
- The ordered normalized lookup touched1,514 buffers for1,022 heap rows.
  Heap/key correlation was0.0034. Existing migrations148/150 deliberately
  removed an extra map index to meet the ingest WAL allowance. Avoid adding a
  covering index: it would add write cost and indexed unbounded text can reject
  previously valid node values. The hidden dependency is physical heap locality.
- Guarded owned10x CLUSTER using the existing normalized index took1.74s,
  preserving all49,542 rows by SHA256 and every index definition. Heap bytes
  fell21,553,152→14,204,928 and key correlation became1. The action committed
  once; its wrapper's JSON parse failure is retained separately from the
  successful native action and43-service/37-log closure. Evidence:
  final-evidence/owned-node-cluster-6f362fe-3zqn2_e5/closure.json.
- The existing candidate window then passed all4 modes/3 trials, with C8 warm
  p95≤74.69ms and13 resource checks passing. All36 compared HTTP case/trial
  data digests and entry counts matched. A separate same-SQL/parameters cold
  map probe after the same regional read needed103 disk blocks/~11.6ms,
  compared with1,137 blocks/162ms before clustering. These are diagnostic
  results, not fresh paired qualification. The original stores are untouched.
- Put the populated-store upgrade in migration153 under the existing finite
  db-migrate owner. Keep5s lock and60s statement bounds and atomic rollback.
  Later DEV bulk loads belong to the existing sample preparation finalizer,
  including scaleLookupFixture which already calls it. Finalize node layout
  there on the existing admitted/pinned maintenance session, in its own bounded
  transaction; never in an HTTP handler, retention tool or background loop.
  An empty-table migration alone does not organize rows loaded later.
- This is one-time upgrade plus explicit bulk finalization, not perpetual
  physical ordering. Preserve row values, schema/roles, indexes, freshness,
  pools, resource limits and privacy fences. Main configuration must own this
  maintenance before any fresh matched evidence is collected at either scale;
  fixture-only conditioning cannot substitute for the final runtime.

## Protect interactive buffers from the admitted regional scan

- The 488ea22 short trial passes the default map at 201.6 ms but fails history,
  observer map and packet detail during regional source I/O. Integrity and all 13
  resource checks pass; retain the failure. Pool waits are below 100 ms and the
  execution histograms include the SQL outliers. Node locality remains useful
  but does not protect packet/history pages from the shared scan pressure.
- Actual main PostgreSQL is 16.11. Its tableam.h starts bitmap scans without
  SO_ALLOW_STRAT, while heapam.c permits bulk-read rings for sequential scans
  only above one quarter of shared_buffers:
  https://github.com/postgres/postgres/blob/REL_16_STABLE/src/include/access/tableam.h
  https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/access/heap/heapam.c
  With 256 MiB, multiple 34–60 MiB chunks use normal buffers. At 128 MiB they can
  use the bulk-read strategy. This explains a mechanism to test, not a claim
  that every observed delay has a single cause.
- Lower only the shared main PostgreSQL buffer default to 128 MiB, retaining its
  explicit larger-VPS override and separate CI policy. Discourage bitmap scans
  only in the regional rebuild pool, owned by regionRebuildSettings and its
  existing startup-option merger. REGION_SCOPE_BITMAP_SCAN=1 restores them.
  HTTP, ingest, owner and other analytic pool planner flags remain unchanged.
- Bitmap-off does not guarantee sequential plans. Inspect actual regional
  plans/session settings and pg_stat_io normal/bulk-read activity; those counters
  aggregate client backends and must not be attributed solely to the region job.
  A smaller buffer cache can increase other misses: existing matched reads and
  operations, plus unchanged short/full workloads, decide acceptance.
- Preserve locality maintenance, all pools, deadlines, durability, response
  caches/freshness, history, algorithms, cadence and criteria. No resource cap
  increase or new runtime service/framework is introduced. Check defaults and
  overrides, actual regional sessions and producer/privacy behavior before
  canonical rollout and fresh qualification.

## Share the statistics summary's identity snapshot

- The unchanged52 preflight passes1800 ACKs,43 useful services,65 application
  access checks and13 VM checks. Its formal10x native windows all pass, with
  candidate p95 maxima71.85/71.42/74.88ms. Retain the formal batch failure:
  baseline demand completed9600 correct responses but admission lateness329ms
  exceeded100ms. Do not change the workload, baseline, criteria or resource caps.
- A bounded read-only diagnostic reproduces the backlog and captures the
  overlapping SQL at09:59:46–49. Summary counts, topology scope and full state
  queries compete inside PostgreSQL. Low regional-worker JavaScript CPU does
  not identify SQL ownership; no regional fix follows from that observation.
  Evidence: final-evidence/read-demand-activity-52f3725-8awz1mjr.
- First remove the summary owner's repeated work: fetchStatsSummary currently
  evaluates core.node_identity_nodes four times under its existing two-query
  limiter. Materialize one narrow projection with the common network/observer
  scope, then derive stale/map/total/international results from it. Keep the
  four packet/longest-hop queries, limiter and public service contract intact.
- Predicates differ intentionally: international uses raw last_seen, separate
  latest latitude/longitude subqueries, existing country boxes and null SUM/MAX;
  map uses effective presence and existing role/coordinate/freshness rules;
  total has its own role/private-name exclusions. Preserve bigint strings and
  row shapes. Do not push one category's predicates into the shared scope.
- Establish real PostgreSQL behavior coverage before adjusting implementation-
  coupled SQL assertions. Compare old/new values in a pinned read-only snapshot
  on retained10x, then inspect untimed plans/buffers/temp before adoption.
  A materialized projection can still spill. Measure this one owner before
  considering topology or full-state changes. No cache age, history, pool,
  timeout, service, privilege, durability or mathematical contract changes.

## Reuse the canonical identity projection for topology membership

- The adopted f8 summary removes three repeated identity evaluations, but the
  unchanged baseline demand diagnostic still fails100ms admission:224ms max,
  all9600 responses correct, all13 resources passing. Retain that failure.
- At10:35:44–46, bounded activity separates native readers on172.18.0.1 from
  the normal API on172.18.0.10. The remaining topology DISTINCT identity query
  runs for1.35s alongside health/link/state work. Do not infer SQL ownership
  from JavaScript CPU. Evidence: read-demand-activity-f8a6508-jyty1ukp.
- The topology query uses the wide materialized canonical view even though it
  needs only identity/name/network. Reuse publicIdentityNodeSourceSql, already
  owned and tested by node reads: its singleton projection avoids computing
  irrelevant aggregates, with the complete alias/collision fallback preserved.
- A real HTTP-role repeatable-read probe preserves all49124 production and130
  test identities plus the empty-network case. Untimed warm plans write5984
  temporary blocks for the old source and zero for the existing fast source.
  Evidence: topology-identity-probe-f8a6508-m46_53_8. These are work diagnostics,
  not matched HTTP timings or a qualification pass.
- Change only the identity source of the strict network/no-observer branch.
  Preserve DISTINCT, network=ANY, name privacy, the separate full link scan,
  radio aggregation and endpoint intersection. Keep the comment documenting
  PostgreSQL's174M-probe constraint. Do not reuse getNodes IDs: its sightings
  scope is broader, and its result omits network. All other scopes, counts,
  deadlines, pools, caches, history and limits remain unchanged. Existing native
  projection tests already cover every canonical column, aliases, private names,
  missing canonical rows and legacy collisions; run that coverage for this reuse.

## Bound statistics work to one statement on the lean main runtime

- The f644 diagnostic still fails admission (1005ms,9600 correct responses,
 13 resource checks passing). Actual SQL identifies two backend-analytics
 observer-chart scans running concurrently for8–15s during the backlog. Keep
 the failure; topology membership work is reduced but is not this burst's owner.
- Exact emitted statements match the activity1023-byte prefixes. Both actually
 include privacy predicates, despite one requesting includePrivacy:false;
 preserve emitted behavior rather than changing that separate compatibility
 case. An HTTP/worker read-only probe gives exact ON/OFF results. With current
 work_mem10485kB, shared128MiB, planner4/512 and parallelism0, bitmap-off only
 slightly reduces the two untimed6.7/5.4s scans and leaves13655/6663 temp blocks.
 Do not extend planner policy to unrelated analytics callers on this evidence.
- Trial the existing STATS_DB_QUERY_CONCURRENCY setting at1 in main Compose,
 retaining analytics pool2 and explicit overrides for larger hosts. No new
 queue, connection pool, feature flag or SQL rewrite. Forward the same existing
 setting from the actual consumer to qualification tools so both source labels
 use the same resource profile. Main configuration owns the lean default.
- Queue wait counts toward the existing60s query budget and120s timeout.
 Preserve those limits, HTTP pools, stored-snapshot freshness and warmup cadence.
 A quiet cached cohort is inconclusive: require a complete chart refresh to
 overlap demand and publish, and verify cold/observer work and matched operation
 deadlines/regression. If serial work violates them, fix work instead of raising
 limits. Configuration is a candidate until integrated qualification passes.

## Compile the regional transport probes once per rebuild

- The current six matched native windows pass, but the first fixed-demand
  baseline cohort fails admission (2748ms,9600 correct responses). Preserve the
  failed batch and all short failures. The normal chart refresh completed at
  12:24:41, before demand12:28:47–12:30:47, so it is not this burst's owner.
  Regional application work uses23.36CPU-seconds during29.48wall seconds near
  the backlog. A separate1Hz diagnostic captures learning upserts during a
  different860ms backlog; reducing regional CPU does not establish that every
  other background cost is resolved.
- A read-only CPU replay of6000 existing captured wire fixtures spends16.66s
  of18.37s sampled time in decodeRegionForPacket:12.44s in transport-code HMAC
  calculation and2.08s in repeated region-key derivation. Packet compatibility
  decoding itself takes0.65s. Evidence: final-evidence/
  region-decoder-profile-6a9d0a0-cXw0O5/inclusive-summary.json. This diagnostic
  isolates work on four fixtures; it is not a whole-corpus or HTTP benchmark.
- The regional decoder owns this repeated work. Compile its fixed probe names
  once per rebuild and derive each reached key once. Parse payload hex once with the
  upstream parser and calculate the identical HMAC-SHA256 with Node crypto.
  Keep upstream case/#/UTF8 key derivation, first-match order and returned name,
  payload-type byte masking, little-endian uint16, reserved-code nudges and
  input/error behavior. Keep the framing/compatibility decoder unchanged.
- Use run-local probe data, with no packet cache, new concurrency
  setting or global state. Retain the external firmware equation comment.
  Establish upstream-reference behavior coverage on the old implementation
  before adopting the compiled path. Compare real retained-corpus outputs and
  native CPU work before rollout, then repeat unchanged qualification. Pools,
  cadences, history, privacy fences, publication, deadlines and criteria stay
  unchanged; the learning write cost remains separately observable.
- Independent review identified malformed UTF16 names as an error-ordering
  constraint: eager derivation can fail before an earlier successful match or
  a payload hex error. A retained negative test reproduces this. Derive keys
  lazily only when reached, preserving short-circuit and missing-input behavior.

## Reuse the identity projection inside routine summary counts

- The deployed compiled regional probe preserves all75582 packet decisions and
  reduces the captured6000-frame replay from18.35s to1.63s. Its next baseline
  demand cohort still fails328ms admission with9600 correct responses. Keep
  this failure. No chart, regional or learning job explains that window.
- At13:11:33 the capture observes Health Check's exact30-day path count doing
  IO, followed by normal API summary/full-node reads. PostgreSQL uses9.76CPU
  seconds in5.30wall seconds, reads620MB and is throttled2.12s. These are
  shared-container counters, not evidence that Health Check caused all reads.
- A worker-role read-only plan comparison preserves the exact623552 count and
  the other capacity values. Ordered-time and COUNT(time) variants do not
  improve the existing narrow count plan. Leave Health Check unchanged.
- The summary still projects nine fields through core.node_identity_nodes, a
  regular view with an internally materialized wide CTE. A pinned HTTP-role
  comparison gives exact summary values for global/UK/empty/observer scopes.
  Reusing publicIdentityNodeSourceSql removes5976 temporary write blocks;
  untimed execution is553ms versus133ms. This work diagnostic does not prove
  HTTP latency or resolve every source of contention. Evidence:
  final-evidence/routine-read-plans-33e5ae8-v2-vnuddw7k.
- Replace only that shared summary source with the existing canonical identity
  projection. Preserve its materialized nine-field scope, category predicates,
  output types, other four queries, caches, pools, budgets and refresh cadence.
  Establish native alias/collision/private-name/missing-canonical coverage on
  the old implementation first. International latitude and longitude retain
  their separate last_seen-only selections: test eligible tied values without
  adding a tiebreaker or combining their rows. No new settings or indexes.

## Read Health Check's link activity in one statement

- The deployed summary projection removes its wide temporary work, but the
  next9600-response cohort still fails267ms admission. Preserve that failure.
  Its capture overlaps exact path counting and full-state link/node reads;
  BufferMapping waits show shared buffer contention, not row or DDL locks.
- Current topology uses the stored node_identity_links materialized view
  (migration051, moved by112), not the older regular definition in037. Its
  untimed plan reads11550 blocks; the radio-report input is empty here. Do not
  rewrite that graph source or weaken its publication/scope semantics.
- Health Check's currentWorkers independently asks for recent link count,
  latest computation and backfill count/latest observation. All three scan
  core.node_links and are consumed together. A real-worker read-only snapshot
  preserves all four values in one aggregate. The old plans read22008,21944
  and21880 blocks; the shared plan reads21816. The separate COUNT/MAX variant
  did not help and is rejected. Evidence: final-evidence/
  health-link-shared-plan-673f704-x1iq0anv and full-state-link-plans-673f704-ebzv4kz_.
- Let the health repository own one loadLinkActivity query and remove its three
  surplus methods. Runtime maps that single row to the same worker fields.
  Keep COUNT(*) over every raw link, strict itm_computed_at > NOW()-1h,
  independent MAX timestamps, text/count types, empty zero/null results and
  future timestamps. Establish old behavior on an owned native fixture before
  changing the query and its internal test adapter.
- Keep the remaining Promise.all work, error propagation, defaults, liveness
  logic, metrics, freshness and cadence unchanged. One statement provides one
  snapshot/cutoff; add no cache, settings, index, transaction or new service.

## Give the WebSocket privacy index its own narrow read

- The deployed shared Health Check link scan removes two redundant scans, but
  the next demand cohort still fails:9596 correct responses,4 not admitted,
  maximum admission delay154ms. Preserve the failure and resource/log passes.
  Near its worst delay the capture includes two full getNodes reads and health
  overview work on the HTTP pool. This identifies overlapping work, not proof
  that either alone caused the delay. Evidence: final-evidence/
  chart-demand-6d9b445-uutdjshm.
- PublicWsPrivacyIndex.replace reads only node_id and name; its refresh borrows
  getNodes, coupling privacy maintenance to map fields, presence calculations,
  sorting and wide row transfer. Keep canonical identity and network membership
  under the existing repository helpers. Add a two-column privacy read using
  the same publicIdentityNodeSourceSql and buildNodeScopeClause. Do not filter
  private names, map roles, coordinates, ages or suppressions. The complete
  private identity set, including aliases and recent sightings, remains needed.
- Establish the current full-read projection as a native behavior oracle before
  changing the consumer. Cover aliases, missing canonical and legacy collisions,
  public/private names, non-map roles and invalid coordinates, test isolation,
  recent and exact30-day sighting boundaries. Compare unordered ID/name sets;
  the privacy index owns sets and does not use map ordering. Keep its parallel
  network reads, shared in-flight promise, atomic replacement, error handling,
  refresh cadence and all public map/read contracts unchanged. Add no cache,
  settings, index, service or privacy-rule copy.

## Index the shared viable-link access patterns

- The smaller privacy projection preserves all identities and removes its sort,
  but the unchanged baseline cohort still fails:9600 correct responses, no
  errors or undispatched requests, maximum admission delay372ms. Preserve that
  failure. Its worst scheduled arrival is14:45:46.315UTC. At14:45:44.991 the
  captured link-worker pair scan has spent776ms with DataFileRead, while the
  normal API viable-link materialized-view scan has spent829ms. Concurrent
  timed map reads use the existing fast paged identity projection. These are
  overlapping shared-storage costs, not sole-cause proof. Evidence:
  final-evidence/chart-demand-d6e99d6-_628pw_k.
- Catalog inspection finds only endpoint and unique-pair indexes: the441003-row
  raw link heap is190MB and the identity-link materialized heap95MB. The latter
  is almost entirely all-visible. Both consumers explicitly select the same
  viable-or-forced subset, about99510 rows, but currently scan the broad heaps.
- Test a partial raw pair index and a partial covering identity-link metrics
  index for the exact existing viability predicate. The database owns this
  access path; leave graph queries, identity publication, privacy, scope, cache
  cadence, math, worker lifecycle and resource settings unchanged. Do not
  include changing metrics in the raw pair index. Account for index storage
  and materialized-view refresh/write cost.
- First retain actual application-role read-only plans. Compare exact rows and
  plans before/after indexes in a repeatable-read administrative transaction on
  the explicitly owned10x fixture and roll back all DDL. Role-switched prototype
  queries model planner permissions only; they do not prove authentication or
  application capabilities. Adopt migrations only if reduced work and behavior
  parity are demonstrated; then recheck actual deployed roles and qualification.
- The two-index rollback prototype preserves full rows and the actual bounded
 20000-link seed, reducing read blocks and avoiding both broad scans. Its
 real Python32-pair write/tri-state/refresh trial, however, increases median
 isolated write time10.838ms to13.109ms (20.96%). Retain this adverse result
 and reject the raw-table index from this stage. Do not call this a performance
 pass or offset it with read gains. Evidence: viable-link-index-write-refresh-
 d6e99d6-5195sz5g.
- Repeat the identical AB/BA/AB workload with only the materialized-view cover.
 All controlled values and returned fields match, all DDL/data changes roll
 back, and prototype indexes are absent afterwards. Median write time is
 12.597ms versus11.592ms and concurrent refresh6262ms versus6250ms. These
 small stage timings establish no observed median penalty, not integrated
 acceptance. Cluster WAL insert-LSN deltas include unrelated writes; statement
 EXPLAIN WAL is reported separately. Evidence: final-evidence/
 viable-link-mv-only-write-refresh-d6e99d6-8m_hblfu.
- Adopt only the partial metrics cover on core.node_identity_links. Keep raw
 link storage/indexes unchanged and retain the unconditional unique pair index
 and endpoint indexes. Recheck final-scope read/seed parity, native migration,
 tri-state transitions, concurrent refresh and actual deployed roles before
 the unchanged full-stack trials. All original criteria remain binding.

## Use spare CPU capacity at the shared PostgreSQL boundary

- The published-view cover reduces its work, but the next9600-correct cohort
  still fails129.858ms admission. Preserve that failure. Five-second samples
  show1.3-1.7 host cores idle while the shared database hits its two-CPU quota
  in bursts. The interval preceding the worst arrival has822ms of throttling
  and525MB of read IO; this does not establish a CPU-only cause. Evidence:
  final-evidence/database-cpu-headroom-3337722.json and the independent proposal
  review02420be36d1baf79d2e815df47c7cf82460ad234258d02c61156568bb01f7a96.
- A reversible live2-to2.5-to2 quota experiment keeps the same database process,
  images, sources, targets, credentials, settings and backups. The unchanged
 120-second/80RPS native cohort passes9600 correct requests, no errors or
  undelivered requests, maximum admission56.563ms and all interval/span checks.
  Resources and logs pass. The original quota and entire prior context are
  restored afterwards. No chart refresh overlaps; this is not final acceptance.
- The private diagnostic collected outside the qualification evidence root, so
  its successful native run then fails the existing ownership validator. Retain
  that failure. Copy the raw files byte-for-byte into an owned run directory
  and audit unchanged data, identities, clocks and companions with the same
  strict validator. Only two pointers in a new audit record change; original
  false/error records remain intact. The closure passes with958 in-window reads
  per companion and no failures. Evidence: qualification/
 20261114T010853Z-y87xc51j/cpu25-owned-evidence-closure-paxov5c6. Future private
  read diagnostics must allocate beneath the existing qualification root.
- Put the bounded2.5 default in docker-compose.postgres.yml, the existing shared
  resource owner. Add POSTGRES_CPUS for explicit deployment overrides. Preserve
  every pool, connection reserve, memory/swap ceiling, parallel-worker setting,
  buffer setting, command, binding and durability value. Local clean-environment
  launchers use that default; no second setting or application-specific scheduler
  is introduced. No VPS action is authorized by this configuration change.
- Resolve real Compose inheritance and explicit CPU overrides while preserving
  all other service fields. Keep the CI infrastructure policy independent. The
  current trial supports adoption work, not a final performance result: fresh
  matched full-stack trials, ten-percent limits, recovery and full40 remain
  required under the final source/profile.

## Keep companion qualification fixes with their owners

- CoreScope's unchanged browser suite exposed a mobile VCR overflow. Its mobile
  control gap lost to the desktop selector's specificity, and a single row
  cannot preserve the existing controls, clock and timeline at320px. Fix the
  selector at its owning breakpoint and use two rows below370px. Keep the
  existing height observer, touch targets, theme, data and playback logic.
  Remove obsolete padding-workaround commentary; retain external viewport and
  touch constraints. Adopt CoreScope4dce32f only after its reviewed15-context
  comparison and unchanged full browser suite:152 passes and one formal skip,
  with additional existing manual/early-return limitations retained explicitly.
- Beacon Web's full suite passed1660 tests but failed on an unhandled TanStack
  scroll-end timeout after jsdom removed window. The test file did not own its
  timer lifecycle. Let that file use a fake clock, unmount and drain callbacks
  inside act, require no pending timers, and restore real timers in finally.
  Preserve all original test bodies and assertions. Adopt test-only68ce55a;
  focused18 tests, fresh build/lint and all1660 tests pass. No application,
  dependency, global test policy or configuration change belongs to this fix.
- The subsequent private proxy probe mistakenly targeted CoreScope3102 rather
  than the Beacon3103 endpoint declared by the companion inventory. Preserve
  the failed combined attempt. Correct only the private helper endpoint and
  retain a separate11-case transport proof; do not convert the earlier failed
  receipt into a pass. Beacon's origin admission policy remains unchanged.
- Companion build inputs include their committed revision. Rebuild and publish
  through the existing component owners, then bind new qualification evidence
  to the actual source, artifacts and runtime. The shared binary-directory
  fingerprint can also change Beacon's computed context when Core is rebuilt:
  account for its exact directory delta and prove Beacon's own binary, image,
  environment and configuration remain unchanged. Web's test-only change must
  preserve served application bytes, apart from its source-revision marker.
- Beacon's current native build, formatting, vet, units and selected race
  checks pass. Database-gated skips retain their separate native evidence.
  Govulncheck exits0 with three module-level findings retained in its raw JSON;
  do not describe this as zero advisories. These checks and the companion
  browser results are component evidence, not integrated qualification or
  remote CI. Keep every earlier failure and all acceptance limits.

## Use Beacon's declared toolchain for formatting checks

- The first real Beacon CI run builds successfully but fails formatting one
  test file. Its merge tree exactly equals the reviewed head. CI follows
  go.mod's Go1.26.10; the local Go1.27.4 formatter accepts different map-key
  alignment. A successful check with the newer formatter is not evidence for
  the declared CI formatter. Preserve both actual results.
- Verify the official Go1.26.10 archive and extracted formatter, then apply
  only its three whitespace alignment changes to the existing index-drop
  regression. Keep every key, assertion, function body and other line exact.
  Adopt Beacondf46000 without changing its runtime source, dependencies,
  declared Go version, pipeline or test gates. Its declared-formatter check
  covers the complete repository; the CI retry also passes build, formatting,
  vet and units before continuing the PostgreSQL checks.
- Beacon's source fingerprint includes its test file and committed revision,
  so its source marker, API revision environment value and Compose hash must
  update during the canonical rebuild. Its executable must remain byte-exact;
  the migration job has no source-revision environment value and keeps its
  configuration hash. Account for those precise metadata changes in the
  adoption proof, retaining all existing resource, database and RF guards.

## Keep Beacon's compiler contract in its repository

- The backup CI lane uses a separately pinned Docker builder. Its Go1.26.7
  compiler rejects the declared1.26.10 minimum with GOTOOLCHAIN=local before
  downloading modules. Verify that rejection using the exact image and module
  files. A compatible1.26.10 pin then passes the image build and both PostgreSQL
  backup tests; retain the original failed run.
- The current vulnerability check subsequently rejects Go1.26.10 for
  [GO-2026-6859](https://pkg.go.dev/vuln/GO-2026-6859). Set the owning go.mod
  minimum to1.26.11 and pin the matching official Alpine3.24 builder. Keep
  go.sum, dependencies, application code, runtime image, PostgreSQL16 client,
  scanner and every test unchanged. The local canonical compiler is already
  Go1.27.4, which the advisory lists as fixed. No VPS deployment is involved.
- Reproduce the deployed Beacon binary with the canonical Go1.27.4 compiler,
  source archive and trimpath/build flags, then change only the module minimum
  in the same source directory. The baseline matches the deployed SHA exactly.
  The candidate differs by59 bytes in its Go/GNU build identifiers; executable
  code and all other sections match. Adopt only the exact measured candidate
  SHA4135d16c2258dfd335f984b39875f5bd0ea3d0f672c5fde01467a835125f0e69.
  Keep both Core binaries and all Core/Web artifacts byte-exact in this stage.
- Docker serializes independent bind mounts and dependency-label members in
  varying orders. Compare their exact members at capture, retaining raw order
  for audit, so representation changes cannot masquerade as settings changes.
  Preserve source/destination/mode and dependency conditions. Check every other
  configuration, environment and ownership-label field exactly, allowing only
  the declared source metadata and shared App image revision transitions.
- Preserve all three failed CI runs and the original failed adoption collector.
  Native compatibility and artifact proofs do not replace the pending full CI
  retry, final source bindings or integrated qualification. Keep all existing
  resource, database, backup, RF, permission and performance criteria.

## Qualify the remote GPU default on the four-core DEV VM

- Give the coordinator one remote-only compute policy for scheduled coverage
  and on-demand footprints. DEV currently clears the broker address and the
  chunk dispatcher can fall through to local terrain and propagation. Preserve
  the model, scheduling, checkpoints and published output while making a
  disconnected worker an observable, recoverable job failure.
- Validate effective PostgreSQL service policies at the existing inspection
  boundary. The guard rejects the shared service inheritance before it can
  check the inherited floor, and Beacon's capacity is below that floor.
  Keep the actual Beacon container memory limit fixed.
- Use the migration loader's names and checksums to check both replay ledgers.
  The tests maintained a second migration head and now fail before exercising
  the preserved grants and legacy schema moves.
- Classify known fixture-only scan findings at their owning file and exact
  value, keeping full-history scanning. Fix the affected frontend dependency
  at the root of its graph, with a reviewed and pinned source backport if
  upstream has no suitable release. The full audit remains required.
- Compare feature contracts against GitHub main and the merged DEV revision
  separately. Use the same fixtures for matched performance runs and keep
  the database and RF artifacts owned by the existing lifecycle boundaries.
- Reuse the loopback disposable database helper for compressed replay and run
  it with TimescaleDB. The plain PostgreSQL audit job has a different schema
  contract. Exercise the real launcher lease in the operator script test and
  stub only the fixture and container inventory boundaries.
- Bound Chromium context setup separately in the production basemap tests.
  The CI trace spent 32 seconds creating a context. Keep the recovery
  deadline, assertions and normal pointer action in the test body.

## Reduce buffer reads on 10× search and observer maps

- Give current name, raw ID and IATA candidate discovery one indexed text
  expression. Three GIN probes repeat work on the same index. Preserve the
  canonical-ID and former-name candidates and exact matching after identity
  hydration. Keep the earlier index usable by the sealed baseline and by
  clients rolling back to earlier builds.
- Let observer scope use a covering index on publicly eligible packet flags
  when the current visibility generation is fully materialized and the packet
  rematerialization queue is clear. The current query discovers sources from
  the same index and then revisits every source to prove visibility. Reuse the
  original packet-level checks during rematerialization and preserve the
  observer's own identity, network scope and seven-day membership rules.
- Verify identical rows before comparing timings. The single-scan experiment
  with the whole privacy predicate lost SkipScan and read the packet heap.
  Keep the separate indexed proof during recovery and measure the extra index
  writes under the existing full ingest workload.

## Keep retained CoreScope history independent of cache size

- Give the retained packet reader ownership of pagination, totals, and page
  hydration under one SQLite snapshot. The old time-only fallback treats
  unbounded queries and late observations as cache-complete, skips radio
  filters, and can silently return partial rows after a database error.
- Reuse the packet filters and the page formatters at the history boundary.
  The old fallback has independent definitions of channel and node matching
  and a second set of response fields. Resolve linked metadata once per
  request and return errors before sending a partial result.
- Move per-observation radio data to the observation read path. The global
  map duplicates SQLite and grows independently of packet eviction. Keep the
  radio-stint fallback used by single-radio observers and earlier schemas.
- Keep the DEV cap off until retained packet details, VCR and history consumers
  pass with actual eviction. The cap cannot safely turn a cache miss into
  a historical miss.

## Record MQTT qualification outcomes at their owning stages

- Save publication, delivery, drain and final reconciliation evidence as the
  corresponding work completes. A later SQL timeout currently hides timings
  and counters from earlier work, even though the failure receipt shows the
  exact stage.
- Use the existing matched-read validators to close the MQTT-first profile.
  Keep fixed 80 RPS and mixed-read latency visible as advisory and bind all
  required phases to the same source, runtime profile and intended fixtures.

## Attribute swap without changing the qualification gates

- Count qualification processes separately from resident services. Host swap
  counters show movement but cannot identify the process or tell whether a
  process moved swap traffic and paged back in between samples. Record
  harness PSS and SwapPss alongside the existing resident and host evidence.
- Measure the selected CoreScope SQLite store and WAL during the workload,
  and capture PostgreSQL wait classes without query text. The previous
  evidence combined disk I/O and backend waits into broad counters while
  two SQLite processes shared one retained store.

## Prove exact timed drain through committed parent keys

- Reuse the parent-key checks inside the timed drain and keep the global hash
  audit for final independent reconciliation. The drain and final checks
  currently repeat the same global scans of path history and the ledger.
  Check each hash and reception count during drain so missing packets cannot
  cancel out duplicate observations in aggregate totals. The final global
  scan still detects orphan children outside the publication window.

## Clone retained fixtures through their existing ownership gate

- Copy an unused, validated fixture under the target publication lease and
  verify its SQLite snapshot. The restore path creates a new logical fixture
  from backups, which cannot reproduce the enlarged CoreScope history and
  already-converged PostgreSQL baseline together. Keep source fixtures and
  original stores available for comparison.
- Check free space against all three PostgreSQL stores, SQLite, and a write
  allowance before creating any database. Use PostgreSQL's file-copy
  strategy. Revoke PUBLIC access only on new databases, then let the existing
  selected-target switch grant application roles. Provisioning during clone
  creation would revoke access from the currently selected pair.

## Warm up an actually public packet detail

- Select the soak's sample packet through the same network scope and privacy
  predicates as packet detail. The two materialized flags alone can select a
  hidden or differently scoped packet after fixture preparation. Preserve
  the required HTTP success for all eight routes.

## Keep replay preparation from holding two decoded CSV tables

- Reuse the streaming CSV reader for replay inputs. The publisher needs the
  raw-frame identity lookup and the ordered publication schedule, but it
  currently retains both expanded source tables while building them. Keep
  arbitrary input order, original filters, and the same QoS and rate.

## Remove whole-history bookkeeping from the RF startup pass

- The resident and retained global readers already visit one transmission per hash. Preserve the existing regional observation traversal and scope behavior, and keep only per-transmission hourly deduplication in the global reducer. The previous shared reducer allocated regional hash sets for the default startup and concatenated each hash with its hour.
- Record running sums and extrema for RF time and payload-type groups. Those responses do not need a second copy of every SNR value. Keep the full inputs to the exact median, histograms, and existing scatter sampling.
- Establish startup memory and response parity on the verified 10× SQLite copy before changing the packet-store default. The prior heap sample attributed about 99 MiB to the active RF aggregation.

## Settle owned qualification processes before restoring a fixture

- Run the workload in an owned process session whose supervisor remains available during cancellation. The previous Bash trap and workload shared one signal group, so restoration raced inherited target-lock descriptors.
- Make the soak's HTTP-reader ownership recoverable before launching Docker. Docker owns the detached helper even if its CLI is interrupted. Use the run's ownership receipt for cleanup after process settlement.
- Preserve canonical target publication and require the exclusive lease before restoration. A bounded wait lets short nested readiness sessions release inherited descriptors after their own cleanup.
- Defer cancellation during the canonical selection and start. The controller must close the pending intent before the fixture can be changed again. Keep disk observations in memory until startup settles, then skip the workload and restore if the reserve was lost.

## Separate allocated disk occupancy from bytes written

- Record allocated SQLite bytes, PostgreSQL WAL and temporary file residency, and the selected database size alongside existing cgroup and cumulative counters. The late 10× write burst showed that a WAL position and temp-write counter cannot explain current free space.
- Keep the minimum disk reserve in the qualification common settings so the host report, fixture copy and guarded workload use the same threshold.

## Attribute map and search database time in their native read windows

- Sample PostgreSQL statement counters immediately before and after each read mode. Bind them to the selected database and HTTP role. Use the existing node-map query annotation and record only query IDs, known shapes, call counts, execution time and block counters.
- Keep counter observation outside the native HTTP intervals. Leave the current thresholds and response validation in the benchmark. Counter resets, evictions and unavailable telemetry must remain visible in the evidence.

## Keep hop context from retaining unrelated decoded payloads

- The retained 10× startup heap includes about 81 MiB of full decoded maps populated by topology. Hop disambiguation reads the sender pubkey alone.
- Share the existing decoded cache for packets with a possible key. Exclude JSON that cannot contain the exact property before opening the cache, including a fallback for escaped keys. Preserve every hop and all decoded packet fields.

## Shrink the repeater enrichment snapshot

- The larger retained store reached complete packet load and then exhausted the 512 MiB cgroup during the synchronous enrichment prewarm. Keep one compact instant and the advertisement flag per indexed transmission. The reducer compares instants without losing offsets or fractional seconds.
- Capture sparse transported scope names alongside the other fields while holding the existing store read lock. Preserve the separate scope attribution and time-window rules.

## Serialize CoreScope's high-memory background passes

- Share one cancellable work slot between analytics recomputes and repeater enrichment. Their startup passes could allocate whole-history scratch data at the same time. Keep the individual refresh intervals and response caches.
- Let waiting work leave on shutdown. Signal every analytics worker before waiting for their completion.

## Keep statistics complete when the packet cache is limited

- Count transmissions and logical observations in one SQLite snapshot when the resident index may omit history. Use the transmission index for zero or one observation, then group repeated observations by their exact observer and path for that transmission.
- Keep the current physical observation counts for the last hour and day. Exercise both SQLite schemas and keep the resident path for a complete cache.

## Read direct reception evidence across the retained history

- Node health uses a background direct-reception snapshot. A bounded packet cache can omit evidence from that snapshot even while SQLite retains the observations.
- Feed the same radio evidence reducer from a complete resident store or from bounded SQLite batches. Keep the prior published snapshot when a retained read fails.

## Limit neighbor-graph decoding to authoritative origins

- The full 10× stack passed startup at 512 MiB. CoreScope was OOM-killed just after the first scheduled neighbor-graph refresh began. The startup heap profile showed that graph construction was populating decoded payload caches even for packets that can only contribute last-hop edges.
- Read origins for advertisements and anonymous requests. Keep last-hop edge construction for every packet type and put scheduled graph construction behind the existing cancellable analytics work slot.
- Use sustained reads through the first full refresh to verify cgroup memory at the unchanged limit.

## Resume an inactive fixture consumer before restoring targets

- The first 10× read attempt left CoreScope inactive. Target publication correctly refused to switch until the previous consumer could be observed.
- Classify that refusal before publishing the selection. The fixture supervisor can start the still-selected owned fixture through the launcher and retry the original guarded switch once. Other refusals still require their own diagnosis.

## Wait for the maintenance fixture's final PostgreSQL listener

- Hosted CI stopped before the maintenance fixture executed its seed SQL. The fixture checked log text and probed a Unix socket, which is also used by the image's temporary initialization server.
- Use one bounded TCP readiness probe for the final server. The dry run, signed apply, and idempotent retry continue to use the existing maintenance command and safety gates.

## Plan broad node lookups with the supplied values

- The 10× comparison recorded 334–367 ms of average PostgreSQL execution for broad map and short-search shapes in the candidate. Forced generic plans reproduced about 410 ms of execution. With the same values, custom plans took 7–8 ms. JIT was already disabled.
- Send broad lookups without candidate IDs as unnamed statements so PostgreSQL plans for the actual scope, cursor and limit. Keep bounded preparation when the SQL first identifies candidate IDs. Both paths retain the complete identity projection and visibility checks.

## Select the native benchmark observer once per read window

- The exact seven-day observer ranking took about 18 seconds in a representative 10× mode. Running it in all 24 subprocesses consumed the collection deadline even after the candidate latency improved.
- The first mode selects the observer through the existing authenticated read-only session and owns a private receipt. The next three modes create fresh runtime sessions and use that same observer. The controller compares receipt hashes across modes and paired windows, and removes the private file when the window closes. The measured routes, samples, companion cadence and deadline remain the same.

## Reserve memory for the local Redis process

- The full soak reached Redis's 256 MiB cgroup limit and the kernel killed the process. Redis reported no internal limit. The live feed lost its Pub/Sub connection while PostgreSQL continued to commit every observation.
- Limit accounted Redis memory to 160 MiB and keep `noeviction`. Leave capacity for allocator and client overhead. Check that an owned pressure fixture refuses additional allocations, retains existing data, and still delivers a Pub/Sub message.

## Bound the Redis cache of completed link jobs

- Active and dead work already have budgets. Completed jobs left four Redis records behind for up to seven days. The owned 10× database contains 365,771 durable job receipts and added 24,055 during the soak.
- Reap completed Redis markers by age and toward a target of 32,768 entries, including while the ready queue stays busy. Process up to 500 markers every five seconds. Leave active work, leases, dead jobs, and the PostgreSQL receipts intact. A replay outside the recent Redis cache can reach the worker, where the durable receipt prevents a second graph write or notification.

## Isolate basemap failure scenarios at the browser process

- Two hosted runs stalled while opening a new context in the Chromium process already used for the preceding WebGL test. Launch Chromium for each scenario, keep one worker, and close each context before closing its browser so Playwright can record failure artifacts. Keep the renderer options, assertions, and deadlines.

## Stream the owned soak replay input

- One process in the timed harness accounted for up to 253 MiB of swap. The harness materialized the full packet table for subscription checks and then assembled another list of serialized broker messages.
- The owned fixture writes paired packet and raw CSV records in chronological order. Verify the full ordered pair list, scope, count, and distinct hashes before publication, then stream the same records through the existing paced MQTT publisher at QoS 1. Retain only the hashes and observations needed for reconciliation. Keep the ordinary replay CLI's ability to join and sort independent files, and include the executable replay helper in the guarded failure receipt.

## Reduce CoreScope memory across retained-history reads

- Share immutable path JSON, parsed paths, and adjacent equal timestamps inside CoreScope's load passes. Packet responses copy the shared path slice before returning it. The hashing pass keeps the original hash formula and reuses the stored canonical hash.
- Reduce the repeater index under one read lock and reuse prefix deduplication. Keep the timestamp, route, and scope rules. Restore the lock and eviction proof at the published-result boundary and measure the whole reducer.
- Reuse scratch space for aggregate hop resolution. The distance index reserves a small starting buffer and grows with resolved records. It uses the existing sender lookup to avoid retaining unrelated decoded payloads. The private cold-route probe verifies the 202-to-200 transition against the complete 461,125-row fixture at 512 MiB.

## Handle process exit during resource sampling

- The soak collector stopped when Linux returned `ESRCH` while a process was disappearing. Skip `/proc` traversal for an empty native process set. Treat `ESRCH` like a missing process file in the per-process reads. Missing required containers still fail resident coverage and stability, and surviving containers still supply their effective cgroup limits.

## Wait for accepted traffic to settle after cancelling a replay

- The disk guard stopped the owned publisher while UKMesh was still committing queued traffic. Restore now retries an explicit busy reading from the initial intake check. It holds the existing lease, starts retries within the 120-second drain budget and runs full protection before publishing the original selection. Malformed evidence and refusals after protection starts still stop the attempt.

## Resolve multibyte paths without storing expanded observation sets

- An eight-day statistics refresh used more than 2 GiB of PostgreSQL temporary space on the owned 10× fixture. It materialized the full observation set at several steps and joined expanded hops back by observation ID.
- Build the canonical identity projection and its prefix dictionary once per statement. Reduce each observation's hops in order and stream the resolved facts into the existing upsert. Keep receiver-role trimming, ambiguity and repeated-node checks, and the privacy generation and coverage update in the same statement.
- The matched read-only projection returned the same two result digests across 53,737 rows. The complete eight-day read-only plan processed 715,587 rows with 5,850 temporary blocks written. The disposable PostgreSQL test exercised the fact writes and privacy fence.

## Filter the independent soak audit in each source table

- The final reconciliation exceeded its 30-second deadline after all 72,000 ACK packets were acknowledged, delivered live and drained. The audit joined the materialized wanted hashes to four complete sources. Each source can use the same hash array as a direct membership filter before aggregation.
- Keep the independent counts across all partitions and retain matching children with no packet parent. The wanted set still owns missing hashes, observer expectations and mismatch output. Keep the same 30-second deadline and the separate parent-linked ingest-drain proof.
- After restoring the original stores, both read-only queries returned all 72,160 observations and ledger records, all 72,000 paths and facts, and zero mismatches on the retained fixture. The direct filter took 7.76 seconds and the subsequent join took 12.17 seconds. This ordered diagnostic does not replace the timed run. The native PostgreSQL oracle still covers duplicate inputs, auxiliary observers, missing hashes and unrelated rows, and children with no matching parent.

## Select a scoped packet for the shared lookup workloads

- The first 1× baseline window stopped when its positive packet-detail case returned 404. The latest row with two public flags was outside the UKMesh network scope. Both sealed source versions choose the same public, in-scope packet when using the route's predicates.
- Use the production packet scope and privacy builders for the shared packet selection, as the soak already does. Keep the packet-detail route, required 200 and separate missing-packet 404 case, cache modes, source seals, and demand deadlines.

## Reduce scoped region rows as PostgreSQL returns them

- During the 10× soak the scheduled rebuild added about 217 MiB of anonymous memory. It held the full scoped SQL result, a second packet-to-region map and arrays of unattributed packets. The read-only worker-role diagnostic returned the same 249,868 ordered rows with either transport. Fetch batches of 2,048 reached 150,296 KiB RSS versus 403,244 KiB for the array, with elapsed times of 26.86 and 28.80 seconds respectively.
- Preserve the scoped SQL, history, ordered paths and capacity sentinel. Use the reader connection the region job already budgets and finish it before publishing on the advisory-lock connection. Keep one statement budget across the reader's database calls and the existing outer process deadline.
- Reduce each row once with the capped identity metadata. Add unknown rows to the existing greedy clusters in packet order and perform merges and cluster ID assignment after the final row. The five-table publication and generation checks remain in their current transaction.
- The full read-only rebuilds matched the digest of every value destined for publication. Both processed 249,868 packets, decoded 215,112, and produced 387 memberships, 8,718 classifications and 15 clusters. Peak RSS fell from 549,216 to 244,172 KiB. Elapsed time was 77.13 seconds for the previous rebuild and 69.66 seconds for the bounded one, measured in that order on the same fixture. The native publication suite also verified the empty and over-capacity cases, visibility changes, deadline and claim cleanup.

## Limit the established spam refresh to eligible adverts before grouping

- The periodic refresh joined node candidates to broad packet chunks before filtering usable adverts. Under soak traffic it hit the existing 30-second read deadline. Select and project only qualifying adverts first, then retain the same node join and grouped counts. Keep the 30-day window, thresholds, duplicate handling, refresh interval and snapshot budgets.
- On the retained 10× fixture, both paired orders returned the same digest for all 12 records. Before versus after was 27.39 versus 3.90 seconds in the first order and 14.40 versus 5.00 seconds in the reverse order. The read plans used about 265,000 and 81,000 shared read blocks respectively. These were read-only observations outside qualification.

## Count packet and observer region activity from one grouped scan

- The existing chart summary sorted nearly 950,000 observations to calculate four distinct counts over two keys. Give each region and key one last-seen value. A key was present in a shorter window exactly when its latest observation was in that window. Keep null keys out of the distinct counts while allowing them to contribute to last observation time.
- Keep the existing seven-day UKMesh and observer input filters, alias resolution, IATA fallback, ordering, and independent region-series query. On the owned fixture, broad and per-observer results matched exactly. Grouping sets wrote 17,886 temporary blocks versus 24,044, with comparable elapsed time. The independent two-scan variant used less temporary space but was slower by more than 30% and was not adopted.

## Shorten the CoreScope default packet read transaction

- The next soak stopped at the 10 GiB reserve. The CoreScope WAL exceeded 870 MiB while SQLite had backfilled over 99% of its frames. The server held active WAL read locks, and the recorded read marks advanced. During this workload each default packet page rescanned all transmissions for its exact total before fetching 20 rows.
- For the unfiltered page, count the whole table by its compact index and subtract the union of invalid row IDs from indexed blank and null checks. Preserve both existing validity checks, the exact count and page snapshot, and the existing predicate for every filtered page. A nullable legacy corpus excludes the same invalid combinations and counts overlapping exclusions once.
- The six alternating-order pairs agreed at 480,129 rows, with medians of 51.89 and 1.83 ms. The route now uses the indexed count in CoreScope `610cba2d`. The focused retained packet tests and the full Go server suite passed. The WAL and whole-VM resource effect still require the integrated workload.

## Retry an apt package replaced during a mirror update

- Hosted Compose CI stopped when apt's signed index selected Ubuntu nginx archives that returned 404 from the package pool. Allow the current bounded retry for that specific package-fetch error. Preserve immediate failure for missing repositories, manifest failures, unsigned or unavailable packages, and application build or test errors.

## Keep one copy of CoreScope's selected observation

- On the 461,125-transmission 10× fixture, CoreScope's selected packet read kept the sampled WAL below 8 MiB. During the 40-minute live replay, web anonymous memory rose from about 421 to 477 MiB before the 512 MiB cgroup OOM at approximately 503,664 transmissions. The isolated ingestor committed all 72,000 ACKs. The canonical restore needed to start the same web consumer temporarily at 640 MiB, then restored all original targets at default limits and passed all 43 inventory checks.
- Each loaded transmission copies the winning observation's observer labels, signal pointers, path and direction into its permanent record. Keep the winning observation by reference and use the packet store's same longest-path rule, stable ties, cached parsed path and nil fields when there are no observations. Preserve complete history and keep SQL and wire formats as they are.
- Check selected-observation changes and tie behavior on startup, on the retained SQL path and after late observations. Use a retained snapshot and both Go suites to check packet, graph, live feed and analytics parity. Verify the main packet page in Chromium and measure memory at the complete grown history before repeating the guarded soak. The periodic neighbor graph also carries a transient full snapshot at this scale. Measure it after this storage change before deciding how to reduce that peak.
- Before DEV integration, a repeat default-limit cold start on 533,245 rows was OOM-killed. On the separate 480,129-row fixture, the exact unfiltered page and the populated Chromium journey passed at 512 MiB. Keep the grown-fixture startup failure open.

## Bound CoreScope cold-load and graph-refresh allocation peaks

- Profiled the f361d4b1 candidate on a fresh verified clone of the grown 533,245-row fixture at the default 512 MiB limit (core-startup-profile.wNyzpr, protobuf heap/allocs profiles; the debug=1 text format was verified against a known 105 MiB live heap to report raw unscaled sampled bytes and is not used for totals). Two cold starts OOM-killed around the load tail plus path-hop index rebuild, before the post-load analytics pass; the earlier surviving run peaked at the cap during that analytics pass. Scaled profiles attribute about 851 MiB of cumulative load allocations against about 387 MiB live: resolvePathForObsColdLoad→parsePathJSON about 48.5 MiB (532,345 of 533,545 observations share one distinct non-empty path), addTxToPathHopIndex about 30 MiB (a per-transmission seen map plus per-hop ToLower), per-chunk seenTxIDs maps about 29 MiB, and byHash/byTxID/byObsID/packets doubling growth about 139 MiB.
- Couplings recorded before editing: resolvePathForObsColdLoad gains a bounded parsed-path pool and a nil-safe lowercase cache (call sites: legacy Load twice, scanAndMergeChunk, and the retained spansNode read path, which keeps nil pools). addTxToPathHopIndex gains allocation-free dedup for short paths and a nil-safe lowercase cache (call sites: buildPathHopIndex bulk pass, the ingest batch loop, the single-tx update path, and eight test files). seenTxIDs becomes ordered last-id dedup: the chunk SQL orders by t.id ASC so equal ids are adjacent, and advertTxIDs/advertEvidenceForIDs/mergeAdvertEvidence consume an unordered set. LoadChunked pre-sizes the four startup indexes only while they are empty, from the filtered DB count clamped by the memory cap. The neighbor-graph builder reuses loadParsedPaths and StoreObs.ParsedTime: edge times serialize through UTC RFC3339, identical instants keep identical payloads, and parseTimestamp remains for neighbor_persist's small reload.
- The full graph snapshot stays unchunked in this batch: the prior entry requires measuring the refresh peak after the storage change before deciding how to shrink the snapshot. The 390-second probe must observe both scheduled refreshes at the default limit before any snapshot change is considered.

## External merge of PR #246 / fork PR #1 while the startup fix was in flight

- At 2026-11-19 22:57 UTC the user merged CoreScope fork PR #1 (f361d4b1 → gadgethd/CoreScope ukmesh-dev, merge commit 965cc93d) and UKMesh PR #246 (ebf1d67 → gadgethd/ukmesh ukmesh-dev, merge commit a960fc3). All three CI runs on ebf1d67 had finished green (44633216887, 44633206418, 44633216783). The grown-fixture cold-start OOM was still open at merge time; the merge is recorded as a user action, not a qualification verdict, and the 10× default-limit startup criterion remains unmet until the fix below passes.
- Landing plan for the fix: the fork tree at 965cc93d is identical to f361d4b1 (empty diff), so the verified scratch worktree carries its edits onto a new branch ukmesh/lean-cold-load created at 965cc93d; ukmesh/bounded-packet-history stays at f361d4b1 and nothing is reset. After both Go suites, the 390-second default-limit probe with both scheduled refreshes, payload parity against the original binary, and the Chromium packets journey pass, the fix commits on ukmesh/lean-cold-load and opens a new fork PR; UKMesh then re-pins the gitlink on a new branch from a960fc3 with a new PR. No history rewrite, no force-push, receipts stay per-run.

## Post-load peak reclaim + review-driven hardening (lean-cold-load, attempt 4)

- Measurement (core-leanload-verify.8w2bA2, candidate ELF 7748c4d7): the allocation cuts fixed the load-tail OOM (window A) — LoadChunked now completes 533,245 tx / 533,545 obs in 4.74 s and readiness is reached — but the process still OOM-killed at t≈6.0 s at the 512 MiB default cap. Memory timeline: working set 469.7 MiB at load-complete vs ~410 MiB live, then oscillating 495→512 across the post-load analytics recompute + graph rebuilds + the read burst (reads start at early readiness t≈1 s). Root cause: the faster bounded load interleaves less GC, so ~60 MiB of load garbage is still uncollected when the analytics pass layers its scratch on top; the combined peak breaches the cap. The earlier surviving f361d4b1 run peaked 494 MiB sampled only because its slower load interleaved more reclaim.
- Coupling: RunStartupLoad (chunked_load.go) is the single production load orchestrator; the post-load analytics recompute is gated on StartupLoadDone(), signalled by the deferred signalStartupLoadDone at RunStartupLoad return. A single runtime.GC() placed after LoadChunked succeeds (before that signal) therefore reclaims load garbage BEFORE the analytics pass starts, returning the heap to the ~410 MiB live baseline. This is a one-time bounded phase-boundary reclaim, not a steady-state intervention; it changes no stored data or API output. Tests call LoadChunked directly and do not hit it. Expected post-fix peak ≈ 452 MiB (live 410 + one gated recompute scratch), comfortably under 512. The t≈300 s full-store graph refresh is covered by the same pooled-path/ParsedTime cuts and is re-measured by the 390 s probe.
- Independent subagent review (ses_e099ea172ffeB3r3IpXpSw4i69) verdict on the production diff: behavior-preserving across all eight axes (ordered dedup count/maxID/cursor, chunkTxs≡old map incl. shared-hash, pre-size race/clamp, shared path slices never mutated, ParsedTime instant+render parity, ≤8 linear dedup, cachedToLower nil-safety, AGENTS.md). Its one blocker (false test premise: Go time.Parse accepts fractional seconds even when the layout omits them) was already corrected in attempt 3 — those shapes are parity cases, not a strict superset. Review-driven additions in attempt 4: (1) explicit size bound maxLowerCacheEntries=4096 on cachedToLower inserts (AGENTS.md map-cap compliance; pure optimization, misses recompute); (2) regression tests for shared-hash dedup, memory-clamped pre-size, non-UTC edge-time end-to-end, and the bounded lowercase cache.
- Build-environment note: the first two pipeline attempts were compiler OOM-killed inside the 1184m no-swap build container while the host had ~683 MiB available; --memory-swap was raised to 2624m (build container only) so the toolchain can spill to host swap. Runtime probes/soak keep their own limits; this does not relax any runtime gate.

## Isolated-probe warmup race (diagnostic fix, not a product change)

- The BDzNE6 verification run aborted in the REFERENCE phase: the entry burst reads default-shape /api/analytics/rf and /api/analytics/topology immediately after backgroundLoadComplete, but the #1659 analytics warmup gate returns 503 until the first post-load recompute completes (a beat later). The original reference binary is unchanged and passed earlier runs by poll-timing luck; this is a flaky isolated diagnostic, not a candidate memory result. The candidate never ran in that attempt.
- Fix in the isolated probe.py (active evidence dir core-leanload-verify.mEn757): after readiness, a bounded (45 s) wait polls /api/analytics/rf until it returns 200, mirroring the canonical read protocol's warmup wait. This does not weaken any gate — steady-state latency/failures read only /api/packets (warmup-independent), and packet payload parity hashes are warmup-independent. BDzNE6 is marked SUPERSEDED-warmup-race; 8w2bA2 keeps the pre-GC-barrier candidate OOM receipts. Each verification attempt uses a fresh evidence dir; the launcher tolerates an already-released build cache and fully frees the tmpfs clone (incl. -shm/-wal sidecars).

## Periodic analytics-pass OOM at t≈310 s and the work-gate reclaim (attempt 5)

- Measurement (core-leanload-verify.mEn757, candidate ELF 14886090): the RunStartupLoad GC barrier FIXED the startup OOM — the candidate survived load, the startup analytics pass ("recomputed 11 snapshots in 1.352s"), and the first full-store graph rebuild at t≈301 s (+23 MiB, reclaimed; the pooled-path/ParsedTime cuts hold). Steady baseline settled at ~443 MiB working (store + populated analytics caches). It then OOM-killed at t≈310 s — the first PERIODIC 5-minute analytics recompute: working 443→485→511 MiB. The startup pass survived only because the GC barrier gave it a ~410 MiB baseline (caches not yet populated); the periodic pass starts from the ~440 MiB populated-cache baseline.
- Attribution: each analytics compute allocates ~5-27 MiB of transient scratch (aggregateRF snr/rssi/size/scatter buffers ~15 MiB; hash-sizes globalAdopterHS map; topology filtered slice). The analyticsWorkGate serializes the 11 computes, but with GOMEMLIMIT below the live heap the GC cannot reclaim each compute's scratch before the next starts, so ~85 MiB accumulates across the pass. 440 + 85 > 512.
- Fix: analyticsWorkGate.release() now runs runtime.GC() BEFORE freeing the slot, so the finished compute's scratch is reclaimed before the next compute is admitted. This bounds the sequential peak to live-baseline + a SINGLE compute's scratch (~467 MiB) rather than the sum. The reclaim runs while the slot is held (next compute waits) and is background-only — requests read the atomic cache, never the gate — so no request-path latency is added. The RunStartupLoad barrier is kept (clears load garbage before the pass); the two are complementary. Coupling: release() is called via defer in analyticsRecomputer.runOnce and by the neighbor-graph / repeater-enrich gate users; acquire's stop-abort path also calls release(). No re-entrant acquire exists (1-slot serialization invariant unchanged).

## Periodic-pass attribution diagnostic (core-periodic-profile.7heJvc)

- Code reading identifies computeAnalyticsDistance as the likely dominant periodic-pass allocator: it copies the ENTIRE precomputed distance index into filteredHops (distHopRecord = 136 bytes: 7 string headers + 3 words) and filteredPaths even when unfiltered (matchSet==nil), then builds catDists/distByHour/allDists from the copy. filteredHops is NEVER mutated downstream (only filteredPaths is sorted), so the full hops copy is pure transient waste in the default recomputer shape. At ~400-530k hop records that is ~55-72 MiB — matching the observed +56 MiB peak over the ~443 MiB populated-cache baseline (512 cap, ~69 MiB headroom).
- To confirm empirically before cutting, a short diagnostic forces the WORST-CASE periodic pass: analytics.pauseWhenIdle=false + defaultIntervalSeconds=25 fire all 11 recomputers together ~25 s after start, at the 640 MiB recovery-only size so the pass survives for pprof attribution (diagnostic, not the 512 gate). ENABLE_PPROF on; cumulative allocs + live heap captured densely t=20..52 and differenced offline. Reuses attempt-5 ELF (2ff394d1); does not release the build cache. If distance dominates, the behavior-preserving cut is to read hopsSnap directly when unfiltered (no copy) and avoid the full paths copy for the top-20 sort.

## Periodic-pass root cause CONFIRMED by profile: aggregateRF signal-slice growth (attempt 6)

- The forced worst-case diagnostic (core-periodic-profile.GPMWeS, attempt-5 ELF 2ff394d1, 640 MiB recovery size, pauseWhenIdle=false + defaultIntervalSeconds=25 so all 11 recomputers fire together) survived and captured protobuf allocs/heap across the periodic ticks. Startup log shows distance=0s: the distance index is LAZY (empty at startup, built only on a distance request, which the probe never makes), so the earlier distance-full-copy hypothesis was WRONG and distance is left untouched.
- pprof -alloc_space -base allocs-t28 allocs-t40 (one periodic pass): dominant allocator main.aggregateRF.func3 (the observe closure) = 68.23 MiB (48.6%): rssiVals append 14.74 MiB, snrVals append 14.74 MiB, scatterAll append 38.75 MiB. Line-level -list confirms store.go:6626/6632/6635. These slices were pre-sized estCap/2 (snr/rssi) and estCap/4 (scatter), but nearly all 533,545 observations carry SNR/RSSI, so they grew via Go's 1.25x large-slice reallocation; the cumulative backing-array garbage is ~68 MiB/pass. On the ~443 MiB populated-cache baseline (512 cap, ~69 MiB headroom) that breaches at the periodic recompute. (Remaining diff nodes are pprof-capture overhead — compress/flate, runtime/pprof — and steady /api/packets reads, not product scratch.)
- Fix (behavior-preserving): pre-size snrVals/rssiVals/scatterAll to estCap (the total-observation upper bound) so appends never grow. Identical values/output; ~17-21 MiB live backing arrays instead of ~68 MiB of churn. packetSizes already txCap (unchanged). The analyticsWorkGate.release() GC reclaim is KEPT — it bounds the OTHER computes' sequential scratch (topology ~13, hash-sizes ~7.5, retransmissions ~8.5 MiB); expected periodic peak now ~443 + max-single-compute (~21) ≈ 464 MiB, ~48 MiB under cap.
- Regression: TestAggregateRFSignalSlicesPreSized (cold_load_pool_test.go) feeds 100k pre-built signal-bearing transmissions through aggregateRF and asserts a steady pass allocates in [32,64] B/obs — above the exercised minimum, below the growth-era ~140 B/obs. Perf-characteristic assertion per AGENTS.md.
- Diagnostic probe bug fixed en route (7heJvc SUPERSEDED): warmup wait must cover BOTH rf and topology (entry burst reads both; topology's #1659 gate clears after rf), and profiling starts after warmup so the baseline is post-load. GPMWeS is the clean attribution receipt.

## Attempt-6 verification (core-leanload-verify.RgxmwA)

- Attempt-6 ELF 57487307 (RF signal-slice pre-size + gate reclaim + GC barrier + Stage-1 load cuts). Server suite ok 48.653s, ingestor ok 247.456s, build clean. TestAggregateRFSignalSlicesPreSized PASS 0.09s (explicit -v receipt rf-presize-test-attempt6.log in the stage dir).
- Verification probe warmup wait hardened to cover BOTH rf and topology (entry burst reads both; topology's #1659 gate clears after rf). Does not weaken any gate: steady latency/failures read only /api/packets; parity uses packet payloads. Candidate runs the default 512 MiB for 390 s (startup + t≈301 graph rebuild + t≈310 periodic analytics pass + Chromium journey); reference original binary at 640 MiB recovery override 30 s; exact packet payload parity.

## Attempt-6 verification RgxmwA: MEMORY GATES PASS; browser blocked by missing host libs (environment, not product)

- core-leanload-verify.RgxmwA (candidate ELF 57487307, default 512 MiB, 390 s): NO OOM (oom_kill=0), sampled working peak 488.2 MiB and cgroup memory.peak 491.5 MiB (both < 512), readiness 9.65 s, startup heap 399.4 MiB, resident coverage 1.0, both graph refreshes (default 2 / unfiltered 2), steady 390.0 s with 3119 reads, ZERO failures, p95 13.28 ms. The RF signal-slice pre-size + gate reclaim + GC barrier resolved the periodic-pass OOM. Reference (original binary, 640 MiB) passed, peak 568 MiB.
- The ONLY failing gate was browser_passed: Playwright chromium_headless_shell-1243 could not launch — host missing libatk-1.0/libatk-bridge/libatspi/libXcomposite/libXdamage/libXfixes/libXrandr/libgbm/libasound (+transitive libXrender/libXi). This is a host environment defect (no passwordless sudo; recent verification attempts had OOM'd before ever reaching the browser, so it was never exercised here). NOT a product regression.
- Non-root workaround (no system changes): apt-get download the Noble t64 packages (libatk1.0-0t64 libatk-bridge2.0-0t64 libatspi2.0-0t64 libasound2t64 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libxrender1 libxi6), dpkg-deb -x into /tmp/opencode/chromium-libs/prefix, export LD_LIBRARY_PATH=<prefix>/usr/lib/x86_64-linux-gnu. Smoke test: chrome-headless-shell --version OK (153.0.8010.12) and Playwright launch about:blank OK. The proper host fix is `sudo playwright install-deps chromium`; the local prefix is the non-root equivalent used for this run. launch.sh exports LD_LIBRARY_PATH so the candidate browser subprocess inherits it (docker containers are unaffected — they get explicit env).

## Attempt-6 verification EmnIRI: memory gates pass again; browser harness expectation STALE (fixed)

- core-leanload-verify.EmnIRI (same ELF 57487307, 512 MiB, 390 s, Chromium libs fixed via the local prefix): NO OOM, sampled working peak 496.1 MiB, cgroup memory.peak 502.5 MiB (both < 512), readiness 11.3 s, coverage 1.0, refreshes 2/2, steady 390.0 s / 3119 reads / ZERO failures / p95 14.85 ms. Memory fix confirmed reproducible across two runs (RgxmwA peak 488.2, EmnIRI 496.1).
- browser_passed=false AGAIN, but differently: Chromium launched, the SPA booted (server log shows [ws] client connected at the browser moment), yet page.waitForResponse timed out waiting for /api/packets?limit=20. Root cause: the leanload-verify browser.mjs is a STALE variant. The current packets page initial load uses PACKET_LIMIT = isMobile ? 1000 : 50000 (public/packets.js:757, present even at the old pin 610cba2d), so the page NEVER requests limit=20 on initial load. The earlier successful Chromium receipt (480,129 fixture, publish-browser.n87pTm/browser.json responsePackets=20) was produced by a DIFFERENT, canonical journey: publish-browser.n87pTm/browser-journey.mjs, which (1) verifies /api/packets?limit=20&offset=0 via direct fetch (total===expected, exactly 20 rows — response-limit fidelity), then (2) navigates to /#/packets?hash=<first-hash> and waits for the /api/packets?hash=<hash> detail request the app actually issues (packets.js:1349 _initUrlParams.get('hash') → filters.hash → buildPacketsParams line 791), asserting the focused hash, rendered rows, and zero page errors.
- Fix: the next verification dir uses the proven browser-journey.mjs content as browser.mjs (probe.py invokes `node browser.mjs <endpoint> <expected>` — argument-compatible). This RESTORES the canonical journey; it does not weaken any gate (it adds the direct limit=20 listing check the stale variant only attempted via the page). Not a product regression: public/ is byte-identical between the submodule pin f361d4b1 and the scratch worktree (diff -rq clean).

## Independent review of gate-reclaim + RF pre-size: no blockers; four improvements applied (attempt 7)

- Subagent review verdicts: CHANGE 1 (GC in gate release) SAFE — all callers acquire-then-defer-release, stop-abort releases only after a successful send, background-only, no re-entrancy; CHANGE 2 (estCap pre-size) SAFE/behavior-preserving — ≤1 append per slice per observation in normal/regional branches, estCap=0 fine, no overflow; CHANGE 3 (pre-size test) SOUND.
- Applied before the attempt-7 rebuild: (1) acquire's stop-abort path now calls releaseSlot() (no forced GC — no compute ran; keeps Stop's 5 s timeout margin); release() = runtime.GC() + releaseSlot(). (2) Softened the release comment (13 gated computes incl. neighbor-graph/repeater-enrich; GC is process-wide but the pacer is already saturated at this GOMEMLIMIT; measured peak 488-496 MiB replaces the ~467 estimate). (3) Qualified the store.go pre-size comment: capacity hint, not a hard bound — legacy obs-less txs, retained-mode request scans larger than the resident cache, and >2M-obs stores can still grow (append handles it, as before). (4) RF test now asserts result totalPackets/totalAllPackets == n (the allocation lower bound alone cannot prove filling — pre-sized arrays allocate even when empty) and documents the Z-suffix timestamp requirement; NEW TestAnalyticsWorkGateReleaseReclaimsScratch pins NumGC delta across release(), slot re-acquire, releaseSlot freeing, and stopped-acquire false+slot-empty.
- Comment/test edits shift line numbers relative to the verified ELF 57487307, so attempt 7 rebuilds and RE-VERIFIES at 512 MiB (fresh dir) before commit — the committed tree will exactly match the qualified binary's source.

## Disk reclamation (pre-attempt-7): dangling docker images

- Free space had dipped to 9.99 GiB (gate: ≥10 GiB). Reclaimed ONLY dangling (untagged, unreferenced) docker images via `docker image prune -f`; no tagged image, volume, container or evidence file touched. Receipt before removal:
  - 04e40053a0d1 300MB created=2026-11-18 12:05:21 +0000 UTC
  - bc5afe0ad5ab 300MB created=2026-11-18 00:08:33 +0000 UTC

## Attempt-7 build + verification (core-leanload-verify.1HN7hc)

- Attempt-7 ELF 9b0e72e299ba5915 (review improvements: releaseSlot stop-abort, comment qualifications, RF test output assertions, gate GC-ordering test). Server suite ok 49.566s, ingestor ok 195.298s, build clean. Explicit -v receipts: TestAggregateRFSignalSlicesPreSized PASS 0.08s, TestAnalyticsWorkGateReleaseReclaimsScratch PASS 0.00s (stage1-test-receipts-attempt7.log). Attempt-6 receipts preserved (server/ingestor/pipeline/ELF *-attempt6).
- Verification 1HN7hc: same probe (dual rf+topology warmup), launch.sh with the non-root Chromium lib prefix, and the PROVEN canonical browser journey (publish-browser.n87pTm/browser-journey.mjs content: direct limit=20 listing fidelity check + hash-focused page journey). Candidate default 512 MiB / 390 s; reference original binary 640 MiB / 30 s; exact payload parity. This is the commit-gating run: the committed tree will exactly match this ELF's source.

## Stage 1 VERIFIED — core-leanload-verify.1HN7hc FULLY GREEN (commit-gating run)

- Candidate ELF 9b0e72e299ba5915 at the DEFAULT 512 MiB, 390 s: passed=True, oom_kill=0 (all cgroup memory events 0), state running, coverage 1.0, readiness 5.62 s, startup heap 391.4 MiB, sampled working peak 485.1 MiB, cgroup memory.peak 494.7 MiB, swap 0, refreshes 2/2, steady 390.0 s / 3119 reads / ZERO failures / p95 13.88 ms, browser_passed=True.
- Canonical Chromium journey receipt: {"passed":true,"status":200,"retainedPackets":533245,"responsePackets":20,"renderedRows":1,"errors":[]} — the limit=20 listing fidelity check AND the hash-focused page rendering both pass on the grown fixture at 512 MiB.
- matched-packet-parity.json: passed=true, packet_payloads_identical=true, rows 533245. Reference (original binary, 640 MiB recovery override, 30 s): passed, peak 570.8 MiB, startup heap 467.9 MiB, ready 7.08 s. Candidate startup heap is 76.5 MiB LOWER than the original at readiness.
- Three 390 s runs (RgxmwA 488.2, EmnIRI 496.1, 1HN7hc 485.1 MiB working peaks) all passed every memory/read gate; the final run additionally passed the browser journey and exact parity. The two earlier browser failures were harness/environment issues (missing host libs; stale limit=20 expectation), both documented and fixed without weakening gates.
- Landing: commit the 14-file batch on ukmesh/lean-cold-load (base 965cc93d), push, fork PR → ukmesh-dev; re-pin UKMesh gitlink from a960fc3 on a new branch with the handoff update; new UKMesh PR. Merges remain user actions; the 10× startup criterion is now CLOSED by this evidence (fix verified at the default, not the recovery size).

## Stage 1 LANDED (pending user merges + CI)

- CoreScope: commit b1ffa00317d98829a5b880bf7bfa0e3da1be0bfb on ukmesh/lean-cold-load (14 files, +717/-47; explicit git add; gofmt clean; identity gadgethd noreply, repo-local config). Pushed to the fork; PR #2 https://github.com/gadgethd/CoreScope/pull/2 (head b1ffa003 → base ukmesh-dev, like PR #1). Commit message carries the full perf proof.
- UKMesh: branch ukmesh/lean-cold-load-repin from a960fc3; commit aae266f re-pins third_party/corescope f361d4b1 → b1ffa003, rewrites handoff section 1 (memory proof complete), annotates the web-OOM failure as addressed pending soak re-qualification, and records this log. PR #259 https://github.com/gadgethd/ukmesh/pull/259 (base ukmesh-dev). Follow-up commit adds the PR/commit cross-links.
- Submodule working tree checked out at b1ffa003 (matches the new gitlink). plans/ is git-ignored but decisions.md is tracked — staged via git add -u (no -f, nothing untracked added).
- GitHub API auth quirk: the gho_ OAuth token intermittently returns 401 "Requires authentication" for REST calls while git HTTPS auth always works; both Bearer and Basic succeed in the good windows. PR creation used a retry loop alternating schemes. No credential changes made.
- Merges remain USER ACTIONS (as with #1/#246). The 10× startup criterion is CLOSED by the 1HN7hc evidence. Next: CI green on both PRs, then Stage 2 batches (charts/spam/swap/disk) with before/after owned-fixture evidence, then Stage 3 mqtt-first qualification on the final source.

## Stage 1 DEV landing verification (post-restart inventory)

- Canonical `scripts/dev-all.sh restart corescope` rebuilt both Go binaries from the submodule at b1ffa003 and recreated the web+ingestor pair (exit 0, both Healthy). Deployed COMPANION_SOURCE_REVISION 6950bd186b4c396fd02a6ddf8a7d74f972a8b65d1fbb33559ba8d02b3860b01f equals source-hash.py output for the submodule at b1ffa00317d98829a5b880bf7bfa0e3da1be0bfb. Web container: memory 536870912 (512 MiB default), memory_swap equal (no container swap), RestartCount 0, GOMEMLIMIT=256MiB unchanged.
- Fresh inventory receipt stage1-landing.eqdzon: 43/43 services passed, coverage checks all true, public_tunnel_off true. All DB consumers on original targets (meshcore / meshcore_owner_auth / beacon_dev); both CoreScope containers mount the ORIGINAL plans/companion-dev/runtime/corescope directory (web/ingestor read/write split preserved). No fixture selected; retained fixtures untouched.
- CI watcher running on UKMesh PR #259 head 4e1a3d9 (run 44666069250 pull_request in progress at landing time). CoreScope fork has no CI workflow; UKMesh CI builds the pinned submodule and is the authoritative gate. Merges remain user actions.

## Stage 2 reconnaissance — chart-refresh budget cascade and spam lost-clients (read-only)

Evidence sources (all read-only, originals selected, no fixture mutation): the exact soak receipts
guarded-indexed-core-soak.nsY0CJe6 (steady-runtime-logs.json digests verified against backend.log
89c170e6… and sample-db.log afe2fc4a…), Prometheus history for 2026-11-19 (retained), and
pg_stat_statements on the fixture DB lean_10x_20261118215522_9c0a37 (stats_reset 2026-11-13).

### Chart refresh (batch 1) — root cause is serial work, not pool/admission contention
- DEV backend env: STATS_DB_QUERY_CONCURRENCY=1, STATS_AGGREGATE_READS_ENABLED=false,
  STATS_AGGREGATE_SHADOW_ENABLED=false, DATABASE_STATEMENT_TIMEOUT_MS=30000. Analytics pool = worker
  role, random_page_cost=4, effective_cache_size=512MB, PG statement_timeout=max(900000,30000)=15min
  (database.ts analyticsStatementTimeoutMs). So the binding deadlines are app-side only.
- With aggregates OFF, fetchChartsData issues 17 queries through one Promise.all, all serialized by
  the concurrency-1 slot FIFO (statsRepository.ts query()). Each query's 60s budget clock starts at
  wrapper entry BEFORE acquireQuerySlot, so it includes slot wait (established semantics, decisions
  "Bound statistics work to one statement on the lean main runtime": queue wait counts toward 60s/120s;
  "If serial work violates them, fix work instead of raising limits").
- The multibyte fact summary is query #12 in the array. It COMPLETED (returnedRows=1) at cumulative
  wait+exec ~60.96s -> logged [stats-query-budget] exceeded, threw STATS_QUERY_BUDGET_EXCEEDED, which
  chartAbort.abort() cascaded to the 5 still-queued queries #13-17 (observerDiversity, signalSummary,
  routeTypes, transportCodes = 4x mesh.packets; pathDecodeTrend = 1x multibyte_path_facts). Each logs
  "query failed after budget" (the exceeded query logs BOTH exceeded and failed-after-budget = the
  "duplicate reporting"). 6 clusters x 7 matching lines = 42 backend failure lines. The "chart refresh
  failed" line is the 7th match per cluster.
- Prometheus FAIL window 09:29-09:39: analytics/other success = 82 queries / 378.85s exec (mean 4.62s);
  analytics pool wait only 0.13s total / 83 checkouts (negligible). EARLY 09:00-09:06: 30 queries /
  116.86s (mean 3.90s). 17 x ~4.6s ~= 78s serial > 60s budget. Contention is execution volume, not waits.
- pg_stat_statements fixture means (per chart query, ~25 calls): region series (iata/day COUNT DISTINCT
  packet_hash) 11.13s; region summary (GROUPING SETS) 12.88s; totals sumResult 6.71s; observerDiversity
  (per_packet) 6.60s; pathHashWidths (unnest) 5.15s; prefix_counts (unnest) 4.79s; packet_types 4.63s;
  multibyte summary 3.83s; packets/day 2.56s; hop distribution 2.39s; signal percentile 2.26s; radios/day
  1.35s; packets/hour 0.84s. Sum ~= 70s serial. The two 7-day region scans and the repeated 24h scans
  (10 queries re-scan the same 24h window, each re-evaluating the canonical privacy predicate) dominate.
- The 79s admission wait on ONE cycle (09:30:26->09:31:46) is secondary: even waitMs=2-3 cycles failed at
  ~60-62s of work. No other service logged db-admission in that gap (region/privacy-remat worker logs empty
  for the whole window); holder not identified from logs, but it is not the recurring cause.
- Candidate fix (authorized by next-ai-prompt "reduce repeated fact scans or consolidate same-snapshot work"):
  consolidate same-window/same-snapshot legacy scans into fewer statements sharing one materialized scoped
  base, preserving exact rows/order/values, the asOf snapshot, privacy-generation fences, completeness,
  per-scope data, cache freshness, and the aggregate feature switches (left OFF). Highest yield: merge the
  two 7-day region scans; merge the two 24h path_hashes unnests; merge the simple 24h/7d count aggregates.
  NOT raising budgets/concurrency, NOT enabling aggregate reads (that is a separate documented rollout
  requiring backfill + shadow parity; switches preserved).

### Spam lost-clients (batch 2) — app-side 5s budget abort destroys the connection
- sample-db.log: exactly 6 real FATAL "connection to client lost" (09:06:39, 09:11:44, 09:16:49, 09:21:54,
  09:26:58, 09:37:06), each preceded ~2s by backend [spam-msg] status=timed_out. The other 12 scanner hits
  are SQL-text lines containing the literal 'failed' inside the multiline STATEMENT dump (the remat
  NOT EXISTS subquery of the spam visibility predicate) — false positives, not separate failures.
- Mechanism: spam analyzer interval 300s, SPAM_MESSAGE_ANALYSIS_BUDGET_MS=5000 (whole pass). loadRecentMessages
  runs WITH logical_messages via analyticsQuery with heartbeat.signal. On deadline the heartbeat aborts;
  databaseRuntime.queryPool onAbort calls client.release(true) (DESTROYS the connection) -> PG logs FATAL
  connection to client lost (not a clean cancel). Completed passes analyzed 158/188 messages; timed-out
  passes analyzed 0 (query never returned within 5s).
- pg_stat_statements fixture: WITH logical_messages mean 1.84s, MAX 4.89s (27 calls) — under load spikes it
  breaches the 5s whole-pass budget. Same canonical per-row privacy predicate (visibility EXISTS + remat
  NOT EXISTS with unnest(path_hashes)) as the chart scans. ranked_observers follow-up mean 0.05s.
- Fix direction: confirm plan/waits/rows under the worker role on the fixture; reduce the query's own work so
  it meets the 5s budget under load (preserve signature grouping/order, duplicate weighting, windows,
  visibility, privacy, cancellation). Fix actual lost clients BEFORE any log-parser repair; parser repair must
  handle PG multiline SQL explicitly and retain genuine ERROR/FATAL, with regression cases + saved input.

### Couplings recorded before editing
- statsRepository.ts query()/limitedQuery: budget+timeout+slot FIFO shared by ALL chart queries and the
  multibyte readiness check; source attribution = first FROM table. fetchChartsData shadows query with
  chartQuery (bindChartSnapshot asOf + chartAbort cascade). Consolidation must keep one asOf, the abort
  cascade, the per-query budget/timeout semantics, and identical response row shapes consumed by
  computeChartsData -> validateChartSnapshotPayload -> stats_chart_snapshots.
- fetchObserverRegionSummary is also exported and used elsewhere (observer-scoped); its SQL is shared with
  the broad region summary. Any merge must keep the observer-scoped variant's exact output.
- spam repository.loadRecentMessages/countLogicalMessages/persistIncidents + analyzer.analyzeOnce heartbeat;
  databaseRuntime.queryPool abort path is shared by ALL analytics/oltp signal-carrying queries (changing the
  destroy-on-abort behavior would affect every caller, not just spam).

## Stage 2 batch 2 design — spam window scan fetches every packet type (measured)

- Role-faithful diagnostic: the canonical restore (access.sh) revokes database CONNECT on retained
  fixtures, so diagnostics connect as the owning superuser and SET ROLE meshcore_worker; every
  table/permission check then runs as the soak analytics pool did. No RLS exists on any involved
  relation; worker table SELECT grants survive on the fixture. Planner options match production
  (random_page_cost=4, effective_cache_size=512MB; server work_mem=10485kB, shared_buffers=128MB,
  max_parallel_workers_per_gather=0). Zero fixture mutation for the plan probe.
- EXPLAIN (ANALYZE, BUFFERS) of the exact STATEMENT-dump SQL with the failing soak window
  (2026-11-18T09:06:39Z..2026-11-19T09:06:39Z, networks ukmesh/northeast/teesside, $4=8 $5=4 $6=10001):
  mesh.packets is a TimescaleDB hypertable. The plan time-index-scans two chunks covering the window,
  fetching ~292,787 heap rows (33,586 shared read blocks ~= 262 MiB) and heap-filtering to 326 type-5
  rows (206,775 + 85,686 rows removed by filter). The remat anti-join and visibility One-Time Filter
  are trivial (0 unresolved remat rows). Execution 2,176 ms cold; pg_stat_statements soak mean 1.84 s,
  max 4.89 s vs the 5 s whole-pass budget -> heartbeat abort -> client.release(true) -> PG FATAL.
- No existing index encodes (packet_type, time): the only type-5 partial index additionally requires
  companion_sender IS NOT NULL, which the spam predicates do not guarantee. A query rewrite cannot
  avoid the all-type window fetch; the structural fix is a retainable partial index (Stage 2 item 3
  explicitly lists retainable indexes). Precedent: migrations 154/155 added read-model indexes with
  SET LOCAL lock_timeout/statement_timeout; both stores' ledgers are at 155 and the canonical startup
  runs tools/migrate.ts against the selected database, so one migration reaches originals and fixtures.
- Fix: migration 156 creates mesh.packets (network, time DESC) WHERE packet_type = 5. Predicate kept
  at packet_type only (not payload/visibility) so countLogicalMessages, which has no visibility fence,
  uses the same index. Expected: window fetch falls from ~292,787 rows to the 326 type-5 rows.
  Signature grouping/order, duplicate weighting, windows, visibility, privacy and cancellation are
  untouched — the change is access path only. Before/after digests and paired timings on the retained
  fixture follow; the index is created on the owned fixture as the owner role exactly as the migration
  will create it canonically.

## Stage 2 batch 2 measured — partial type-5 index removes the spam budget breach

- Migration 156_index_message_window.sql: CREATE INDEX IF NOT EXISTS idx_packets_message_time
  ON mesh.packets (network, time DESC) WHERE packet_type = 5. IF NOT EXISTS follows migration 154
  so a diagnostic pre-application on a retained fixture is idempotent with the canonical db-migrate
  step (control.sh runs it against the selected database on every start; both ledgers were at 155).
- Applied to the owned fixture lean_10x_20261118215522_9c0a37 as the owner role with the exact
  migration SQL (BEGIN/SET LOCAL/COMMIT): build 2.17 s, propagated to all 16 chunks, total chunk
  index footprint ~12.1 MiB for 618,909 type-5 rows. Evidence: stage2-diagnostics.Z7TQiL
  (fixture-index-apply.sql/.txt, index-size.txt). No data rows touched.
- Before (no index): exact soak-window EXPLAIN ANALYZE cold 2,176 ms, repeat 3,473 ms; 33,586-34,000
  shared read blocks (~262 MiB) per pass — larger than the 128 MB buffer pool, so the scan evicts
  itself and stays cold under the 300 s interval; ~292,787 heap rows fetched, 326 kept.
  Soak pg_stat_statements mean 1.84 s / max 4.89 s vs the 5 s whole-pass budget.
- After (index): same statement, same parameters, Execution Time 16.5 ms; 269 total blocks
  (112 hit / 157 read); chunk scans 7.7 ms + 1.2 ms fetch exactly the 326 type-5 rows.
  countLogicalMessages uses the same index: 1.1 ms, 254 hits. ~130-210x less execution and ~126x
  fewer blocks; the 5 s budget now has three orders of magnitude of headroom even under load.
- Result parity: 163 rows, byte-identical aligned output, sha256
  f36e1798c34202578e6251ffe563c6197102a24365fdbd00e716f44c28bf2c0c before and after
  (before-rows.txt / after-rows.txt). Signature grouping/order, duplicate weighting, windows,
  visibility, privacy and cancellation untouched — access path only.
- Suites: spam repository/privacy/analyzer/config + migrations unit tests 27/27 pass;
  migrationBootstrap + legacyMoveReplay integration 5/5 pass on a fresh scratch database
  (full 156-migration replay, 70 s bootstrap). PGlite spam SQL fixture not installed locally (opt-in).
- Remaining for batch 2 closure: concurrent-ingest proof in a canonical fixture session (the query
  is now 16 ms / 269 blocks, so the soak's spam passes must complete with zero lost clients), then
  the runtime_logs.py multiline-SQL parser repair with regression cases and the saved input.

## Stage 1 CI split verdict on 2c687b8 — confirmed pre-existing test-harness flake

- Watcher verdict: push run 44666753261 SUCCESS (all 7 jobs incl. Backend), pull_request run
  44666759322 FAILURE — Backend job 136057910560..993, step 21 `npm run test:ingest-integration`,
  single failing test `spam snapshots execute complete bounded SQL and retain verdicts on a
  rejected refresh` (src/mqtt/spamSnapshot.integration.test.ts), error "terminating connection
  due to administrator command". Same head 2c687b88 for both runs; the PR changes only the
  CoreScope gitlink, handoff and receipts — no backend code — so this cannot be a regression
  introduced by the PR.
- Mechanism (CI service-container PG log): at 03:20:20.904 UTC the test's OWN teardown
  `DROP DATABASE "meshcore_spam_10063_1795144819748" WITH (FORCE)` terminated connection [554]
  one millisecond later, while a test query was still in flight. spamDetector.refreshSpamDetectorCaches
  deduplicates through a module-global refreshInFlight and awaits Promise.all([established, identity]);
  when the identity refresh rejects (the test's deliberate oversized-name rejection), Promise.all
  settles without cancelling the established sibling, whose remaining fixture-pool work races the
  t.after pool.end()/DROP DATABASE FORCE teardown. Timing-dependent: the push run passed the same step.
- Action: rerun the failed PR job on the unchanged head; keep both run IDs and the job log
  (/tmp/opencode/ci-backend-pr.log, private). If the rerun reproduces, the minimal harness fix belongs
  in a separate qualified commit, not silently in the Stage 1 pin PR.

## Stage 2 batch 2 parser repair — multiline PostgreSQL SQL is data, not failures

- runtime_logs.py: inspect_log now tracks PostgreSQL log-record state. A record prefix
  (date time tz [pid] SEVERITY:) starts a new record; STATEMENT/QUERY/DETAIL/HINT/CONTEXT/LOCATION
  records and their unprefix continuation lines are SQL/bind echoes and are skipped by the failure
  scan. ERROR/FATAL/PANIC severity records remain failures; LOG/WARNING and every non-PG line keep
  the previous scan. failure_line's per-line API is unchanged; skipped lines still feed the digest.
- Regression: test_runtime_logs.py gains synthetic FATAL+multiline-STATEMENT, ERROR+DETAIL+STATEMENT,
  dump-only, dump-then-new-ERROR and LOG cases, plus the saved real input
  fixtures/runtime-logs/soak-spam-lost-clients.log.txt (exact 516-line excerpt of the retained soak
  sample-db.log, named .log.txt because the repo ignores *.log: six FATAL records each followed by its logical_messages STATEMENT dump; synthetic
  fixture data only, no credentials). 6/6 tests pass; test_qualification + test_soak_prepublication
  85/85 pass.
- Measured on the retained soak receipts: sample-db.log failure_lines 18 -> 6 (exactly the genuine
  FATALs at lines 88/262/436/610/784/960); backend.log stays 42 (real application failures retained).
  The repair follows the actual lost-client fix (migration 156), per the stage order.

### CI rerun verdict — PR #259 green on unchanged head

- Rerun of failed jobs on run 44666759322 (pull_request, head 2c687b88cff2) completed:
  attempt 2 SUCCESS — Backend, Workers and Compose, Frontend, HopReach RF gates, Monitoring
  configuration, Database Audit Tests, Secret scan all success. Combined with push run
  44666753261 SUCCESS, Stage 1 CI is green on both event types at the final head. The
  attempt-1 failure is confirmed as the documented teardown race, not a regression; the
  minimal harness fix (await the in-flight refresh before DROP DATABASE FORCE) remains a
  separate qualified change if pursued. Merges of PR #259 / CoreScope PR #2 stay user actions.

## Stage 2 batch 1 chart consolidation — couplings recorded before editing

- Budget mechanics (statsRepository.ts 141-222): the 60 s STATS_QUERY_DURATION_BUDGET_MS is
  per query() call and INCLUDES slot wait (startedAt before acquireQuerySlot). With
  STATS_DB_QUERY_CONCURRENCY=1 all 17 Promise.all queries queue serially, so query N's
  duration ≈ sum of executions 1..N; the 78 s total made query #12 exceed 60 s and abort
  the cascade. Fix = reduce total serial execution, not raise limits.
- Scan inventory (reads OFF): 15 mesh.packets scans + 2 multibyte_path_facts scans + coverage
  check. Same-snapshot groups: (a) six switchable simple parts ph/pd/pt/hd/route/transport
  (7d base, 24h subsets); (b) radios rh/rd + totals total_24h/total_7d/unique_radios_24h
  (src alias LEFT JOIN); (c) pc + pathHashWidths (identical 24h path_hashes unnest base;
  pc filters NULLIF(TRIM(h),'') and HAVING>1 LIMIT 10, widths counts ALL elements incl empty);
  (d) orSummary + orSeries (7d rx regex scan) + observerDiversity (24h rx join) + signal (24h).
- Privacy: packetsAlias ALWAYS emits publicPacketPrivacySql + network/test exclusions +
  observer scope regardless of includePrivacy (networkFilters.ts 262-287); includePrivacy:false
  only changes the unqualified `packets` string, which fetchObserverRegionSummary never uses
  (it uses packetsAlias('p')). One scoped base per group preserves every fence.
- Join fan-out: core.node_identity_aliases.source_node_id is PRIMARY KEY (migration 036) →
  LEFT JOIN is 1:1; COUNT(*) totals over a joined base equal the original unjoined counts.
- TZ: fixture and DEV sessions are UTC → date_trunc('hour'/'day') ≡ time_bucket('1 hour'/'1 day').
  Consolidated SQL keeps each part's ORIGINAL bucket expression and ORDER BY anyway.
- Ordering/casts: CTE counts stay bigint for ORDER BY (text casts only inside jsonb_build_object,
  mirroring fetchLegacyAggregateChartParts); original tie-order non-determinism preserved
  (pt ORDER BY count DESC only) — before/after comparison normalizes tie order.
- Aggregate switch: the six parts remain individually served by aggregateReadPartsPromise when
  reads ON; the consolidated legacy counts promise is LAZY (created only when a legacy fallback
  is actually invoked) so reads ON never starts the legacy scan. Observer scope always legacy
  (aggregatePartsPromise null when observer set) — unchanged.
- Shadow path untouched: fetchLegacyAggregateChartParts / scheduleAggregateShadow /
  compareAggregateShadowRows keep their current SQL and callers.
- fetchObserverRegionSummary stays exported with identical SQL/output (public repository API;
  statsService.test mocks it). fetchChartsData's region summary now comes from the consolidated
  region query whose region_summary CTE replicates its exact expressions; equivalence proven by
  statsSummary.integration.test.ts deepEqual (string counts, ::text timestamps, row order) and
  fixture before/after digests.
- Row shapes: jsonb parts (ISO timestamp strings, text counts) are the SAME shapes aggregate
  reads already produce; every computeChartsData consumer verified Number()/fmt*()-safe
  (statsService 271-445). orSummary keeps {iata,packets_24h,packets_7d,active_observers,
  observers,last_packet_at} with text values and ORDER BY packets_7d DESC, iata ASC.
- Abort/asOf: all consolidated queries go through chartQuery → bindChartSnapshot pins every
  NOW() to one aggregateAsOf; any failure aborts chartAbort and cascades to siblings (unchanged).
  Consolidated queries return 1 row « maxResultRows 100k. Budget source attribution logs
  mesh.packets (first FROM).
- Test couplings: statsRepository.test.ts 'WITH prefix_counts' fragment must become
  'prefix_counts' (CTE no longer first); the observer-fallback fragment
  "time_bucket('1 hour', p.time) AS bucket, COUNT(*)::int AS count" is preserved verbatim in the
  consolidated counts SQL (downstream CTEs alias the base as p); percentile_cont p.rssi/p.snr
  expressions preserved (signal CTE aliases base as p). All semantic assertions (no LIMIT 50000,
  network filter present, true medians, LIMIT 10, alias canonicalization, no NOW() after binding,
  aggregate switch counts) remain enforced.
- Fixture probe safety: fetchChartsData may backfill multibyte facts (WRITE) if coverage missing;
  probe must first verify multibyte_path_fact_state readiness at the pinned asOf/generation and
  only then run the real repository path read-only. Before = original 17 SQL texts verbatim
  (saved probe), after = real createStatsRepository.fetchChartsData, same pinned asOf, worker
  role via superuser SET ROLE, aggregates OFF.

## Stage 2 batch 1 chart consolidation — measured before/after on the owned fixture

- Implementation: four consolidated same-snapshot queries replace fifteen mesh.packets scans in
  fetchChartsData (statsRepository.ts): fetchLegacyChartCounts (six switchable parts, one 7d scan,
  LAZY — never started when aggregate reads serve them), fetchRadiosAndTotals (rh/rd/totals, one
  7d src-alias scan), fetchPathHashParts (pc+widths, one 24h unnest), fetchRegionReceptionParts
  (orSummary/orSeries/diversity/signal, one 7d rx-alias scan). Multibyte fact summary/trend and
  the coverage check are unchanged (different table). Shadow path, fetchObserverRegionSummary
  export, aggregate switches, abort cascade and pinned asOf unchanged.
- Evidence: plans/lean-runtime/qualification/stage2-chart-consolidation.eDGS7E (private):
  probe.mjs (pinned Date at fixture horizon 2026-11-19T09:39:05.726Z, generation 41, worker role
  via superuser SET ROLE, planner rpc=4/ecs=512MB, aggregates OFF, STATS_DB_QUERY_CONCURRENCY=1),
  before.json (original code from fresh worktree /tmp/opencode/chart-before-wt at f364035,
  node_modules symlinked; budget raised to 600 s FOR BASELINE CAPTURE ONLY so the pre-fix rows
  could be collected — the 60 s budget failure under load is the bug being fixed), after.json
  (new code under the REAL 60 s budget / 120 s timeout), compare.mjs + comparison.json.
  Facts coverage verified ready beforehand (covered_from 2026-11-10T23:04:44Z <= horizon-8d) so
  no backfill write occurred; fixture untouched (read-only probe).
- Equivalence: ALL 16 normalized parts IDENTICAL for broad ('ukmesh') and observer
  (15C00C48…CB8D) scopes — normalized sha256 a9eff528c0e2ba95… (broad) and 1c231b1bc0eb2baf…
  (observer); normalization applies exactly computeChartsData's Number()/ISO conversions and
  deterministic tie ordering. statsSummary.integration.test.ts deepEqual (string counts, ::text
  timestamps, row order, observer alias scope) passes through the new consolidated region query.
- Timing (warm fixture cache, serial concurrency=1): broad 18 queries/40,089 ms -> 7 queries/
  26,093 ms (-35%), max query 14,879 ms (region/reception); observer 18/10,880 -> 7/4,317 (-60%).
  After breakdown: coverage 44 ms, radios+totals 2,557, paths 997, region/reception 14,879,
  counts 2,645, fact summary 3,259, fact trend 1,708. The soak's cold+concurrent-ingest baseline
  was 78 s total (budget failure at query #12); the consolidated path passes the real 60 s budget
  warm with 34 s headroom. Concurrent-ingest proof follows in the canonical Stage 3 session.
  If cold-soak margin proves thin, the recorded next lever is merging radios+totals into the
  region/reception scan (both always-on 7d alias-join scans) — not raising limits.
- Tests: statsRepository.test.ts fragments updated for the consolidated CTE structure
  ('WITH prefix_counts' -> 'prefix_counts'); all semantic fences still asserted (no LIMIT 50000,
  scoped network predicate, true percentile medians, top-10, alias canonicalization, no NOW()
  after binding, aggregate-switch call counts). New regression test asserts 4 packet scans +
  2 fact reads with reads OFF, zero legacy counts scans with reads ON, and single shared scans
  per group. Stats unit suites 40/40; full backend npm test result recorded on completion.
