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
