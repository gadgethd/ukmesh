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
