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
