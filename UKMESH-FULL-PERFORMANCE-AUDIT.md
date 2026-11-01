> Deployment history: sections A–O describe the audit and production cutover to
> `00d99bdd5f94de2a30f12a91e515820a48cc7c30`, observed through 2026-11-01 00:47 UTC.
> The subsequent GitHub reconciliation is recorded in section P. The merged
> source retains newer upstream code/dependencies and is not identical to that
> deployed release. This document does not claim a subsequent production rollout.

# UKMesh — full-system performance, correctness and optimisation audit

> Latest production status: section O supersedes the earlier pending/degraded follow-up status in section N.


Date: 31 October 2026 (Europe/London). Repository: gadgethd/ukmesh. Audited checkout: /home/ben/ukmesh/meshcore-analytics, HEAD e92c42f. Source locations in the historical audit sections are repository-relative and refer to the audited checkout at that stage. Section O records the exact final release and evidence.

**Final follow-up status, 1 November 2026:** F01–F24 are covered by the deployed signed release at commit `00d99bdd5f94de2a30f12a91e515820a48cc7c30` and the live broker/alert configuration. Section O records final qualification and practical limits. Sections A–N retain the original audit and earlier deployment history.

## A. Executive summary

UKMesh has substantial reliability infrastructure: durable MQTT acknowledgements, bounded application queues, transactional ingestion, canonical identities, privacy generations, separate analytical connections, leased background work and bounded browser caches. These mechanisms are active, but their presence does not establish complete correctness. This investigation reproduced integration failures and performance waste that earlier tests and documentation did not detect.

**Twelve production-code corrections were implemented locally**, covering cache scans, midnight aggregates, intake capacity, two privacy-sensitive response caches, alert durability and storage recovery, detached flush failures, fresh database startup, message snapshot aggregation, RAM/SQL DTO parity, and two tagger failures. Supporting regression tests and a reproducible cache benchmark were added. A stale migration expectation was also corrected. Existing unrelated work was preserved.

The strongest measured improvements are:

- At a 10,000-entry cache, 10,000 overwrites fell from a conservative repeat-baseline median of **2,378.84 ms to 41.11 ms: 98.27% less elapsed time**, or 57.87 times the component write throughput. This is a cache benchmark, not a system-wide speedup.
- On the same 100,000-row PostgreSQL fixture, five interleaved runs of the 50-message snapshot query fell from **399.06 ms to 207.69 ms median execution time: 47.95% less**. Actual result rows matched under one repeatable-read snapshot.
- Twelve concurrent top-advert requests went from twelve aggregation-loader invocations to one in the controlled route regression. Authority checks add small queries; this is not a claim that total query count fell twelvefold.
- A batch spanning two networks and two dates previously produced only two daily maxima; it now produces all four and preserves them on replay.
- A completed 200-message tagger overlap could permanently hide a newer message. Candidate selection now returns the new work and leaves retry-owned work with its retry queue.

**This is not a clean release sign-off.** Dependency scans found critical advisories in locked MapLibre and proxy-addr versions, with applicability discussed below. Browser journeys, asset budgets and CSS guardrails have outstanding failures. Redis publisher failure handling, deployment query timeouts, broker capacity and long-lived bookkeeping deserve further work.

Every major repository subsystem received an architectural/source assessment; the coverage and limits are explicit in section C. This is not a claim that every line, endpoint plan, production data distribution or failure combination has been exhaustively exercised. Production database statistics, production load limits, whole-host savings, real disk fsync throughput and a long browser soak remain unmeasured. During the original audit, no production connection, deployment, commit, push or PR was made. The subsequently authorised deployment is recorded in section N.

## B. Architecture and intended behaviour

### Repository and execution boundary

The supplied workspace directory contains several projects and deployment archives. The requested Git remote belongs to meshcore-analytics. Its initial status was main, four commits ahead of and 221 commits behind the **cached** origin/main reference, with numerous existing modifications and untracked map assets. This comparison does not establish the current remote head. No checkout, reset, merge or remote synchronisation was performed.

The inventory contained 932 tracked files. The audited source included the existing local changes. Tests and builds ran in /tmp/ukmesh-full-audit-20261031 using an isolated copy. Locked dependencies were installed in separate scratch directories; the deployed checkout's node_modules and dist directories were not modified. Final source parity was checked for all 27 audit-owned source/config/test files. In particular, the pre-existing db/index.ts changes were verified before applying the message-query addition. The two pre-existing ws/server.ts edits were left alone. The pre-existing CI workflow edits were also retained around the added fixture setup.

### Runtime domains

1. **Backend:** Node/TypeScript, Express HTTP APIs and operator pages; MQTT ingestion; PostgreSQL/Timescale access; Redis-backed live delivery; owner authorisation, forwarding and path orchestration.
2. **Frontend:** React, Zustand stores, MapLibre and Deck layers, charts, virtualised feeds, scoped caches, service worker, owner and public pages.
3. **Observed links:** TypeScript producer and Python worker, with bounded Redis v3 admissions, leases, retries, completion identities and rebuild coordination.
4. **Path intelligence:** worker-thread resolution, shared physical/path logic, bounded contexts, historical packet paths and leased hourly model publication.
5. **RF calculation:** vendored HopReach Go services; terrain caching, spatial indexing, progressive Standard/Precision output, checkpoints, nightly and on-demand calculations.
6. **Enrichment and operations:** tagger, spam analysis, privacy rematerialisation, health/retention, synthetic monitoring, alert delivery, backups and recovery.
7. **Infrastructure:** authenticated Mosquitto, TimescaleDB, Redis, Nginx/Anubis and observability services. Split-host Compose assumes some data-plane services are external.

Separate Beacon, Discord, radio-bot and independent health-check repositories in the parent workspace are not part of this repository audit. Their integration boundaries and pinned container/source configuration were inspected where referenced, not their entire external implementations.

### Packet lifecycle

The [MQTT 3.1.1 specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf) defines QoS 1 acknowledgement and duplicate-delivery behaviour. It does not make RF observations exactly once. The [MQTT.js callback contract](https://github.com/mqttjs/MQTT.js#handlemessage) documents sequential callback processing; this checkout deliberately changes its acknowledgement boundary.

A broker delivery enters MQTT.js and the installed durable intake hook. The hook advances protocol parsing so PINGRESP can be processed, while retaining QoS 1 PUBACK at the wire boundary. It holds at most eight active jobs by default, serialises each topic, and admits bounded item/byte totals. Topic parsing derives the observer and network; upstream identity claims cannot grant owner authority.

JSON, numeric domains, framing and path metadata are checked. Valid wire content gets a SHA-256 identity excluding mutable routing/path bytes; observation identity additionally includes observer, topic/network, hop/path/route width and direction. Adverts must pass signature verification before mutating advertised identity. Received time is application time; an upstream timestamp is not substituted as a trusted database arrival timestamp.

In durable mode, entries enter a batch. One transaction acquires the writer advisory lock in a separate command, receives the next statement snapshot, classifies against the privacy fence, claims observation identities and writes packets plus associated path, sighting, aggregate and forwarding effects. Retry arbitration prevents an ambiguously committed batch from repeating its historical effects. PUBACK follows successful persistence; failure leaves QoS 1 work unacknowledged.

In optional RAM mode, admission first reserves bounded space in a Redis Stream with a stable UUID and delivery record. PUBACK follows admission, before PostgreSQL. The writer later commits through the same core, acknowledges the stream and retains committed overlap for 120 seconds. Readers capture overlap before SQL and reconcile by stable IDs under current privacy authority. Redis loss before PostgreSQL commit can therefore lose acknowledged observations; this mode does not have durable mode's failure guarantee.

Committed packets feed scoped SQL reads, Redis live messages and independent link/model work. Initial WebSocket state and recent reads use privacy generations and bounded projections. The frontend applies batched store changes and renders bounded lists/maps; polling supplies data that changes independently of the socket. Predicted RF geometry is separate from observed link evidence.

## C. Subsystem-by-subsystem audit

“Verified” below describes the stated test or inspection, not an assertion of production-wide correctness.

| Subsystem | Implementation inspected | Verification and conclusion | Limits |
| --- | --- | --- | --- |
| Startup, shutdown and configuration | index.ts; platform/config; lifecycle coordinator; Compose; Dockerfiles | Startup ordering, shutdown stages, shared deadline and drain gates inspected. Actual held-SQL RAM SIGTERM/restart/drain test passed. Intake capacity wiring fixed. | No deployment restart; not every standalone worker's termination was fault-injected. |
| MQTT and broker | mqtt/client.ts, durableIntake, durableHandler, topic, statusTelemetry, connectionMonitor, observationIdentity; broker config | Real TCP/MQTT.js test keeps keepalives alive and sends no early PUBACK. Error, old-session, capacity and telemetry tests pass. Current library is MQTT.js 5.15.2. | No sustained production broker replay, resize qualification or reconnect storm. N greater than one remains rejected. |
| Decoding and untrusted input | decodePacket, channelRegistry, envelopeSafety, identityBinding, spamDetector, packetInput | Framing, multibyte widths, canonical hash and signature gates reviewed; unit fixtures pass. Decode benchmark repeated three times. | ACK fixtures do not profile real advert signature cost or every malformed packet distribution. |
| Transactional persistence | db/packetBatch.ts and migrations 059/060/062/068 | Real PostgreSQL atomicity, lock contention, replay, compressed-history coverage and privacy tests pass. Midnight and immediate-flush regressions fixed. | No production WAL/fsync or very large chunk measurement. |
| Schema, indexes and lifecycle | base.sql; migration runner and SQL inventory; dataLifecycle; health retention; maintenance scripts | Fresh database migrations, checksum/rollback rules, indexes and Timescale policy creation exercised. Missing tag read dependency fixed. Actual EXPLAIN ANALYZE used on two read shapes. | Every query/index has not been planned at tens of millions of rows. No production bloat/autovacuum inventory. |
| RAM ingestion | ram/protocol, store, writer, liveRead, publicSource, runtime | Stable-ID handoff, scoped paging, aliases, privacy flips, actual shutdown/recovery and four real ACL/headroom/quarantine gates pass. Tag DTO regression fixed. | Redis-loss recovery beyond persisted data cannot be guaranteed. No new whole-stack durable/RAM efficiency comparison. |
| HTTP API families | route registry/contracts; nodes, misc/feed, stats, pathing, topology, regions, timeline, RF validation, exports, telemetry, radio, owner, spam, retired coverage routes | Contract generator covers 71 API and 11 operator routes. Route/unit tests exercise scopes, DTOs, input limits and overload handling. Privacy caches fixed. | HTTP throughput was not measured for every route; some SQL suites require separate opt-in adapters and remained skipped. |
| SQL read and analytical services | db getters; repositories/nodes, networkAnalysis, productFeatures, pathEvidence; statsRepository/statsService | Time filters, aliases, pagination, combined topology input, rollups, snapshot caching and heavy-work admission inspected. Page-before-aggregation optimisation measured and parity-tested. | Only two SQL query shapes received a sizeable new EXPLAIN fixture. No complete endpoint p50/p95/p99 campaign. |
| WebSocket and live fan-out | ws/server, limits, privacy, initial state and client queues | Real current-authority delivery/initial-state integration passed. Admission, pending handshake, queue and slow-client protections reviewed. | Redis publication promises and offline queues remain an outage concern. No full client-count saturation test. |
| Path resolution and models | path-core; path-beta/sharedDecoder/resolver/workerPool/resolveCache; path-lazy; path-learning/rebuild/deltaPublication | Bounded priority queues, timeout/replacement/abort, multibyte resolution, physical gates and publication fences exercised by tests. Cache write scan fixed. | Model accuracy over new real RF data was not recalibrated. Two-second/750-ms test budgets can be sensitive to host contention. |
| Observed-link queue and Python RF | queue/linkQueueV3, publisher; link_queue_v3.py, worker, terrain/loss and queue tooling | 44 Python unittest cases passed with real GDAL and the owned Redis fixture. Lease expiry, stale tokens, accounting and worker outcomes covered. | No field-scale terrain download/throughput run or real GPU measurement. |
| HopReach | vendored Go commands, compute, coverage, demgrid, propagation, corescope, simulator and configuration; RF docs | Entire go test ./... passed. Indexed compute/parity and progressive/checkpoint tests are active. Separate observed evidence boundary inspected. | Historic UK-4,600 speed/memory claims were not re-benchmarked; no GPU hardware qualification. |
| Owner security and forwarding | ownerAccess/session/grants/ACL reconciler, repositories, forwardingOutbox, packet-share/public-feed delivery, outboundWebhook | Verified grants remain the authority; broker logs remain observations. Session revocation, CSRF, private-feed namespace and bounded delivery tests reviewed/passed. | No real owner login, ACL reload, remote packet relay or external webhook delivery was attempted. |
| Privacy and spam | privatePrefixCache, publicBatch, networkFilters, rematerialisation; spam analyzer/cluster/repository | Generation fences and real rematerialisation/revocation tests exercised. Cached response and tagger egress gaps corrected. Spam budgets/leases and sanitisation reviewed. | Already delivered bytes cannot be recalled. Statistical spam accuracy was not measured against labelled production traffic. |
| Tagger | tagger_worker.py, polling SQL, retry state and external-call path | Actual SQL reproduced overlap starvation and retry ownership failures. Mocked classification with a real DB reproduced private retry egress; six new tests now pass. No classifier service contacted. | Live classifier rate limits, cost, latency and semantic accuracy were not benchmarked. |
| Frontend state/data | App, hooks/useNodes/useLinkState/useWebSocket/useAppMessageHandler/useVisibilityPoll/useMessageTags; scoped caches/request limiter | 167 tests pass, six skip. Bounded feeds/arcs, one-frame batching, scope epochs, cache disposal and hidden-page polling inspected. | Long-session heap retention and low-power device frame times not measured. |
| Frontend maps/pages/assets | MapLibre lifecycle and RF overlay/protocol; topology, feed, owner, stats and regions; Vite/SW/CSS | Build succeeds. Browser run progressed after local library extraction; 15 journeys passed, five failed before the stop limit. Asset/CSS gates fail. | Failed selectors/fixtures need reconciliation before diagnosing them as product defects. No frontend source was changed by this audit. |
| Operations and deployment | health/status, metrics, synthetic monitor, alerts, logging/rules, Nginx, backup/restore/operator scripts, Compose/CI | Alert queue reconnected and SIGKILL, full-capacity, dead-letter and filesystem-recovery regressions pass. Script checks and fixture Compose render pass. | No backup restore drill, real alert notification, production scrape or live infrastructure change. Dependency and browser gates are not green. |

## D. Existing optimisation verification

The PRs were read through GitHub and relevant local history examined, including d3d7c2b (#122), e0a27db (#125), 2e875c4, ec5be34, cbf78a8, e53ce7f and later recovery/reconciliation changes. Both PRs are merged according to current PR metadata. Their bodies retain older “not merged/draft” statements; those statements are stale. Their historical benchmarks are not results of this investigation.

Sources: [PR #122](https://github.com/gadgethd/ukmesh/pull/122), [PR #125](https://github.com/gadgethd/ukmesh/pull/125).

| Earlier optimisation or invariant | Current integration and evidence | Verdict |
| --- | --- | --- |
| Acknowledge QoS 1 after persistence | Current intake holds actual PUBACK; wire test and failure tests pass. | Active for supported QoS 1 durable traffic. |
| MQTT connection pool/shared subscriptions | assertSupportedIngestConnections rejects values greater than one before connection. | Deliberately gated; unused shared topology is not an available performance gain. |
| Release parser without early acknowledgement | Later durableIntake hook supersedes the PR-era serial callback path and hooks MQTT.js private _sendPacket. | Active and wire-tested on 5.15.2; dependency upgrades need the wire gates. |
| Bounded concurrency | Eight active jobs, but at most one per topic. | Active across topics; does not parallelise one observer packet topic. |
| Configured queue bound | Packet-batch queue read the setting; intake admission still used its separate default. | Incomplete integration, fixed by F03. |
| Short/adaptive flushing | 10-ms setting is read; singleton and eight-topic experiments show different batching/latency trade-offs. | Active, not an automatically optimal delay. Default retained. |
| Batch duplicate collapse and replay | Full observation identity, same-client lock-before-mutation, natural retry fallback and UUID ledger are used. Real contention/replay tests pass. | Effective within the documented identity/retention boundary. |
| Best-effort follow-ups outside commit | Separate bounded follow-up count is active. | Helpful, but not a transactional exactly-once guarantee for every downstream effect. |
| RAM admission/headroom/retention | Lua reservations, noeviction/headroom checks, per-entry commit timestamps, quarantine, reclaim and drain are integrated. | Real operational and recovery gates pass; early ACK still depends on Redis survival. |
| SQL/RAM public read parity | Stable UUID overlap and policy checks are active; later tags broke null-field parity. | Regression fixed by F10; real wide parity test passes again. |
| Durable forwarding/alerts | Packet forwarding outbox is connected. AlertDeliveryQueue existed but receiver used a separate memory chain. | Alert integration was ineffective; F06 reconnects it. |
| Stats snapshots, combined topology SQL, analytical isolation | Snapshot generations, single-flight/admission and combined topology inputs are used; separate analyticsPool exists. | Active by code/tests; Compose's 15-minute OLTP timeout weakens the intended isolation. |
| Native decoder/multibyte support | Current decoder path is used and tested. Three ACK microbenchmarks ranged from -6.5% to +10.4% against the bundled legacy simulation. | Correctness integration active; no repeatable decoder speedup established here. |
| RF spatial index and progressive output | CPU compute, conservative superset/parity and checkpoint tests pass across the Go suite. | Active; historical raster speed/memory ratios were not independently repeated. |
| Frontend budgets/caches/frame batching | Runtime bounds and scheduling are used; asset/CSS check commands exist. | Bounds are useful; existing build output fails the asset/CSS release gates. |

## E. Findings register

Evidence labels: **reproduced** means an executed failing fixture or benchmark; **confirmed source** means a directly traced code/config behaviour; **risk/qualification gap** is not a demonstrated production incident. “Fixed” means local source only.

### F01 — Medium — Full cache scan on every write — fixed, reproduced

- Location: backend/src/cache/boundedTtlMap.ts:84 and :133; path-beta/resolveCache.ts:28.
- Observed/expected: every write scanned the timestamps of every cache entry, even at a fixed clock with no possible expiry. Ordinary unexpired writes should not inspect the whole cache.
- Cause/impact: unconditional sweep in set made cache population/churn quadratic or proportional to cache size per update. The path invalidation cache allows 50,000 entries and is written on ingestion.
- Evidence/resolution: five-run cache experiments; add a conservative earliest-expiry bound, recomputed when necessary. Preserve TTL, byte accounting, LRU, cold expiry and clock-change semantics. Regression tests and benchmarkBoundedCache.ts added. This is not proof of a particular production CPU saving.

### F02 — Medium — One daily maximum lost across a date boundary — fixed, reproduced

- Location: backend/src/db/packetBatch.ts:592; packetDailyBoundary.integration.test.ts:10.
- Observed/expected: five observations across two networks/two UTC dates produced two daily rows instead of four.
- Cause/impact: DISTINCT ON network collapsed candidates across dates before the daily upsert, silently omitting valid daily maxima.
- Evidence/resolution: real PostgreSQL failure before the fix; select/order by network and date. Replay still produces four correct maxima and no extra packet effects. Database timezone in the fixture was UTC.

### F03 — Medium — Queue configuration did not control intake admission — fixed, confirmed source and regression

- Location: mqtt/client.ts:316 and :429; mqtt/ingestConfig.ts:11; durableIntake.ts:94.
- Observed/expected: MQTT_INGEST_QUEUE_MAX affected the batch queue but not jobs waiting in intake behind topic serialization.
- Cause/impact: client startup passed only the depth callback, leaving the intake at 1,000 items regardless of the configured count.
- Resolution/evidence: pass validated configured capacity into intake; a capacity-two regression refuses the third job and releases no early PUBACK. Invalid capacity settings now fail explicitly rather than being silently clamped/fallen back. The independent 32-MiB intake byte ceiling remains.

### F04 — High — Top-advert privacy cache and aggregation stampede — fixed, reproduced

- Location: api/routes/nodes.ts:87 and :192; api/routes/topAdverts.test.ts.
- Observed/expected: the same URL returned a revoked node from a one-hour cache and advertised public HTTP caching. Twelve concurrent cold requests invoked the aggregation twelve times.
- Cause/impact: cache key lacked privacy generation, the Map had no implemented count/byte bound, and there was no single-flight coordination.
- Resolution/evidence: generation-specific bounded cache, final generation check, at most 32 distinct loads and shared in-flight work. HTTP Cache-Control is now no-store. Both revocation and in-flight-change tests pass. This intentionally changes caching behaviour to enforce privacy; lightweight authority queries remain necessary.

### F05 — High — Observer activity remained cached after privacy changes — fixed, reproduced

- Location: stats/statsService.ts:766; stats/statsService.test.ts.
- Observed/expected: an observer removed by the next privacy generation remained in the network-only cache.
- Cause/impact: this cache lacked the generation key and publication fence used by other stats caches.
- Resolution/evidence: scope by generation and reject a result if authority changes while the query is running. Tests cover both cached and in-flight revocation. Public schema is unchanged.

### F06 — High — Alert durability implementation was disconnected; storage errors were sticky — fixed, reproduced

- Location: workers/alert-receiver.ts:151, :181, :234 and :268; workers/alertDeliveryQueue.ts; docker-compose.yml:971.
- Observed/expected: receiver forwarding used an unbounded memory Map/promise chain, despite an existing fsynced queue and tests expecting its outbox. Accepted forwarding intents should survive restart and refuse excess work.
- Cause/impact: competing old/new implementations; a rejected archive write also poisoned all later writes. Failures could lose forwarding, retain completed state, bypass disk recovery and report misleading readiness.
- Resolution/evidence: connect DurableAlertQueue; persist intent before HTTP 202; configure a 1,000-item default; return retryable 503 on capacity/storage failure; initialise before listening and close on shutdown. A later archive attempt can recover after an earlier error. SIGKILL/restart, full queue, bounded retries/dead letters and transient EISDIR recovery pass. Invalid JSON remains 400. Dead-letter storage still needs a separate operational lifetime policy.

### F07 — High — Immediate batch failures could trigger global shutdown — fixed, reproduced

- Location: db/packetBatch.ts:258; packetBatch.test.ts.
- Observed/expected: with immediate flushing, the packet promise rejected correctly but the detached flush promise also rejected unhandled, causing a child process to exit nonzero.
- Cause/impact: enqueue called void flush without a rejection handler; this also affects threshold-triggered flushes. A handled poison packet or database error could unnecessarily shut down ingestion.
- Resolution/evidence: consume/log the detached failure while preserving rejection of each affected packet for intake/quarantine. Child-process regression changed from exit one to zero.

### F08 — High — Fresh backend reads depended on optional tagger startup — fixed, reproduced

- Location: db/index.ts:906, :995, :1144 and :1204; new migrations/068_message_tags_read_dependency.sql:1.
- Observed/expected: after base schema and all original migrations, an actual public read failed with relation message_tags does not exist. The backend should work when optional external classification is disabled.
- Cause/impact: mandatory read joins relied on DDL executed only by the Python tagger; ordinary packet/feed reads and initial state could fail after a clean reset.
- Resolution/evidence: additive migration matching the tagger's existing table shape. Fresh-database real getters return empty collections without starting the tagger. Migration numbering avoids known 063–067 names in cached remote history. No historical migration was rewritten and no production schema was changed.

### F09 — Medium — Message snapshot aggregated beyond its returned page — fixed, reproduced

- Location: db/index.ts:922 and :958; packetDailyBoundary.integration.test.ts:70.
- Observed/expected: requesting 50 messages aggregated observer/count information for every distinct message in the 24-hour candidate set.
- Cause/impact: page limit followed the expensive sightings aggregation and joins. Work grew with the whole window.
- Resolution/evidence: materialise selected representatives before aggregation, including captured RAM overlap outside the ordinary page. Preserve all sightings for selected hashes. Five interleaved actual plans, same snapshot/result equality, a multi-observer page test and the real RAM/SQL parity suite support the 47.95% fixture execution reduction. Candidate selection still scans/sorts its source window. This getter serves initial WS messages; the number is not a measured HTTP response speedup.

### F10 — Medium — RAM/SQL handoff changed tag DTO shape — fixed, reproduced

- Location: ram/liveRead.ts:124 and :215; ram/liveStore.test.ts; db/recentRead.integration.test.ts.
- Observed/expected: pending rows omitted tags and tag_confidence while SQL emitted null. A representative replacement also needed to preserve hash-level enrichment.
- Cause/impact: the tagging feature updated SQL projections without updating the RAM projection contract.
- Resolution/evidence: emit nullable tag fields for pending-only rows and exclude enrichment from representative-field replacement. Actual wide SQL/RAM paging/alias/privacy parity and 23 focused RAM tests pass.

### F11 — High — Completed overlap could starve tagger progress; retry limits could be bypassed — fixed, reproduced

- Location: tagger-worker/tagger_worker.py:282; test_tagger_candidates.py.
- Observed/expected: with 200 already-tagged rows inside the overlap and a newer pending message, the limited candidate page contained only completed work. Its timestamps did not advance the cursor, so repeating the poll repeated the same page.
- Cause/impact: completed and retry-owned rows were excluded only after the SQL LIMIT, or not excluded at all for exhausted retries. New messages could remain untagged and failed items could bypass the intended retry owner/limit.
- Resolution/evidence: anti-join message_tags and tagger_retry before paging. Actual extracted poll SQL on an owned database now returns the new message and does not readmit pending/exhausted retries. No classifier requests were needed for the experiment.

### F12 — High — Tag retries could send private content to the classifier — fixed, reproduced

- Location: tagger-worker/tagger_worker.py:149, :179 and :309; test_tagger_candidates.py.
- Observed/expected: retry lookup selected a packet by hash without scope, privacy or current materialisation authority. Fresh work also reused a fetched payload across external retry/backoff attempts.
- Cause/impact: admission-time visibility was treated as sufficient for later egress. A packet made private before a retry could still be submitted externally.
- Resolution/evidence: re-read and authorise immediately before each external attempt, in both main and retry paths. Require network, public visibility and completed/current generation, with no active/failed rematerialisation. Unavailable authority raises a retryable failure; private/deleted/non-taggable work is not sent. Six DB-backed tests cover allowed delivery, private denial, revocation during backoff, unavailable authority and candidate progress. The classifier is mocked; no real external notification/data transmission occurred. A request already authorised and sent cannot be recalled.

### F13 — Low — Migration expectations and CI Redis fixture were stale — fixed, reproduced/source-confirmed

- Location: db/packetBatch.integration.test.ts:135; .github/workflows/ci.yml backend Redis test setup.
- Observed/expected: the test expected five remaining migrations after its deliberate rollback, but the checkout contained additional shipped migrations.
- Cause/impact: stale test expectation prevented subsequent assertions from running; it did not demonstrate that migration execution itself failed.
- Resolution/evidence: retain the explicit ordered assertion, update it to the shipped list plus 068, and run the real migration/ingest gates. Both the original failure and successful retest are recorded. The CI Redis service also had no finite maxmemory despite RAM admission requiring one. A guarded loopback-only CI setup now configures 128 MiB and noeviction before tests; unrelated existing workflow edits were preserved.

### F14 — High — Redis publisher queues and rejected publication promises — open, confirmed source

- Location: ws/server.ts:397, :709, :725 and :739; queue/publisher.ts:19; platform/config/redis.ts.
- Observed/expected: publisher clients use default ioredis offline queuing; WS broadcast discards publication promises without a rejection handler.
- Cause/impact: application/client queue bounds do not bound commands retained inside ioredis. Under an outage, memory/retry work can accumulate and an eventual rejected publication can reach the process-wide unhandled-rejection shutdown handler.
- Resolution/status: qualify bounded publisher commands, explicit error handling and the desired drop/replay policy. Not changed in this audit; Redis-outage memory growth and wire fan-out recovery were not load-tested. No quantified memory-leak or outage claim is made.

### F15 — High — Compose gives OLTP queries a 15-minute timeout — open, confirmed configuration

- Location: docker-compose.yml:120; platform/config/database.ts; db/index.ts pool setup.
- Observed/expected: the backend service explicitly sets 900,000 ms, overriding the 30,000-ms application default. Unsignalled OLTP calls can therefore retain connections much longer than the default suggests.
- Cause/impact: long analytical allowances extend to the shared API/ingest pool despite an analytical pool. Config is active; it is not a dead setting.
- Resolution/status: inventory remaining long OLTP callers, move qualifying work to controlled analytics and canary a shorter OLTP timeout. Do not blindly shorten it before endpoint qualification. No production setting changed.

### F16 — High — Broker and parser admission boundaries need qualification — open, confirmed configuration/risk

- Location: mosquitto/mosquitto.conf:10 and :29; mqtt/client.ts:334; durableIntake.ts:76.
- Observed/expected: the source broker config enables debug logging and limits queued messages to 10,000. It does not set an explicit packet-size limit. Application payload/queue checks occur after MQTT.js has parsed/allocated the packet.
- Cause/impact: application limits cannot prevent all pre-handler allocation; authenticated untrusted publishers can impose larger parsing/broker work. A finite persistent-session backlog can also overflow during a sufficiently long outage. Debug logging adds work on the ingest path.
- Resolution/status: qualify broker-side size/byte limits, queue capacity, overflow visibility and logging on an owned broker. Not applied to the mounted/live broker configuration; no oversized-load attack or production backlog test was run.

### F17 — Medium — Bookkeeping lifetime and disk growth are incomplete — open, confirmed source/risk

- Location: db/migrations/059_ingest_observation_ledger.sql:5; 043_canonical_advert_counts.sql:6; health/status.ts:50; tagger-worker/tagger_worker.py:72; workers/alertDeliveryQueue.ts.
- Observed/expected: packet_paths intentionally has no retention. Ledger, counted-advert and tagger bookkeeping and alert dead files lack an equivalent bounded operational lifecycle in the reviewed retention inventory.
- Cause/impact: some state grows with lifetime traffic even after content-bearing packets expire. This is not justification to delete a correctness ledger or the intentionally retained path archive.
- Resolution/status: measure relation growth, preserve replay/reader watermarks and historical aggregate semantics, then define archival/compaction and dead-letter disposition. No deletion or pruning implemented.

### F18 — Medium — Supported observation identity has remaining edges — open, confirmed source/ambiguity

- Location: mqtt/client.ts:820 and :837; mqtt/observationIdentity.ts:34.
- Observed/expected: a non-decodable packet without a usable upstream hash gets a new random identity. Restart DUP probing cannot match that new identity. RAM packet admission requires an MQTT message ID, whereas valid QoS 0 deliveries have none.
- Impact: malformed/fallback sightings can be counted again after restart, and QoS 0 packet traffic is not qualified for RAM intake. QoS 0 never had a durable redelivery guarantee.
- Resolution/status: specify deterministic fallback identity and RAM QoS policy with real broker tests. Not changed. Identical immutable ACK content can also represent repeated RF transmissions; whether to collapse those within the 120-second identity window is an explicit semantic ambiguity, not a proven defect from missing evidence.

### F19 — Medium — “Ack duration” excludes intake waiting — open, confirmed source

- Location: mqtt/client.ts:334 and :372; durableIntake.ts:52.
- Observed/expected: the timer begins when persistence work is dispatched, after any wait behind the topic queue.
- Impact: healthy-looking processing latency can coexist with seconds of broker-delivery-to-PUBACK latency. Queue depth helps but does not supply end-to-end percentiles.
- Resolution/status: add admission-to-dispatch and admission-to-wire-ack histograms with bounded labels and verify against wire capture. Benchmark percentiles here include the harness admission wait; served metric percentiles were not relabelled as end-to-end.

### F20 — Medium — Frontend release guardrails fail — open, reproduced

- Location: frontend/scripts/check-performance-budgets.mjs:29 and :36; frontend/vite.config.ts; frontend CSS files.
- Observed/expected: final output totals 3,357.3 KiB JS / 956.1 KiB gzip against 2,900 / 850; CSS 255.2 / 45.5 against 230 / 42. MapLibre matcher finds renderer and worker; the expected Recharts categorical filename is absent.
- Cause/impact: both actual total-budget excess and outdated chunk classification are present. CSS checking reports seven duplicate selector contexts and one 9-px declaration.
- Resolution/status: reconcile classification without raising ceilings, then reduce actual shipped payload and repair CSS context issues. No frontend changes were made; these failures exist before the audit fixes.

### F21 — Critical — Locked dependency advisories — open, verified inventory; application exposure conditional

- Locations: backend/package-lock.json and frontend/package-lock.json.
- Evidence: backend audit reports five affected packages: two moderate, two high, one critical. Frontend reports three: two high, one critical.
- MapLibre 6.4.0 is in the affected range for attribution sanitiser XSS; 6.4.1 is patched. Third-party attribution is relevant to map-source handling. See the [MapLibre advisory](https://github.com/advisories/GHSA-jrc7-96c5-q579).
- proxy-addr 2.0.7 is affected by the malformed IPv4-mapped IPv6 trust-subnet issue; 2.0.8 is patched. This application uses an exact-peer trust function rather than the affected short CIDR example, so the advisory alone does not demonstrate a working UKMesh IP-spoofing exploit. See the [proxy-addr advisory](https://github.com/advisories/GHSA-jqcg-44mw-7w3h).
- Backend body-parser/compression and frontend Vite/source-map-js advisories also fail the requested high-severity checks. Development-server advisories do not automatically apply to a static Nginx production build.
- Resolution/status: prioritise narrowly scoped patched dependency qualification, including attribution/worker behaviour and trust-boundary tests. No broad audit-fix/dependency rewrite was mixed into the performance changes. No exploit, compromise or deployed-image version is asserted.

### F22 — Medium — Browser qualification is incomplete — open, reproduced verification failure

- Location: frontend/test/e2e/accessibility.spec.ts:258 and :348; owner.spec.ts:47 and :105; public.spec.ts:5.
- Observed/expected: after supplying missing browser shared libraries locally, 15 tests passed; five failed before the five-failure limit, one was interrupted and 11 did not run. Failures include dialog/keyboard triggers, owner fixture labels/map polling and the 768-px navigation toggle.
- Impact/cause: selectors/fixtures and the current dirty frontend differ; missing expected DOM elements do not alone prove every named product feature is broken.
- Resolution/status: reconcile journeys against the intended current UI and rerun the remainder plus a soak. No speculative UI rewrite was made. These are not regressions caused by the backend/tagger changes.

## F. Performance baseline and methodology

### Conditions

- Eight logical host CPUs; approximately 23 GiB RAM. Shared host with uncontrolled background activity: results are component/fixture measurements, not dedicated-host capacity limits.
- Node v22.22.1; locked validation used TypeScript 7.0.2, pg 8.23.0, MQTT.js 5.15.2 and Vite 8.2.1. The initial installed pg was 8.22.0; a separate clean locked install was used for final validation.
- Owned PostgreSQL 16.11 / TimescaleDB 2.25.1 fixture: pinned CI digest, 1.5 CPUs, 1,536-MiB container cap, 128-MiB shared buffers, 128-MiB shared memory, data on a bounded tmpfs. PostgreSQL fsync defaults were not disabled, but tmpfs is **not** representative physical storage.
- Owned Redis: pinned CI digest, 0.5 CPU/256-MiB container cap, noeviction and AOF everysec. Admission/runtime tests used 128-MiB maxmemory; ACL/headroom tests used 48 MiB. AOF on tmpfs verifies behaviour, not power-loss durability or physical write cost.
- Go tests used an owned Go 1.25 container, one CPU, 1 GiB and GOMAXPROCS=2. Python tests used an owned worker-image container with current source mounted separately.
- Only audit-owned fixtures, loopback servers and synthetic identities/payloads were used. No production database, broker, Redis, website load endpoint or external classifier was exercised.

### Cache baseline

Five repetitions per phase, 10,000 entries and 10,000 overwrites, fixed clock, 60-second TTL, eight-byte weights, normal cache metrics enabled. Population happens before the timed region. Size, weight and physical expiry are asserted. Two baseline phases bracket the optimised phase; use the faster repeat-baseline median conservatively.

| Phase | Median elapsed ms | Median process CPU ms | Peak process RSS MiB |
| --- | ---: | ---: | ---: |
| Original first phase | 2,805.77 | 2,712.35 | 162.37 |
| Original repeat phase | 2,378.84 | 2,373.54 | 142.91 |
| Corrected cache | 41.11 | 40.65 | 146.59 |

RSS is a process peak including setup and runtime noise; these numbers do not establish a memory improvement. The corrected phase reduced median timed CPU by 98.29% against the repeat baseline. Warm-up/JIT variation remains visible in the individual repetitions.

### Durable intake and real SQL

Twelve runs: three repetitions of each combination below, 200 unique valid ACK observations per run, one or eight observer packet topics, all submitted as a burst. Real decoding, intake scheduling, batched SQL, ledger, paths and hourly effects execute. The client wire boundary is an instrumented adapter; a separate real TCP test validates actual MQTT.js acknowledgement behaviour.

This is **not** the entire backend/Mosquitto/WS/frontend pipeline. Advert crypto, owner forwarding, external consumers, production hardware and cross-process network costs are not included. Each run asserts exactly 200 packets, paths, ledger entries and hourly contributions.

| Topics / flush delay | Median observations/s | Median burst drain ms | Median p50 / p95 / p99 ms | Transactions | Mean batch | Median WAL bytes |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| 1 / 10 ms | 22.57 | 8,861.34 | 4,301.01 / 8,356.02 / 8,777.15 | 200 | 1 | 1,633,624 |
| 1 / 0 ms | 33.70 | 5,935.54 | 3,078.43 / 5,641.72 / 5,868.67 | 200 | 1 | 1,654,688 |
| 8 / 10 ms | 71.61 | 2,792.82 | 1,447.61 / 2,645.73 / 2,782.95 | 25 | 8 | 1,552,624 |
| 8 / 0 ms | 64.82 | 3,085.25 | 1,629.81 / 2,958.32 / 3,075.79 | 50 | 4 | 1,577,304 |

Zero delay improved the single-topic case by about 49.3%, but reduced eight-topic throughput by about 9.5% and doubled transaction count. These are **configuration experiments**, not a shipped default change. They justify retaining 10 ms pending a representative workload.

Per-run application CPU medians were 1,208.90 / 835.81 / 277.14 / 329.31 ms in the same row order. Peak application RSS medians were 116.08 / 118.42 / 113.02 / 113.67 MiB. These exclude PostgreSQL and other processes; no whole-stack CPU saving is claimed. WAL is an owned-instance LSN difference and can include fixture background work, not physical device bytes.

pg_stat_statements planning was enabled for this campaign. In the one-topic/10-ms runs, the mutation statement was planned 200 times, with total planning 859–1,063 ms and execution 2,701–3,353 ms. Planning is material but not the only cost. A larger pool cannot remove the one-topic sequencing or the global writer lock. Named/generic plans remain an experiment requiring same-snapshot/replay qualification, not an implemented speed claim.

### Query plans and data volume

The read fixture contains exactly 100,000 packet observations, 10,000 repeating hashes, one receiver, network ukmesh, every fifth observation a GroupText, and timestamps distributed over approximately 100,000 seconds (27.8 hours), not 30 days. Chunk relation size was 88,358,912 bytes in the final run. It does not represent tens of millions of rows, many observers or compressed historic query distributions.

Both actual SQL generators were captured. EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ran in one repeatable-read read-only transaction, after asserting exact before/after message-row equality. Five interleaved repetitions give:

| Query | Median plan ms | Median execution ms | Individual execution ms |
| --- | ---: | ---: | --- |
| Original 50-message snapshot | 7.379 | 399.059 | 527.359, 387.560, 407.269, 399.059, 386.983 |
| Selected-page snapshot | 6.620 | 207.694 | 206.885, 218.180, 227.966, 207.694, 195.417 |
| Recent slim packets, unchanged control | 6.161 | 7.739 | 13.888, 6.472, 7.739, 7.972, 6.438 |

Plans include suitable time/network chunk/index access and scans of genuinely broad 24-hour source windows. A sequential scan of most of a fixture chunk is not automatically an index defect. No redundant production index was dropped and no speculative index migration was created.

### Other measurements and unavailable results

The bundled decoder comparison used three valid multibyte ACK shapes, 10,000 warm-up calls and 50,000 timed iterations per phase, repeated three times. Native/canonical throughput was 19,729 / 24,257 / 29,032 packets/s; legacy simulation was 21,091 / 22,367 / 26,297. Variation and different returned work prevent a stable historical speedup claim.

Physical-I/O conclusions are unavailable because the owned database uses tmpfs; no production-scale restore or production access was used. The long browser soak, full RF benchmark and CPU flamegraphs were not collected within this audit window; they are not claimed to be impossible to run. No new defensible measurements exist for production API concurrency, physical fsync latency, disk write amplification, checkpoints under sustained disk pressure, CPU flamegraphs, GC pause percentiles, production autovacuum/bloat, a 30-minute frontend soak, GPU throughput or complete durable-vs-RAM end-to-end savings.

## G. Fixes implemented and verification

| Change | Why and minimal correction | Relevant validation |
| --- | --- | --- |
| F01 | Earliest-expiry bound avoids unnecessary cache sweeps. | Cache boundary/LRU/clock tests; five-run bracketed benchmark. |
| F02 | Daily candidate key includes calendar date. | Actual two-network/two-date inserts and stable-ID replay. |
| F03 | Pass configured item capacity to durable intake. | Capacity-two refusal and no-early-ack test. |
| F04/F05 | Scope caches by privacy generation and check before publication. Bound/coalesce top-advert work; send no-store. | Cached and in-flight revocations; twelve-request barrier-based coalescing. |
| F06 | Use existing fsynced queue; bounded admission and retryable storage errors; recover write chain. | Actual child SIGKILL/restart, queue full, dead letter, EISDIR recovery; 12 combined alert tests. |
| F07 | Observe detached flush rejection while keeping per-packet failure. | Child-process immediate-flush regression; batch retry/poison tests. |
| F08 | Add only the missing shared table migration. | Fresh migrated database and real read getters; migration rollback/checksum gates. |
| F09 | Select representative page/overlap before sightings aggregation. | Actual SQL equality/plans, multi-observer counts and out-of-page RAM capture. |
| F10 | Match SQL nullable tag DTOs and retain existing hash-level tags. | 23 RAM tests and wide actual SQL/RAM parity integration. |
| F11 | Filter completed/retry-owned candidates before poll LIMIT. | Real SQL progress and exhausted-retry tests. |
| F12 | Check current authority immediately before each external classifier attempt. | Allowed/private/unavailable/backoff-revocation tests with real DB and mocked classifier. |
| F13 | Update explicit remaining-migration expectation. | Fresh-database runner reaches subsequent verification. |

Public APIs and database contents were not reset. Deliberate externally visible corrections are the no-store top-advert response, retryable alert capacity/storage failures, nullable pending tag fields, and rejection of invalid intake capacity configuration. These are documented corrections, not silent performance trade-offs.

All changes remain uncommitted. Existing local edits to db/index.ts were preserved around the new CTE. No repository dependency versions, frontend implementation, live broker file or production database setting was changed.

## H. Before-and-after comparison

| Measure | Before | After | Defensible difference |
| --- | ---: | ---: | --- |
| 10,000 cache overwrites, conservative median | 2,378.84 ms | 41.11 ms | 98.27% less time; 57.87x component throughput |
| Same timed cache process CPU | 2,373.54 ms | 40.65 ms | 98.29% less component CPU |
| 50-message SQL execution, same fixture | 399.059 ms | 207.694 ms | 47.95% lower median execution |
| Concurrent top-advert aggregation invocations, controlled loader | 12 | 1 | 91.67% fewer aggregation invocations; authority work added |
| Daily network/date maxima in boundary fixture | 2 of 4 | 4 of 4 | Correctness restoration |
| Immediate poisoned flush child exit | 1 | 0 | Handled packet failure no longer becomes global rejection |
| Fresh read after migrations, no tagger | Missing-table error | Valid empty collection | Correctness restoration |
| Pending/committed tag fields | Missing vs null | Aligned; existing tags retained | DTO parity restoration |
| Full completed-overlap tagger candidate page | 200 completed rows; new row hidden | New row selected | Progress restored |
| Private retry classifier mock calls | 1 | 0 | Privacy boundary restored |
| Receipt retry after a resolved storage error | 503 again | 202 with persisted receipt | Recovery restored |

Trade-offs: privacy fixes perform fresh authority lookups; the tagger now rechecks before each external attempt. Alert acceptance performs fsynced durable-queue work and terminal dead files can accumulate. Snapshot selection still examines its candidate window. No measured cache RSS reduction, production throughput increase or whole-VPS saving is claimed.

## I. Remaining optimisation opportunities

Priority includes correctness/security prerequisites; expected impact below is an estimate unless tied to section F.

| Priority / type | Work | Expected impact and confidence | Complexity / operational risk | Dependencies |
| --- | --- | --- | --- | --- |
| P0 / quick qualification | Patch affected dependency versions narrowly. | Security release prerequisite; affected lock inventory confirmed, exploitation conditional. | Low–medium change; medium validation risk around maps/edge trust. | Attribution/worker tests, trust-boundary tests, build and browser qualification. |
| P0 / quick design then fixture | Bound Redis publisher queues/deadlines and handle failures explicitly. | High reliability/resource impact under outage; high source confidence. | Medium; dropping or replaying live/link work must be intentional. | Real disconnect/recovery campaign and metrics. |
| P1 / operational qualification | Protect OLTP with shorter deadlines and separate remaining long analyses. | High contention protection; configuration confirmed. | Medium implementation, medium rollout risk. | Per-route latency inventory on a representative isolated dataset. |
| P1 / operational qualification | Broker-side packet/queue byte bounds, overflow alerts and logging level. | High outage/input robustness; medium throughput benefit confidence. | Low config complexity, medium delivery risk. | Dedicated real broker tests; never alter live settings speculatively. |
| P1 / quick frontend work | Fix budget classification, then reduce actual payload and reconcile browser failures. | High client/release confidence; budget excess measured. | Medium; existing local map work must be retained. | Browser/axe/soak and current intended UI fixtures. |
| P1 / benchmark-led | Experiment with prepared/generic mutation plans and lower hot-path statement overhead. | Material planning cost measured; whole-pipeline gain uncertain. | Medium; low–medium risk after replay/fence tests. | Named statement support on the same transaction client; realistic distributions. |
| P1 / larger design | Improve single-topic throughput while preserving ordering, advert mutations and replay. | Potentially high; singleton bottleneck measured in component fixture. | High complexity and correctness risk. | Real broker control, ordering contract, durable ack qualification. |
| P2 / lifecycle design | Bound/archive ledger, tag, advert and terminal alert state without breaking provenance. | High long-term disk predictability; growth mechanism confirmed. | Medium–high; high data-integrity risk if pruned incorrectly. | Production-size read-only inventory, reader watermarks and restore/replay proof. |
| P2 / observability | End-to-end queue/wire latency and distinct database planning/commit spans. | High diagnostic value; missing spans confirmed. | Low–medium; low risk. | Bounded metrics cardinality and wire verification. |
| P2 / larger experiment | Incremental summaries for observer activity, owner history and heavy source windows. | Potentially high DB savings at large volumes; not newly measured. | Medium–high with invalidation/privacy risk. | Representative plans, exact counts/revocation and backfill strategy. |
| P2 / external-service constraint | Batch or safely parallelise classifier work if its API/quotas support it. | Could exceed the default one-at-a-time 0.5-second floor; API capability unverified. | Medium–high; external cost/rate/privacy risk. | Service contract, retries and last-check authorisation. |
| P3 / experiment only | Retune RAM batch/age, more MQTT sessions, GPU/RF parameters. | Workload dependent; no new savings claim. | High validation/operational risk. | Whole-stack resource accounting, durability acceptance and parity tests. |

## J. Scalability assessment

- **Packet rate:** one observer packet topic is sequential even with eight workers. The mutation lock also serialises database writer transactions. At larger topic counts batching amortises planning, transaction and row-side-effect costs. Adding database connections alone cannot remove these constraints.
- **Observer count:** more topics improve batch utilisation but add node/sighting work, privacy checks and fan-out. Queue depth must be understood at intake, batch, ioredis and broker layers. Application bounds alone are not the complete backpressure chain.
- **Database size:** short recent windows prune old chunks; message snapshot candidate selection and several activity/history aggregates still scale with their selected window. Retained path/ledger state grows independently of 30-day content retention. Compression changes write/read costs and must be measured on restored-scale fixtures.
- **Concurrent users:** scoped caches and single-flight reduce repeated heavy work; high-cardinality observer scopes can exhaust bounded admission. WS initial state is opt-in and has client/queue limits; budgeted overload is preferable to unbounded pool demand, but publisher failure paths remain open.
- **Frontend users:** visibility-aware polling, one-frame batches, bounded feeds and bitmap disposal limit work. Browser/map assets are still large; the demonstrated budget failure matters on lower-powered devices and slow links.
- **Degraded operation:** durable ingest withholds QoS 1 ack on persistence failure. RAM mode trades earlier visibility for dependence on Redis/AOF and retained reader overlap. Broker queue saturation, malformed identities and unsupported RAM QoS 0 weaken claims of universal replay safety.
- **Capacity conclusion:** the fixture shows important spare optimisation opportunities but does not establish a production maximum or a hardware multiplier. It would be incorrect to call 22.57 or 71.61 observations/s a production capacity limit.

## K. Correctness and reliability assessment

Verified invariants include no early QoS 1 PUBACK in the wire gate; stale sessions cannot acknowledge a new stream; item refusal does not acknowledge dropped durable work; supported duplicate/replay paths repeat no packet/path/hourly/region effect; independent transaction lock precedes the mutation snapshot; source privacy/alias/width/direction affect identity; unsigned adverts do not mutate identity; public SQL/RAM/WS reads honour fresh authority; captured overlap survives a slow read/expiry boundary; shutdown joins/recovers the held RAM write and completes physical drain; quarantine and reservations remain bounded under the operational tests.

Additional restored invariants are daily maxima per network/date, hash-level enrichment across representative replacement, alert forwarding intents surviving SIGKILL, tagger progress through completed overlap and private retry payloads not reaching the classifier mock.

Limits remain:

- QoS 0 and malformed/no-hash fallback observations do not have the same replay contract as validated QoS 1 packets.
- Identity is content/observation based. Equal ACK bytes and path/observer values are not a physical-transmission nonce. Repeated real transmissions versus duplicate sightings need an explicit product/protocol decision.
- Redis admission is not PostgreSQL durability. AOF everysec and loss of Redis before commit have a failure window.
- Public bytes already authorised and delivered, including external classification requests, cannot be recalled after a later privacy change.
- Best-effort map/link follow-ups are not universally transactional or exactly once. Rebuild/backfill and failure visibility matter.
- Long analytical work, retained bookkeeping, dependency advisories and unqualified browser journeys prevent a blanket reliability certification.
- Lack of observed reception is not proof that a transmission or physical RF link was impossible. Physical/path hypotheses and model outputs must remain distinct from positive observed evidence; this audit did not tighten impossibility gates or infer negative observations as facts.

## L. Tests, verification results and reproducibility

### Final relevant results

All Node commands below ran inside the isolated copy, with clean locked dependencies for final checks. Fixture URLs pointed only to the owned loopback PostgreSQL/Redis ports. They must not be replaced with production URLs.

| Command / check | Result |
| --- | --- |
| Backend npm test equivalent, node --import tsx --test --test-concurrency=4 over non-integration tests | **535 tests: 532 pass, 0 fail, 3 explicitly gated performance/production checks skip**, 113.48 s. |
| Backend npm run typecheck | Pass. |
| Backend npm run contract:check | Pass: 71 API and 11 operator routes. |
| Backend npm run build | Pass. |
| Final wired npm run test:ingest-integration | **Nine counted tests pass, zero fail, zero skip**, 67.55 s. Includes fresh migrations, the daily/message-page boundary regression and actual SQL/RAM parity. |
| Broader safe integration discovery, excluding operational/runtime gates supplied separately | Initially 38 counted tests: 14 pass, one fail, 23 opt-in checks skip. Failure was the real RAM tag DTO mismatch, F10. It was fixed; the same failing wide parity test then passed with zero skips. This aggregate run is not represented as originally green. |
| Actual RAM shutdown/recovery/drain test | One pass, zero skips, 148.83 s. |
| Real restricted Redis/headroom/quarantine suite | Four pass, zero skips, 3.29 s. |
| Focused RAM projection suite after tag assertions | 23 pass, zero skips. |
| Alert receiver plus synthetic-alert queue tests after filesystem correction | 12 pass, zero skips. |
| Tagger actual-SQL/egress boundary tests | Six pass, zero skips; classifier mocked, real owned DB. |
| Python worker canonical unittest discovery with real GDAL and owned Redis | 44 pass, zero skips, 2.04 s. |
| Vendored HopReach go test -p 1 ./... in owned Go container | All packages pass, including compute, coverage, propagation and simulator. Simulator package took 190.04 s. |
| Operator scripts typecheck and npm test | Pass; key permissions, symlink and stdout privacy tests pass. |
| Frontend npm test | 173 counted: 167 pass, 0 fail, six browser-only checks skip. |
| Frontend npm run build | Pass, with large-chunk warnings. |
| Frontend npm run budget | Fail, pre-existing classification/total-size issues in F20. |
| Frontend npm run lint:css | Fail: seven duplicate contexts, one sub-10px declaration. |
| Adapted local Playwright/axe journeys | After resolving local shared libraries: 15 pass, five fail, one interrupted, 11 not run at failure limit. F22 records the failures. |
| Backend / frontend npm audit --audit-level=high | Both fail; five / three affected packages, respectively. F21 records applicability. |
| Compose base + CI overlay config --quiet, /dev/null env file, synthetic variables | Pass. No deployment command used. |
| git diff --check | Pass. |
| Audited root files compared byte-for-byte with tested copy | Pass for 27 source/config/test files. |

### Failures and corrections during investigation

The initial backend run had 522 passes, one failure and three skips: the receiver test expected the unused durable outbox. Initial ingest had five passes and a stale migration-list failure. Both were investigated and corrected rather than hidden.

A loaded-host run hit the pre-existing worker-replacement timeout; the isolated five-case API/pool retest and final full suite passed. An initial coalescing regression used a 100-ms scheduling assumption and allowed one late request into the new generation. It was replaced with a twelve-arrival barrier, then the full suite passed.

An initial Python pytest invocation produced 35 passes, two failures and seven skips because conftest installs dummy GDAL functions. The repository's actual CI uses unittest with real GDAL. That canonical command subsequently passed all 44 cases, including real Redis tests. This is a harness distinction, not a terrain fix.

Browser launch initially failed on missing ATK/CUPS/audio/X shared libraries. Distribution packages were downloaded and extracted under scratch; no host packages were installed. The retried run reached real assertions and the five retained failures. API/WS dev proxies were redirected to an unused loopback endpoint, tests used stubs, and production ports/servers were not reused.

The first Compose render intentionally lacked all deployment secrets and failed interpolation. A second render with synthetic fixture variables passed. Scratch script typechecking initially lacked its dependencies; an isolated locked install enabled both checks. New benchmark typing errors and a wrong mock classifier response key were corrected before their final passing results. A later wired integration attempt ran after the ACL suite had lowered the owned Redis fixture to 48 MiB; two admissions correctly refused the default 32-MiB queue plus 32-MiB recovery headroom. The fixture was restored to 128 MiB for the final rerun. This was fixture state, not a product regression. The standard backend ingest integration script now includes the new boundary/page regression and the real SQL/RAM read parity suite. The new tagger unittest remains an explicit manual integration gate; current CI does not automatically discover tagger-worker tests.

### Reproduction material

Primary source benchmark: backend/src/tools/benchmarkBoundedCache.ts. From backend, run:

~~~sh
node --import tsx src/tools/benchmarkBoundedCache.ts
~~~

Defaults are 10,000 entries/writes and five repetitions. BENCH_CACHE_CAPACITY, BENCH_CACHE_WRITES and BENCH_CACHE_REPEATS are positive integer controls. To compare the prior implementation, use a separate isolated copy of the original cache source; do not overwrite a working checkout.

Ingest component harness and read-plan harness remain in the deliberate scratch evidence workspace; scratch dependencies/build outputs were removed, so reinstall the matching locks in an isolated copy to rerun them. They require the owned fixture and do not target production. The read comparison preserves captured original SQL, uses actual new SQL, checks row equality, and interleaves five repetitions. The cache original-source phase was repeated on both sides of the corrected phase.

Raw evidence directory: **/tmp/ukmesh-full-audit-20261031/evidence**. Significant files include:

- cache-paired-before.json, cache-paired-after.json, cache-paired-before-repeat.json;
- ingest-component-campaign.jsonl, ingest-component-errors.log;
- query-plans-final.json, read-query-before.json;
- backend-final-all-tests.log, backend-final-typecheck.log, backend-final-contract.log;
- ingest-final-wired-verified.log, integration-final-tests.log, recent-read-final-integration.log, ram-runtime-final-integration.log, ram-acl-final-tests.log;
- tagger-before-regression.log, tagger-privacy-before-regression.log, tagger-final-tests.log;
- alert-storage-before-regression.log, alert-storage-final-regression.log;
- frontend-final-budget.log, frontend-final-css.log, browser-final-tests.log;
- python-unittest.log, go-tests.log, scripts-final-checks.log;
- backend-audit-dependencies.log, frontend-audit-dependencies.log, compose-final-verify.log.

Earlier failed runs are retained as evidence. This Markdown file is the single authoritative report; scratch logs are supporting measurements, not alternative reports. Scratch is temporary storage and may not survive host cleanup. Test databases, child processes and audit-owned containers were closed/removed at completion; retained evidence contains synthetic fixture data. No unrelated containers, volumes, repository changes or files were removed.

## M. Final verdict

1. **Is UKMesh functioning as intended?** Many core invariants hold, but the original checkout had reproduced correctness/integration failures. The local fixes address the confirmed cases above. Outstanding dependency, resource-protection and frontend validation issues prevent an unconditional yes.
2. **Are previous performance fixes working?** Several are active and useful, including held acknowledgements, topic concurrency, batching, identity arbitration, bounded caches and RAM recovery. Some were incomplete or superseded: queue wiring, alert outbox integration and later tag DTO integration needed correction. Shared sessions remain gated; old PR claims are not current measurements.
3. **What was the single largest bottleneck found?** In the measured durable burst fixture, one-topic sequencing plus one transaction per observation left most batching capacity unused; eight topics used eight-row batches and substantially higher throughput. This identifies a fixture bottleneck, not the production host's dominant bottleneck. The cache scan was the largest directly corrected algorithmic hot-path waste.
4. **What was the largest measured improvement achieved?** The corrected cache reduced 10,000-write median time from 2,378.84 to 41.11 ms, 98.27%. The strongest measured database-query improvement was 47.95% lower message-snapshot execution time with equal results.
5. **What significant opportunities remain?** Publisher outage bounds/error handling, OLTP deadline isolation, broker admission controls, dependency qualification, frontend payload/browser fixes, large-window summaries, planning overhead and long-lived state lifecycle.
6. **Is substantial performance being left on the table?** Yes in demonstrated cache, paging/aggregation and batching cases. The amount of spare production hardware capacity has not been measured.
7. **What should be optimised next?** First qualify dependency patches and Redis failure handling; then measure a restored production-scale workload with safe OLTP isolation and real broker/disk backpressure before changing concurrency or defaults. Complete browser/budget qualification alongside that work.
8. **What prevents greater confidence?** Shared-host variance, small/synthetic data distributions, tmpfs storage, missing production plans/metrics, incomplete browser/soak qualification, skipped opt-in SQL adapters, no new full durable/RAM reader campaign, and no field/GPU/model-accuracy measurements.

The original audit performed no deployment, production schema/configuration change, commit or PR. Section N records the later authorised release.


## N. Authorised deployment and subsequent remediation — 31 October 2026

### Deployed release

The user explicitly authorised deployment of the twelve fixes, then authorised committing and pushing the isolated release candidate. The release branch is [audit-twelve-fixes-20261031](https://github.com/gadgethd/ukmesh/tree/audit-twelve-fixes-20261031). Its final source revision is [920336c323f8c8d7d502a0eef7d3def3eae8d064](https://github.com/gadgethd/ukmesh/commit/920336c323f8c8d7d502a0eef7d3def3eae8d064). No PR was created and the user's original checkout/branch was not reset or switched.

The clean candidate excludes unrelated uncommitted frontend, RF, broker and infrastructure work. A comparison with running JavaScript identified already deployed RF public-key projections and receiver authentication absent from the tracked baseline. These were preserved in the release. In particular, the running receiver already contained the durable queue and receipt-write recovery described by F06: its bearer authentication, `.queue` storage directory, health metrics and legacy queue-capacity setting were retained. The new capacity name remains supported. This is not a claim that all twelve defects were present in production immediately before deployment.

| Service | Immutable deployed image |
| --- | --- |
| backend; alert-receiver | ghcr.io/gadgethd/meshcore-backend@sha256:77e45a94b1ad8d043d9faf98236ba1c22c76041d2a16fbaacc978feb4e9a52b1 |
| tagger-worker | ghcr.io/gadgethd/meshcore-tagger-worker@sha256:69aa3e86092d41ebac7b77e1f830963d99102ecf456e18edb8ef7618ce162590 |

The [release run](https://github.com/gadgethd/ukmesh/actions/runs/41236713698) passed the policy, backend and tagger jobs, including critical-image scans, SPDX SBOM generation, signatures and provenance. Cosign 3.1.4 was obtained from its official release and checksum-verified. Both image signatures were independently verified against the exact release-workflow branch identity and GitHub Actions OIDC issuer; both revision labels match the commit. Other matrix jobs did not all pass: app, website, RF worker and health-check failed their critical vulnerability gate. Those failures remain release work for those components; those images were not deployed. The entire workflow is therefore not reported as green.

The signed restore receipt verified cryptographically and met the seven-day release gate (backup approximately 6.77 days old; restore approximately 5.96 days old). The live migration ledger matched all existing source checksums. Only migration 068 was pending; its `CREATE TABLE IF NOT EXISTS message_tags` and migration record were applied. The existing production tag table was preserved. This release does not reconstruct historical daily aggregates previously missed at midnight.

Only the three selected application services were replaced, using signed images and `--no-deps --no-build`. No broker, Redis, TimescaleDB, frontend or other worker was recreated. Immutable backend/tagger pins were persisted in the existing `.env`. The prior local image IDs and non-secret pin values remain available for rollback. The status JSON is an operational receipt, not a newly cryptographically signed release receipt.

Evidence is under `/home/ben/ukmesh/audit-release-20261031/evidence`, particularly `deployment-status.json`, `deployment-final.log`, `prior-*.json`, `prior-image-pins.json`, `release-backend/`, `release-tagger-worker/` and `cosign-verified-*.json`. The clean candidate is retained separately from the dirty original checkout.

### Deployment deviations and incident record

The runbook's normal full-start compatibility check was stopped before application replacement because the additional old backend started a long identity refresh. It held an open transaction; other application queries also showed relation lock waits, but a complete blocking graph was not collected, so direct lock attribution is not established. The owned compatibility container was removed. A bounded, five-second prior-image probe then compiled its actual recent packet/message read queries against the migrated schema using zero-row limits; the still-running prior backend also passed readiness. This checks schema/read compatibility without claiming a second complete cold-start qualification. Fresh-database and real SQL/RAM fixture gates passed separately.

An initial replacement attempt incorrectly relied on subprocess environment variables surviving `sudo`. Compose selected the old backend image from `.env`; the main backend remained unchanged, while the receiver briefly ran that older image. The unauthenticated HTTP probe detected the missing receiver authentication. The prior receiver image was immediately restored and its 401 response verified. One empty probe was archived while forwarding was unconfigured; it was not sent to a webhook. The corrected invocation passes public image references explicitly through privileged `env`, and additionally asserts each actual container image before recording success. The final receiver again rejects unsigned requests with 401. This operational mistake and correction are retained in `deployment-retry.log` and `deployment-final.log`.

The successful replacement completed at 20:11:22 UTC. The backend is healthy, MQTT connected, and all three new containers had zero restarts at the final checks. Process metrics and positive MQTT processing counts were observed; public stats, app and website returned successfully. A live scoped WebSocket delivered initial state. That initial payload was approximately 8.07 MB, within this host's explicitly configured 16-MiB ceiling; it is not evidence of a small browser payload.

The receiver's `/healthz` is live and its authentication passes, but `/readyz` remains 503 and Docker health remains unhealthy because `ALERT_FORWARD_URL` is unconfigured. This was present before the release. Pending and dead-letter counts were zero. No forwarding destination was invented or enabled, and no synthetic external notification was sent.

### Current finding status and additional local changes

The following additional fixes remain **local and undeployed**, apart from the backend dependency patches necessary for the signed release. Existing unrelated edits were retained.

| Finding | Current result |
| --- | --- |
| F01–F12 | Covered by the deployed release; already deployed receiver protections preserved. |
| F13 | Regression/CI fixture wiring included in the release source; GitHub CI itself was not separately asserted green. |
| F14 | Fixed locally. Publisher clients disable offline command queuing and replay and have five-second command deadlines. A shared gate bounds 64 outstanding jobs and 8 MiB per publisher. WS publication rejections become bounded outcome metrics instead of unhandled rejections; link admission reports unavailable capacity. A timed-out ioredis command is retained internally even after its promise rejects, so the gate now blocks further admission and reconnects that stream before reopening. A real-client RESP fixture verifies recovery, empty queues and no replay. Live updates use an explicit best-effort drop policy during outages; persisted state remains authoritative. |
| F15 | Compose's local backend default now matches the application's 30-second OLTP budget and is configurable. Separate analytical/worker budgets remain. The rendered candidate is checked; the live backend remains at 900,000 ms. Production-scale endpoint and cold-start qualification is still needed before deploying this setting. |
| F16 | An owned Mosquitto fixture accepts normal QoS 1 traffic and disconnects an oversized frame without PUBACK. `mosquitto/capacity-limits.conf.example` records a 1-MiB frame ceiling, existing 10,000-message limit, candidate 32-MiB queued-byte bound and reduced logging. This file is not included by the live broker. Backlog sizing, overflow/reconnect observation and the required outage window remain open; the profile is not claimed to guarantee lossless unlimited replay. |
| F17 | Still open. A bounded production metadata query reports approximately 8,203,492 ledger rows and 2,176,466,944 total relation bytes; message tags occupy 52,379,648 bytes with approximately 97,870 rows. These row counts are planner estimates, not exact counts or growth rates. Retry/state tables were small. No identity ledger, historical paths or dead letters were deleted. An archival/compaction design must preserve replay and aggregate semantics. |
| F18 | Hashless undecodable envelopes now get a deterministic, domain-separated SHA-256 of their exact bytes. RAM QoS 0 deliveries receive independent admission claims without requiring a nonexistent packet ID. QoS 1 retains stable delivery keys and rejects invalid IDs. Real MQTT wire tests cover ID-less QoS 0; existing commit/old-session/no-early-PUBACK tests remain green. Identical immutable ACK content versus distinct RF transmissions remains a protocol/product semantic question; that policy was not silently changed. |
| F19 | Fixed locally. New histograms measure admission-to-dispatch waiting and admission-to-successful PUBACK write. The prior metric's help text now accurately identifies processing time. The held-commit wire test verifies that neither a successful ACK nor its latency sample occurs early, and both queue/ACK measurements include the multi-second wait. This measures the client write callback, not proof of broker receipt. |
| F20 | CSS structure now passes: seven duplicate contexts consolidated, shared tag styles centralised, 9-px text raised to 10 px, and the cookie notice's privacy link has explicit contrast and underline styling. Stable MapLibre/Recharts chunk names and corrected budget matching account for renderer wrappers without counting the worker as a renderer. Actual JS/CSS size limits still fail; no ceilings were raised and no features were removed to manufacture a pass. |
| F21 | Backend's five affected npm packages were patched within allowed dependency ranges, verified with zero npm advisories and deployed in the signed backend. Local frontend locks use MapLibre 6.4.1, Vite 8.3.5 and source-map-js 1.2.2 and also scan clean. Frontend images have not been replaced. Other failed release image gates remain outstanding; clean npm results do not establish that every application image or OS package is vulnerability-free. |
| F22 | Public accessibility/owner/navigation journeys and corrected desktop/mobile map journeys pass in isolated runs. The accessibility tests now derive the dashboard origin from project configuration; hardcoded 4174 URLs had escaped the original isolated-port override. Mobile journeys now open/close the actual Layers menu and compare terrain/coverage URL membership without imposing a meaningless order. Full-run and targeted results, including earlier failures, are recorded below. A 30-minute soak remains unperformed. |

F14 locations: `backend/src/platform/config/redis.ts`, `queue/publicationGate.ts`, `queue/publisher.ts`, `ws/server.ts`. F18 locations: `mqtt/observationIdentity.ts`, `mqtt/client.ts`. F19 locations: `mqtt/durableIntake.ts`, `mqtt/client.ts`, `metrics.ts`. Paths in this paragraph are relative to `backend/src` after the first full path. The follow-up also restores the already deployed synthetic sender bearer header in local `backend/src/workers/synthetic-monitor.ts` and passes the token through its Compose service. F20 changes include `frontend/scripts/check-performance-budgets.mjs`, `frontend/vite.config.ts`, shared/owner/feed/region CSS and browser fixtures. The new broker wire regression is `backend/src/mqtt/brokerLimits.integration.test.ts`.

### Further confirmed operational finding — F23, high, open

`backend/src/index.ts` awaits `refreshNodeIdentityAliases` before opening HTTP/MQTT. `backend/src/db/nodeIdentity.ts` performs evidence collection, in-process grouping and writes inside one advisory-locked transaction, including a packet evidence window of up to 365 days. The compatibility experiment exposed a long transaction before the server listened, and the real replacement took roughly 95 seconds from recreation request to Compose health. Resource limits and shared-host load affect that elapsed time; it is not an isolated CPU benchmark. Move evidence computation outside a short publication transaction with a validated generation/snapshot protocol, avoid redundant concurrent refreshes, and qualify startup against restored production-scale data. Simply skipping identity/privacy reconciliation or killing a correctness transaction by an arbitrary new timeout was not implemented.

### Follow-up verification results

| Check | Result |
| --- | --- |
| Isolated twelve-fix candidate typecheck, contract, build and focused regressions | Pass; 59 focused tests. |
| Backend after patched lock | 532 pass, zero fail, three gated skips. |
| Release preservation tests (receiver auth/storage and WS link helpers) | 14 pass, zero fail. |
| Final release PostgreSQL/Redis ingestion script | Nine pass, zero fail, zero skip; 65.36 s. |
| Final release tagger SQL/privacy tests | Six pass, zero skip, classifier stubbed. |
| Further local backend typecheck, contract and build | Pass. |
| Further local full backend suite, before the final timeout-buffer refinement and notifier wiring | 541 counted: 538 pass, zero fail, three gated skips; 128.67 s. |
| Final Redis refinement and related queue/privacy regressions | 17 pass, zero fail, including actual ioredis timeout/reconnect/no-replay behaviour. The full suite was not redundantly rerun after this isolated refinement. |
| Final internal notification/receiver auth wiring | 13 pass, zero fail; existing authenticated synthetic sender behaviour retained in local source and Compose. The live synthetic monitor was not replaced. |
| MQTT framing, queue/ACK timing, QoS 0 and identity regressions | Eight pass, zero fail, zero skip, including the owned broker. |
| Frontend build and unit suite | Build passes; 173 counted, 167 pass, zero fail, six skips. |
| Emitted production map worker and fallback/recovery regressions | Six pass across desktop/mobile, 2.3 min; this uses the actual built worker rather than the dev-server transform. |
| Frontend CSS structure | Pass: 13 files, zero duplicate contexts, no sub-10-px text or undefined custom properties. |
| Backend and local frontend npm audit | Both report zero vulnerabilities. |
| Browser full run before fixture corrections | 28 pass/four fail; one introduced missing `info` fixture parameter was corrected, cookie contrast fixed, mobile flow corrected. |
| Subsequent full browser run | 29 pass/three mobile failures, including all 24 public/owner/accessibility checks passing. |
| Final desktop and mobile map rerun after correcting menu interaction and URL membership | All eight pass, one worker, 60-second per-test timeout; 1.8 min. Thus the 32 distinct journeys are covered across the final public and map runs; this is not a claim of one final all-32-green run or a long soak. |
| Rendered Compose after local timeout change | OLTP 30,000 ms; privacy worker 900,000 ms; receiver authentication configured. Live timeout was separately verified unchanged at 900,000 ms. |
| Live deployment checks | Signature/revision verification, migration checksums, readiness/MQTT, metrics, scoped WS initial state, public stats and both frontend HTTP probes pass; receiver remains intentionally degraded as described above. |

Corrected asset accounting measures the MapLibre renderer group at 958.6 KiB raw / 245.8 KiB gzip against 820 / 225; Recharts at 394.5 / approximately 110 against 400 / 110 (passes); all JS at 3,353.2 / 948.3 against 2,900 / 850; CSS at 254.8 / 45.4 against 230 / 42. These remain real release failures. They include the current pre-existing frontend feature work; no isolated production page-load improvement is claimed.

The final local test evidence is in `remediation-*.log` alongside release evidence. Final parity between tested scratch source and the original checkout was checked for 27 follow-up source/config/test files. Temporary dependencies, generated test builds and the redundant remediation copy were removed after verification; the clean release checkout, test configuration snapshots and evidence remain. The owned PostgreSQL, Redis, Mosquitto and compatibility containers were removed. Isolated browser ports were closed. Temporary duplicate production credentials were removed; only non-secret image rollback pins were retained. The independent pre-existing server on port 4174 was not stopped or reconfigured. Source/test changes remain in the user's checkout; additional fixes were not mixed into the deployed twelve-fix image or committed without release need.

The highest-priority remaining work is to qualify and deploy the further security/reliability changes, clear the other component image gates, reduce the actual frontend payload without removing required functionality, address identity-refresh startup duration, and design semantics-preserving bookkeeping compaction. Broker backlog capacity and the missing alert forwarding destination remain operational configuration work. The source fixes do not constitute a full production-scale capacity or retention sign-off.

## O — Remaining audit fixes: production release and final qualification

**Updated 2026-11-01T00:47:40.015963+00:00. This section supersedes section N's “local and undeployed” table and outstanding-work summary.** The previously deployed twelve-fix release remains part of the history. The further fixes and the selected worker safeguards are now deployed to eleven application services from clean commit `00d99bdd5f94de2a30f12a91e515820a48cc7c30` on branch `audit-followup-20261031`. The original dirty checkout was retained. All 109 release files were compared and match the retained checkout, including the final identity CPU correction and development/CI qualification fixes. Unrelated changes were not reset.

### Release evidence and live services

The [final signed release workflow](https://github.com/gadgethd/ukmesh/actions/runs/41255573648) passed the policy job and all nine component jobs: backend, tagger, combined, app, website, RF worker, HopReach, Mosquitto reloader and health-check. Every exact image has a zero-critical Trivy result, SPDX SBOM, signature and registry provenance. All nine signatures were independently verified with checksum-verified Cosign 3.1.4 against the exact `release.yml@refs/heads/audit-followup-20261031` identity and GitHub Actions OIDC issuer. Selected pulled image revision labels match the full commit. There are no active critical-vulnerability waivers. This is a critical-vulnerability gate, not a claim that every dependency or OS package is free of all severities.

| Deployed roles | Immutable image |
| --- | --- |
| backend, alert-receiver, synthetic-monitor, health-worker, privacy-remat-worker, path-learning-worker | `ghcr.io/gadgethd/meshcore-backend@sha256:97b5071e8ea8617f15cbbeb3452f98671225620d21ac9b1e78ac0b6658215f0d` |
| app-ukmesh | `ghcr.io/gadgethd/meshcore-app@sha256:73b7a108ffb41617133e26fd306820a17d487fcfa2215cf2a8543919ee8a08d4` |
| website-ukmesh | `ghcr.io/gadgethd/meshcore-website@sha256:22667f4ad7bd5c92fcc123e1f176f53461c60c9d4130d5034e3448dc0d67f5b1` |
| mesh-health-check | `ghcr.io/gadgethd/meshcore-health-check@sha256:ca0c993d0a1ff1c98e3c5abac0d067addb794cf7dfe3a083961e5f9b0e73659f` |
| link-worker | `ghcr.io/gadgethd/meshcore-rf-worker@sha256:bde6ca09a05f82ffb558d322344b7892057f7d935a71de16a53cc1d6a2eebbe1` |
| tagger-worker | `ghcr.io/gadgethd/meshcore-tagger-worker@sha256:963a2ad95a7856e802e76c5518d1873ddcc671db8696a66dce081b34c880cd86` |

All eleven services are running with zero container restarts at final verification. Every configured Docker healthcheck is healthy; the backend-derived workers without container healthchecks also respond on their metrics endpoints, with results retained in `worker-live-metrics.json`. Immutable pins were persisted in the protected deployment environment. Replacements used the clean candidate Compose configuration, the existing live overlay, explicit privileged image variables, and targeted `--no-deps --no-build` operations. TimescaleDB, Redis and the broker were not recreated. Other independent services and unused component images were not replaced merely because their release jobs passed. The active HopReach service retains its v0.1.32-ukmesh.5 rate-limit baseline; the workflow’s separately pinned ukmesh.3 artifact was not promoted over that stronger deployed baseline. The infrastructure-project broker reloader was also retained. Their artifact gates must not be confused with replacement or a new scan of those running baselines. Exact prior image references and local IDs remain in `prior-services.json` and the final `deployment-status.json` for rollback; the prior signed backend/tagger images were retained.

The existing signed restore receipt verified against its separately provisioned pinned key. Backup completed 2026-10-25 01:37:43 UTC; restore verified 2026-10-25 21:01:59 UTC. Both met the existing seven-day gate during this release. No new backup/restore-drill success is claimed. Operational deployment JSON is an observation receipt, not a newly signed backup or release receipt.

### Finding status

| Finding | Final result |
| --- | --- |
| F14 — Redis command retention and capacity | Deployed. Publisher connections disable offline queuing and command replay, have five-second deadlines, and use a shared admission gate of 64 outstanding commands / 8 MiB. A timed-out ioredis command blocks new admission and forces that stream to reconnect before reopening. Real-client RESP tests establish recovery and no replay. Live WS publication explicitly drops bounded best-effort updates during outages; SQL remains authoritative. |
| F15 — OLTP timeout | Deployed and verified at 30,000 ms. Analytical and worker budgets remain separate. Actual final startup loaded 1,412 established nodes, 6,790 known IDs and 2,728 identity evidence names before MQTT connected; no startup SQL timeout was observed on the successful attempt. The final cutover with a prepared identity cache reached full readiness in 20.993 seconds on this shared host. |
| F16 — Broker limits and capacity | Applied live by reload, preserving PID and restart count. Frames are capped at 1 MiB; offline queued bytes at 32 MiB per client; the existing 10,000-message bound remains; debug logging is disabled. Owned QoS 1 tests replayed all 6,000 × 600-byte messages, representing 30 minutes at the observed approximately 200 messages/minute. A 400 × 128-KiB overflow delivered 256 and reported exactly 144 dropped messages. Oversized frames disconnect without PUBACK. High-backlog and any-overflow Prometheus alerts are installed and validated. Live dropped count remained zero. This qualifies that workload for one queue, not simultaneous unlimited outages across every client or a general host-capacity guarantee. |
| F17 — Bookkeeping lifecycle | Additive migration 070 is live. It moves at most 5,000 cold ledger rows per five-minute job after 60 days into a Timescale archive, compresses archive chunks after 14 days, retains a permanent exact UUID guard, and exposes complete sequence/metadata history through `ingest_observation_history`. Both old and new SQL writer forms, including stale Repeatable Read writers, reject archived replays. Actual production job executions succeeded. No historical paths, canonical classification/advert identities or unresolved dead letters were deleted. See storage limits below. |
| F18 — Observation identity | Deployed. Hashless envelopes use domain-separated SHA-256 of exact immutable bytes. QoS 0 has independent admission UUIDs without requiring a nonexistent packet ID; QoS 1 retains stable valid delivery identities. Existing immutable-content-versus-distinct-RF-transmission semantics were preserved; no missing protocol identity was invented. |
| F19 — Queue/PUBACK timing | Deployed. Admission-to-dispatch wait and admission-to-successful PUBACK write have separate histograms. The former acknowledgement metric now describes processing time accurately. Held-commit wire regressions prove no early successful ACK/sample. Dispatch samples are positive after the final cutover; its successful wire-ACK count is 0. Positive live successful PUBACK samples were also retained from C78. Every compiled backend module except nodeIdentity.js is byte-identical across those images; no positive final-container wire sample is claimed while its counter is zero. A successful write callback is not proof of broker receipt. |
| F20 — Frontend size and CSS | Both deployed targets pass unchanged raw/gzip ceilings. Production entry points split app and public-route delivery; explicit MapLibre exports and a shared renderer/worker graph reduce duplicated code; scoped Recharts minification provides gzip headroom. The complete JS budget includes shared dependencies and worker code. CSS checks pass across 13 files, including contrast and minimum text size fixes. Map functionality remains covered by desktop/mobile production tests. |
| F21 — Dependency/image gates | Backend, frontend and isolated health-check npm audits reported zero advisories. MapLibre 6.4.1, Vite 8.3.5 and source-map-js are locked; frontend runtime Brotli, RF certifi and health-check dependency fixes pass the image gate. All nine final image jobs and independent signature checks pass. The deployed RF allocator and health-check runtime security baseline were preserved. |
| F22 — Browser qualification | One final serial run passed all 32 production journeys. Six emitted-worker/basemap fallback tests passed before deployment. The 30-minute final-artifact soak passed every gate: retained heap, one map construction, no page errors, no HTTP 5xx, and visibility throughout. The deployed app then passed the same six basemap tests. See measured scope below. |
| F23 — Identity startup | Expensive evidence collection now uses a consistent read-only snapshot outside the short publication transaction. A session advisory lock serializes refreshes; publication validates the privacy generation and preserves manual aliases. Migration 069 stores only completed, algorithm-versioned, generation-bound results. A valid result can be reused for 30 minutes. The bucket-level active-repeater uniqueness result is computed once per bucket, preserving exact grouping semantics. A final production full refresh took 12.970 s; verified reuse in the same preflight process took 5 ms; the final backend cutover using the completed cache and including ingest warmup took 20.993 s. An expired or invalidated cache still requires full evidence collection and reconciliation. |
| F24 — Map fallback timer | Fixed and deployed. An early watchdog callback re-arms only the remaining absolute attempt budget. Nine focused health tests and real emitted-worker fallback/recovery tests pass. |

### Frontend budgets and soak measurements

| Target / asset group | Raw KiB | Gzip KiB | Raw / gzip ceiling KiB |
| --- | ---: | ---: | ---: |
| App: all JS | 2590.1 | 728.0 | 2900 / 850 |
| App: all CSS | 161.9 | 25.2 | 230 / 42 |
| App: MapLibre renderer group | 749.2 | 195.6 | 820 / 225 |
| App: Recharts group | 324.4 | 92.5 | 400 / 110 |
| Website: all JS | 2849.2 | 812.0 | 2900 / 850 |
| Website: all CSS | 208.7 | 36.9 | 230 / 42 |
| Website: MapLibre renderer group | 756.5 | 198.0 | 820 / 225 |
| Website: Recharts group | 386.9 | 106.9 | 400 / 110 |

App figures are from the retained final artifact; website figures also record the Node 26 budget calculation used to qualify release-runtime gzip variation. Both targets passed Node 26 checks, and both Docker builds enforce budgets. Named library groups are not the entire transitive dependency graph; the all-JS row includes every emitted JS chunk. No budget ceiling was raised.

The MapLibre source plugin reconstructs the exact integrity-locked upstream 6.4.1 source inventory from its npm source maps, applies upstream-compatible class-field semantics, shares worker-safe code through the bundler graph, and emits its BSD license. It is version-bound and fails closed on an unexpected source inventory. A MapLibre upgrade requires requalification of this integration and the real worker tests.

The soak ran 30.15 minutes including a 2-minute warmup, with 60 samples. Its owned fixture supplied 4,600 synthetic public nodes and 10 valid node updates/second (18010 delivered), real MVT decode and terrain fixtures, active camera movement, and the final production bundle. Community tile hosts were intercepted/blocked. Retained data in V8 heap snapshots changed from 23,561,912 to 24,473,479 bytes (3.869%; 10% gate). Runtime and total-heap measurements remain separately recorded, rather than being substituted for retained application data. This is an owned browser stability workload on shared-host Chromium/SwiftShader, not a production-user latency, RF-accuracy, mobile-device or Internet tile-service capacity benchmark.

Public HTTPS checks returned the app/public HTML at `app.ukmesh.com` and `ukmesh.com`; all 10 referenced entry JS/CSS assets returned HTTP 200 and matched the exact signed-image bytes. These are functional delivery checks, not a public traffic capacity benchmark.

### Data preservation and practical limits

Migration 070 preserves all observation sequence values and metadata. The 20,000-row owned SQL fixture had identical complete-history digests before and after compaction and rejected replay through current/prior writers. Its archive relation compressed from 3,809,280 to 1,220,608 bytes (about 68%); that comparison excludes permanent-guard overhead and is not a whole-database saving. Existing production ledger entries were younger than the 60-day threshold, so no immediate 2.18-GB reclaim is claimed. Hot bookkeeping now has an automated lifecycle; permanent exact dedupe/history still grows and needs disk monitoring. `packet_paths` and complete history remain protected.

The old single-process/full-start compatibility approach was not repeated. Bounded prior-image read queries compiled against the migrated schema, and real SQL fixtures exercised fresh migrations, replay, compressed privacy updates and stale-generation rejection. Snapshot publication is rejected if visibility changes during evidence computation. Node-scoped privacy rematerialization cannot close a pre-existing unrelated generation gap or bless a missing state row; a re-request stays pending.

Before replacing workers, the candidate was reconciled with deployed health/privacy/path-learning safeguards: separately pinned restore keys, lifecycle target completeness, privacy-generation health, native Timescale diagnostics, worker heartbeat checks, inactive-node archival and public training/prefix filters. Health-check source is pinned to deployed baseline `c93af192ec7cc0ecdfb3e9d0ea163840202ccbfc` with a patched dependency lock; it retains Turnstile/WebSocket authorization, origin/capacity/rate limits and persisted results. Service-specific channel configuration, volume mappings and the RF `libtcmalloc_minimal.so.4` allocator were retained. Retired `path-history` reports unavailable/retired while retaining its historical error for audit; supported path learning remains active.

The live scoped WebSocket initial state was 8,058,562 bytes, still within the explicitly configured 16-MiB ceiling. This release does not claim that the large initial payload is compact. Source/dependency and fixture gates do not establish unlimited production throughput.

### Verification and deployment incident record

| Check | Result |
| --- | --- |
| Final backend unit suite after identity CPU correction | 575 total: 572 pass, zero fail, three gated skips. |
| PostgreSQL/Redis ingest + archive + identity + privacy-completion integration | 12 pass, zero fail/skip in final CI. The earlier complete shared-host repeat also passed in 129.36 s. |
| Restored analysis lease SQL | One pass, zero fail/skip. |
| Health packet/vacuum SQL | 22 pass, zero fail/skip. |
| Preserved path/retirement regressions | 10 pass, zero fail. |
| Backend typecheck, build and OpenAPI contract | Pass; 71 API and 11 operator contracts. |
| Frontend unit suite | 174 total: 168 pass, zero fail, six browser-gated skips. |
| CSS / app + website budgets / npm audit | Pass. |
| Production browser journeys | 32 pass in one final run, desktop/mobile. |
| Emitted worker + fallback suite | Six pass before deploy and six against the deployed app. |
| Final-artifact browser soak | Passed; 30 minutes, all five gates. |
| Health-check source after patched lock | 54 pass, zero fail; npm audit zero. |
| Signed health-check image | Real bootstrap JSON 200; forbidden WS origin 403; missing WS authorization 401. |
| Signed RF worker image + real owned Redis | 44 pass, zero fail. |
| Owned broker framing / offline replay / overflow | Two pass, zero skip. |
| Monitoring / operator safety | 36-rule configuration and complete Prometheus rule fixture suite pass; Alloy validates; mocked rollback/schema drills and an owned compressed-chunk maintenance fixture pass. |
| Signed release policy, scans, SBOMs, signatures and provenance | All nine component jobs pass; signatures independently verified. |
| Live HTTP/WS/MQTT/auth/metrics/services | Eleven running services; configured container healthchecks and public stats, app, website and health-check probes pass; MQTT and positive dispatch samples confirmed; successful wire-ACK sample scope is recorded under F19. |

A first integration run observed an administrative connection termination during fixture teardown. The affected case passed independently and the complete 12-test repeat passed; the first failure log is retained, and no assertion was weakened. Earlier real frontend chunk-cycle/worker failures and owned fixture corrections are also retained in evidence. Final browser runs used frozen artifacts and ran serially after shared-host CPU contention was observed.

During the preceding C78 rollout, the first follow-up backend replacement passed Docker liveness before MQTT warmup completed. A one-shot readiness probe returned 503 and the rollout helper immediately restored the prior exact signed backend. The prior backend completed its cold startup and MQTT recovered. The helper was corrected to wait up to 90 seconds for full readiness after liveness. The C78 retry then completed in 43.093 seconds with populated anti-spam caches, owner authorization enforced, MQTT connected and no observed startup query timeout. No failed attempt is counted as a successful release; `c782c7c18007-deployment-status.json`, both cutover logs and the preserved journal document the rollback and retry. The broker's final dropped-message count was zero. Prior image IDs remain available.

Longer post-release observation exposed a short timeout burst at 23:00 UTC during scheduled identity reconciliation: two MQTT persist operations failed their deadline and correctly remained unacknowledged; the broker connection recovered. The pure grouping function recalculated an unchanged bucket-level uniqueness result inside every pair assessment. On a protected snapshot of 7,580 live node metadata records, computing it once per bucket reduced CPU time from 57,345 ms to 464 ms. The complete serialized grouping result (9,460 assessments and 77 aliases with empty evidence maps) had the same SHA-256 before/after. No identifiers were retained in that comparison result, and the temporary live metadata file was removed. This benchmark isolates grouping CPU; the separate production preflight measures full SQL evidence and publication. The correction is included in the final signed release. The full 575-test backend suite and typecheck/build passed again; final CI also repeats SQL integration. Earlier 75.606-second full-refresh / 119-ms reuse observations belong to the preceding release and are superseded by the final measurements above. The earlier cutover logs and deadline observation are retained.

Alert forwarding is configured in the protected runtime environment using the supplied destination. A clearly labelled functional notification was accepted and the durable queue drained. After signed-image replacement the receiver again returned health/readiness 200, rejected unsigned alerts with 401, reported no pending/dead-letter items and retained a successful forwarding timestamp. Alertmanager’s configured credential matches the receiver, and its active runtime configuration includes bearer authorization. A read-only public-node probe also checked 41 private identities and found none in the public response. The webhook value is excluded from reports and release source.

Owned scratch source/dependency trees and superseded test builds were removed after qualification, releasing more than 4 GB of temporary copies. Browser harnesses, frozen qualified frontend artifacts, test evidence, exact clean release source and all prior rollback images were retained. The independent user development server was left running. No production volume or shared Docker cache was pruned.

Evidence is retained under `/home/ben/ukmesh/audit-followup-20261031/evidence`; key files are `live-verification.json`, `deployment-status.json`, `verified-release-00d99bdd5f94.json`, `production-initial-identity-preflight.log`, `production-preflight.log`, `browser-soak-release-30m.json`, `frontend-built-serial-release.log`, `frontend-basemap-live-00d99bdd5f94.log`, `backend-identity-cpu-unit.log`, `backend-final-integrated-repeat.log`, `signed-runtime-image-tests-final.log`, `broker-capacity-integration.log`, `alert-live-final.json` and `final-source-parity.json`. The exact clean candidate is retained at `/home/ben/ukmesh/audit-followup-20261031/candidate`.

The first scheduled identity refresh after the final cutover completed a full reconciliation (no cached-result reuse). Across 595 readiness probes, every response was ready/HTTP 200, with maximum observed latency 4.547 seconds. Backend logs contained no query-timeout or durable-ingest-failure events from this cutover through the observation; the container had zero restarts. The owned probe continued for at least 30 seconds after refresh completion. This confirms responsiveness through one real periodic cycle, without claiming that all possible workloads have been capacity-qualified. Evidence: `first-periodic-identity-refresh.json`.

The final release also includes the development/CI corrections first qualified on `51656c96a76d1c3945e4364d36960557bcba4205`. It builds the missing tagger image, exercises its real SQL using an owned DB and stubbed classifier, provides an authenticated internal alert sink, and keeps the external-classifier daemon scaled to zero during the smoke test. These CI corrections do not change runtime application behavior. The development-only Vite fixes explicitly optimize CommonJS dependencies and apply upstream class-field semantics to queried TypeScript URLs. Both desktop/mobile development workers decoded real fixture tiles. Production app JS/CSS matched all 16 retained assets byte-for-byte; a same-environment before/after build matched all 55 public-site JS/CSS assets. The final signed images additionally match all 18 app and 57 website JS/CSS files from the already soaked C78 images, including both service-worker files. The existing 30-minute qualification therefore covers the exact final frontend bytes. Browser CI now qualifies the emitted app/public builds with the existing 32 assertions, separately from the development renderer. A stale July/unscoped keyboard fixture now uses current, scoped rows; its keyboard assertions are retained. Browser diagnostics are uploaded on failure. An intermediate full development-server journey run had 28 passes and four fake-clock/fixture timing failures; an initial canonical production run had 31 passes and one stale keyboard-fixture failure, which then passed after correcting its timestamps/network. Those partial runs are retained and are not represented as all-green. The final CI runs all 32 unchanged journey assertions against emitted production targets plus two separate real development renderer/worker checks. The initial runtime-commit CI worker job failed on its missing local tagger image; backend, frontend and secret jobs passed. A subsequent fresh-stack run exposed a first-forward readiness cycle. The CI-only bootstrap now sends an authenticated notification to the owned sink and proves readiness before Alertmanager starts; a local real-container test confirms readiness 200 and unsigned POST 401. Production forwarding and readiness requirements were preserved.

Remote CI status at final observation: [CI](https://github.com/gadgethd/ukmesh/actions/runs/41255574077) — `completed` / `success`; job results are retained in `final-ci-status.json`. This is separate from the signed release workflow. The preceding CI run was superseded after production/development journeys passed, so the final run qualifies the additional identity CPU correction too.

## P — GitHub reconciliation, 2026-11-01

The production observations above remain tied to `00d99bdd5f94de2a30f12a91e515820a48cc7c30`.
The local development checkout had diverged from GitHub: four local commits and
a dirty audit/deployment tree, while main contained 224 newer commits at the
start of reconciliation. Applying the old tree wholesale would remove newer
privacy, owner-authorization, quarantine and map-suppression protections.

[PR #183](https://github.com/gadgethd/ukmesh/pull/183) ports the audit delta onto
main `abccd7e3b18f5f351692d9df5388c26f5d8e9c62`, resolving those overlaps. It keeps
the exact receiver/network/time/hash identity for tagger egress and rechecks
policy before every attempt. An unavailable generation or pending privacy
repair retains retries. Existing dynamic migration inventory, newer owner/region
checks and deterministic worker-pool timeout tests remain intact.

The second, operational reconciliation publishes this single audit record and
the existing signed public restore receipt. It brings broker configuration
ownership repairs into source control, retains the newer explicit owner-grant
and rollback safeguards, and fails reloads when an ownership repair fails. The
legacy receipt-directory verifier is removed; runtime verification continues
to require the separately provisioned key and fingerprint. No signing keys,
deployment credentials, receiver token or alert destination are published.
The protected original snapshot and commit history remain available locally.

### Merged source versus the deployed frontend

The merged source retains main's MapLibre 6.11.2 and deck.gl 9.4 dependencies;
the deployed audit artifact used MapLibre 6.4.1. Source-map reconstruction was
requalified against all six 6.11.2 module source maps and its omitted three-export
index-array barrel. Production output includes the upstream licence. The
renderer and worker share a graph and all JavaScript chunks receive Terser
compression. No map feature was removed to meet the budgets.

An isolated build of upstream main measured 3,239.4 KiB raw / 929.7 KiB gzip JS
and 256.4 / 45.7 KiB CSS. The reconciled website measures approximately
3,082 / 875 KiB JS and 209.3 / 37.2 KiB CSS. These dependency versions exceed
the deployed release's 2,900/850-KiB total JS ceiling; the merged source therefore
uses an explicit 3,200/900-KiB total ceiling. Renderer (820/225), charts (400/110)
and CSS (230/42) limits remain unchanged. Section O's unchanged-budget and asset
byte-identity results describe its exact deployed release, not this newer tree.

### Reconciliation verification

Local backend typecheck and the 72-API/11-operator-route contract check pass.
The complete backend suite passed 755 tests with three gated skips; frontend
unit tests passed 179 with ten skips. CSS structure passed across 14 files.
Owned PostgreSQL/Redis fixtures passed all 22 ingestion, archive, replay and
public-authorization tests and all 17 visibility-convergence checks. Eight
actual tagger SQL tests passed with a stubbed classifier, including private-copy
isolation, revocation during retry backoff, and unavailable-policy retry
retention. Broker reload tests and the explicit-owner-grant/discovery/rollback
script checks pass. PR checks record the subsequent production-artifact browser,
worker, Compose and secret-scan evidence for the final proposed revisions.

The reconciliation does not promote new images or rerun a production cutover.
Production remains pinned to the signed release described in section O. A new
rollout must qualify its exact merged commit and images independently.
