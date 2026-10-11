# TASK BRIEF — ukmesh MQTT ingestion: performance investigation & optimization PR

Session: `ukmesh-ingest-perf` · Host: projects-vps (`vps-0e251c4d`, 57.129.131.35) · Opened: 2026-10-11 by Hermes (infra lane)
Reviewer: Ben — he reviews the PR; nothing gets deployed. Operator (Hermes) collects your final report.

**Before anything else: reason thoroughly and design fully before writing code.** Then execute autonomously end-to-end per this brief and the original task in Appendix A. Update §8 PROGRESS LOG at every phase boundary — the operator reads it for check-ins.

---

## 0. Mission

Investigate the MQTT ingestion pipeline of this repo at current HEAD, verify-or-refute six claimed findings with first-hand evidence (code + measurements), build a reproducible **synthetic** benchmark on an **isolated** stack, implement the **smallest maintainable** optimizations that measurably improve resource efficiency, and open ONE pull request to `gadgethd/ukmesh` from branch `perf/mqtt-ingest-20261011` — **left unmerged** — containing the full evidence package (8 required sections, see §7).

Done means: (a) PR open with all eight required sections; (b) baseline-vs-optimized benchmark results with raw outputs preserved on disk; (c) all tests green, CI green on the PR; (d) `FINAL-REPORT.md` written; (e) final message = short summary + PR URL + headline numbers.

**The live UKMesh production must remain completely untouched. This is pass/fail.**

---

## 1. Ground rules — ABSOLUTE

### Production is off-limits (read the whole list before starting)
- **Never** modify / restart / recreate / stop / `exec` / `logs` / `inspect` ANY production container. Production names: anything starting `meshcore-analytics-` or `meshcore-infra-` (incl. `meshcore-infra-timescaledb-1`, `meshcore-infra-mosquitto-1`, `meshcore-analytics-backend-1`), plus compose projects `meshcore-analytics`, `meshcore-infra`, `meshcore-beacon-ukmesh`, `meshcore-health-check`. Do not run ANY docker command against them.
- **Never** touch production databases (`meshcore`, `beacon_ukmesh`), their volumes, or the production Mosquitto config/logs.
- **Never** touch `~/ukmesh/meshcore-analytics` — the deployed checkout with uncommitted deploy lineage. Not yours. All repo work happens ONLY in **this worktree**: `/home/ben/ukmesh/.agent-worktrees/mqtt-ingest-perf-20261011`.
- **Never** touch `~/ukmesh/meshcore-infra/` (prod compose + `.env`), prod cron jobs, or any other directory under `~/ukmesh/` besides your worktree and `~/ukmesh-bench/`.
- **Never** restart ANY service on this host. If you notice prod trouble, note it in PROGRESS and do not fix it.
- **Never** run load tests / packet replays / benchmarks against any production endpoint. Synthetic traffic only, isolated stack only.
- **Never** subscribe to or publish to the production broker. The bench broker is yours and isolated.
- **No deleting other people's stuff:** no `docker system prune`, no `docker volume rm`, no `docker rmi` of images you didn't build for the bench. Cleanup only with your own bench compose project (`docker compose -p mqttbench … down --volumes` from your bench dir). No `pkill`/pattern-kills anywhere — track PIDs.
- Read-only production inspection that IS allowed (task explicitly permits already-available read-only metrics): `curl` against the read-only Prometheus/metrics endpoints if reachable (e.g. production backend `:9091/metrics`, VPS Prometheus `:9092`). No prod config changes to enable anything.

### GitHub rules
- **Allowed:** `git push origin perf/mqtt-ingest-20261011`; open the PR to `main` via `/home/ben/bin/gh`; watch CI checks; edit the PR body.
- **Forbidden:** merging the PR; pushing any other branch; creating tags or releases; `workflow_dispatch`; editing `.github/workflows/**`; touching repo settings; printing secrets.
- **Deploy-gate (verified by operator 2026-10-11; re-confirm cheaply by reading the workflows at your branch):** `.github/workflows/release.yml` fires only on `release: [published]` + `workflow_dispatch` → a branch push / PR cannot trigger it; `ci.yml` runs tests on push + PR entirely inside GitHub runners (isolated smoke stack, torn down); the actual production deploy is a manual VPS procedure that you must not perform. If you find anything that contradicts this, **stop before pushing** and report.
- **Git identity:** the worktree is already configured (`gadgethd <111318106+gadgethd@users.noreply.github.com>`), a previously-used identity for this repo's agent branches. `gh` is at `/home/ben/bin/gh` (not on PATH — use absolute path or `export PATH="$HOME/bin:$PATH"`). Push auth is verified working.

### Resource caps (shared host)
- Give every bench container cgroup limits so production keeps headroom (suggest: timescaledb 2 cpu / 2 GB; backend 3 cpu / 1.5 GB; mosquitto and publisher minimal). Cap the whole bench footprint ≈ ≤4 vCPU burst / ≤4 GB.
- Never bind a production port. Taken (verify with `ss -tln`): 53, 1883, 3000-3010, 3080, 3090, 4174, 5432, 6379, 8080-8099, 8923, 9001, 9092, 13000, 20241-20245, 32720, 35809, 36667, 37855+, 39999, 40504. Suggested bench ports: **25432** (pg), **21883** (mqtt tcp), **29001** (mqtt ws), **23000** (backend http), **29091** (metrics) — re-verify free first.
- It is currently ~01:00–03:00 local (BST, UTC+1) — the quiet window. Prefer heavy runs now; if you slip into the UK day, keep caps in place anyway.
- Never embed secrets anywhere; the bench broker gets its own throwaway credentials (see CI bootstrap pattern in §2).

---

## 2. Environment (verified 2026-10-11 by operator)

- **Worktree:** `/home/ben/ukmesh/.agent-worktrees/mqtt-ingest-perf-20261011` — branch `perf/mqtt-ingest-20261011` tracking `origin/main` @ `fa801dc` (clean; `HEAD == origin/main`). This is your cwd.
- **Bench dir** (your scratch; keep everything not meant for the repo here): `/home/ben/ukmesh-bench/mqtt-ingest-perf-20261011/`
- **Host:** 8 vCPU, ~23 GB RAM (~17 GB available), 61 GB free on `/`. Docker + Compose v5.4.0. Node v22.22.1 on the host.
- **Pinned digests to reuse (from `.github/workflows/ci.yml`):** timescaledb `sha256:22e8a5ae7aef121d1537afe946dd7cc5deeeb63ab36ce19849d671bd3b663509`, redis `sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065`, mosquitto `eclipse-mosquitto@sha256:9cfdd46ad59f3e3e5f592f6baf57ab23e1ad00605509d0f5c1e9b179c5314d87` (verify exact strings in ci.yml).
- **Proven recipes to adapt (do not reinvent):**
  - `scripts/ci-smoke-repro.sh` — fresh-DB stack recipe (migrate with retry, CI-shaped boot, 150 s cap).
  - CI "Workers and Compose" job in `.github/workflows/ci.yml` — mosquitto creds bootstrap (`scripts/bootstrap-mosquitto.sh` + `mosquitto_passwd` via the mosquitto image), ACL lines, `db-migrate` service, readiness/WS fanout checks (great template for your bench smoke test + WS-preservation check).
  - `docker-compose.ci.yml` — CI overrides for the stack; `docker-compose.yml` has the service definitions (backend env at ~L60-125).
- **Bench stack shape (suggested):** compose project `mqttbench` (own network `mqttbench_default`, own volumes), services: `timescaledb` (pinned image, `shm_size: 512mb`+), `mosquitto` (pinned image, your own passwd/acl, listeners on bench ports), `db-migrate` (from your backend image), `backend` (built from THIS worktree's `Dockerfile.backend`, `SOURCE_REVISION=<branch sha>`, env pointing at bench DB/broker only), publisher (run from the backend image or a minimal node image — `mqtt` is already a backend dependency).
- **Test traffic shape:** publish to realistic topics `meshcore/<IATA>/<OBSERVER_64HEX>/packets|status|neighbors` (the isolated DB means you can use the full-fidelity non-test path; `meshcore-test/...` routes to network `'test'` scope and SKIPS some SQL sub-writes — prefer the full path for fidelity, note the difference). Craft packet payloads with valid `raw` hex so the decode path is exercised (borrow samples from `backend/src/mqtt/decodePacket.test.ts`, other tests, and CI E2E payloads; you may also generate via `@michaelhart/meshcore-decoder` if it supports it). Status envelopes must satisfy `statusEnvelopeTargetsObserver` (see `identityBinding.ts`) or they're rejected — read it before crafting status messages.
- **Operator-local reference files (for context, may be stale vs your HEAD):** none needed — read the code.

---

## 3. Operator pre-verification (2026-10-11) — re-verify EVERY item yourself; cite `file:line` + measurements. Do not trust; confirm or refute.

**F1 — MQTT.js serializes processing; the durable handler holds the flow. [CONFIRMED at source level by operator; re-confirm on the installed build]**
- `backend/src/mqtt/durableHandler.ts` (whole file): `handleMessage` override calls the MQTT.js callback **only after** `persist()` resolves; on failure it reports + closes the stream (`client.ts:311-319`, via `mqttClient.stream.destroy()`) so the broker redelivers.
- `backend/src/mqtt/client.ts:346` assigns it onto `client.handleMessage`.
- MQTT.js **5.16.0** (pinned in `backend/package-lock.json`): see installed `node_modules/mqtt` after `npm ci` (`backend`). In `src/lib/client.ts` `connect()` (~L988-1105): each TCP chunk is fully parsed into a `packets[]` queue, then `work()` handles **one** packet; the `done` handed to handlers (`nextTickWork`, L1083-1098) advances the queue only when called. For QoS1, `src/lib/handlers/publish.ts` (case 1, ~L280-315) sends PUBACK only inside `handleMessage`'s callback and passes that same `done` to the PUBACK write — so the pump advances only after the PUBACK is actually written. The library's own doc comment on `handleMessage`: *"Handle messages with backpressure support, one at a time. Override at will."* (client.ts:842-849).
- **Consequences to verify empirically:** ingestion is strictly one message at a time end-to-end; `enqueuePacket` is called once per pump step (only caller chain: `client.ts:755` → `db/index.ts:633,677` → `packetBatch.ts:147`), so the batch NEVER coalesces multiple packets; the 50 ms flush timer (`packetBatch.ts:15,123-132`) therefore adds pure latency to EVERY packet message — expect `meshcore_packet_batch_size` histogram ≈ all-1s on the baseline at every rate. Sustainable per-message rate ≈ 1/(50 ms + decode + write) ≈ ≤ ~20/s from the timer alone, regardless of DB capacity. If you observe larger batches, my model is wrong — re-investigate before designing anything.

**F2 — batch utilization:** consequence of F1 + single producer. Confirm via the histogram (baseline burst scenario) and code.

**F3 — single active flush:** `packetBatch.ts:539` (`if (activeFlush) return activeFlush`) + drain loop L541-550. Note what this actually means under F1 (pending[] only ever holds the current message) and what changes if/when multi-pending is possible.

**F4 — dead config:** `MQTT_INGEST_CONCURRENCY` (docker-compose.yml:119, .env.example:39) and `MQTT_INGEST_QUEUE_MAX` (docker-compose.yml:121, .env.example:40) are **read by nothing** in `backend/src` (grep: only `MQTT_INGEST_ENABLED` is read, `index.ts:69-70`). Leftovers from the pre-durable design. Repair options: remove, or repurpose (document choice + compatibility).

**F5 — pool utilization:** OLTP pool max = `DATABASE_POOL_MAX` (compose default 16; `platform/config/database.ts:29-35` default 8; analytics pool max 2, `db/index.ts:53-57`). Ingest commits use ONE connection at a time **by design** (serial pipeline). Measure actual concurrent connections per scenario (`pg_stat_activity`) and state plainly that spare connections cannot accelerate a serialized pipeline (they serve API/WS/stats in parallel).

**F6 — SQL shape:** the per-batch statement is one big CTE with up to **8 write targets** — `packets`, `packet_paths`, `nodes` (observer upsert), `node_network_sightings`, `packet_daily_stats`, `packet_hourly_stats`, `observer_region_packet_sightings`, `observer_region_observer_sightings` — plus materialized CTEs incl. `FOR KEY SHARE` on `public_visibility_state` and a `privacy_rematerialization_queue` EXISTS check (`packetBatch.ts:243-481`). At 1-row batches the per-execution overhead matters: enable **pg_stat_statements** in the BENCH db (preload + `track_planning=on`-equivalent; set `pg_stat_statements.track_planning = on` if the background worker approach is awkward — a plain `ALTER SYSTEM`+restart of the bench pg is fine) and record `total_plan_time` vs `total_exec_time` per call for this statement. This measurement decides whether prepared statements / restructuring are worth it.

**Existing metrics inventory (backend :9091 `/metrics`):** `meshcore_mqtt_messages_total`, `meshcore_mqtt_ingest_outcomes_total{outcome}`, `meshcore_mqtt_ingest_active`, `meshcore_packet_batch_flush_total{outcome}`, `meshcore_packet_batch_retries_total`, `meshcore_packet_batch_size` (histogram), `meshcore_packet_batch_duration_seconds` (histogram) — `backend/src/metrics.ts`. Use these in the bench; §6.6 adds phase-timing metrics.

**Per-message serial path (profile it):** decode/parse (`decodePacketCompat`, `buildSummary`, channel keyStore) → advert crypto (`verifyAdvertSignature`) where applicable → dedup check → `insertPacket` → batch commit → post-commit `emit()` subscriber chain (`broadcastPacket` WS fanout, owner packet share, link queue — `index.ts:222-247`) → ack write. Everything in that chain delays the PUBACK. Quantify where wall time goes before optimizing anything.

---

## 4. Design thinking — decided constraints + evidence-gated candidates

**The hard constraint (state it in the PR):** QoS1 PUBACK must not be sent before its persistence commits — and in MQTT.js 5.x the pump's flow control is coupled to that same callback (see F1). So **cross-message batching/concurrency is impossible without either (a) breaking the library's callback contract, or (b) internals monkey-patching** (e.g. intercepting `_sendPacket` to defer PUBACK writes) — both fragile, both against the spirit of "design around the constraint rather than bypassing durability guarantees". **Do NOT ship any internals hack in this PR.** If you believe a *bounded, low-fragility* alternative exists after your own investigation, present it in `FINAL-REPORT.md` as a proposal with risks — do not ship it. The PR's job: make the necessarily-serial path as cheap and low-latency as possible, and prove the serialization claim with evidence so the reviewer understands why batching isn't the answer here.

**Candidates (keep/drop by measurement — implement the smallest set with meaningful, demonstrated gains):**

1. **Immediate flush on the serial path [prime candidate].** Under F1 a message can never coalesce with a later one, so the flat 50 ms wait is pure latency on every packet. Flush on next-tick when an item is enqueued and no flush is active (keep `MAX_BATCH_SIZE` + drain-loop semantics for any multi-pending path, keep retry logic untouched). Make timing behavior configurable with safe defaults; document. Prove no coalescing is lost via the `packetBatchSize` histogram (baseline vs optimized). Expected: p50 persistence latency collapse (≈50 ms → few ms) + higher sustainable rate; measure the real numbers, don't assert them.
2. **Plan-cost reduction [only if F6 shows planning dominates].** E.g. named prepared statements per batch length, validated against pooled connections; or statement restructuring. Measure before/after `total_plan_time`.
3. **Move best-effort post-commit work off the ack path [only if profile justifies].** `recordMultibyteEvidence` (`client.ts:795-807`) is awaited inline today, already best-effort (errors logged, never rethrow) and a known deadlock source; candidate for a bounded async queue (drop counters + graceful drain on shutdown). Keep semantics; drop if complexity outweighs measured gain. Also quantify the post-commit `emit()` chain — defer anything that provably doesn't need to precede the ack.
4. **Decode-result cache for repeated raws across observers [optional; only if multi-observer profile shows repeated decode cost].** Bounded LRU keyed by raw hex. Drop if marginal or risky.
5. **Dead-config cleanup [F4].** Remove or repurpose; note compatibility implications.
6. **Phase-timing metrics [required by the spec's "Add metrics where necessary"].** Time waiting for messages / processing / queueing / DB write / pool-acquire wait — bounded cardinality, wired into `metrics.ts`, safe defaults, no behavior change.
7. **NOT to be done:** no dependency upgrades; no forking/patching `node_modules` in shipped code; no unrelated refactors; no `.github/workflows` edits; no schema migrations unless unavoidable (they complicate rollback — if one is truly needed, say so loudly in the PR).

---

## 5. Benchmark requirements (from the spec; methodology is yours to perfect)

- **Baseline** = build from `origin/main` @ `fa801dc` (record image digest + sha). **Optimized** = build from your branch. Same harness, same stack config, **interleaved A/B/A/B**, ≥3 repetitions for headline numbers. Keep both images tagged until Ben has reviewed.
- **Scenario matrix (name them, run both sides):** (S1) low steady ≈2 msg/s ×≥3 min — prove no efficiency regression when idle-ish; (S2) sustained ≈25-50 msg/s ×≥5 min; (S3) burst (e.g. ≈300-500 msg/s for 10-20 s, then quiet, measure drain time); (S4) duplicates flood; (S5) multi-observer (same packets via 3+ observer keys); (S6) DB congestion (e.g. `docker pause` the bench timescaledb for a few seconds mid-run, or a competing heavy query — your choice, document); (S7) reconnect (restart bench broker connection / drop the backend's TCP, verify QoS1 redelivery); (S8) crash mid-burst (`docker kill` the bench backend; restart; verify zero loss of *sent* messages, dedup handles replays) + shutdown drain (`SIGTERM` → clean drain, no stuck `MQTT_DRAIN_TIMEOUT`); (S9) privacy transition sanity on the bench stack (visibility classification still fails closed; the intended flows from the privacy tests still hold).
- **Measure per scenario:** persisted msg/s; persistence latency avg/p50/p95/p99 (**define and document the method** — e.g. a bench-only instrumentation patch applied identically to BOTH builds writing per-message phase timestamps to JSONL; keep that patch as a file in the bench dir and do NOT ship it in the PR; post-hoc "visible in DB" polling is acceptable as a cross-check but state its granularity bias); CPU-time per 1,000 persisted messages (cgroup `cpu.stat` delta of the backend container ÷ persisted count; also report per-scenario CPU% and total); average batch size (histogram); SQL executions per 1,000 messages (pg_stat_statements delta); DB connection utilization (peak concurrent `pg_stat_activity`); peak memory (cgroup `memory.peak`); queue/lag behavior (broker queue depth if available + backlog drain time).
- **Raw outputs preserved:** one file per run under the bench dir (`results/<scenario>-<variant>-run<N>.json|txt`); consolidated `bench.md` with method → raw results → timeline → threats-to-validity → exact repro commands. No invented numbers — if something couldn't be measured, say so.
- **Repro commands** must be exact and self-contained enough for the operator to re-run.

---

## 6. Correctness & tests (must preserve — from the spec)

Preserve: QoS1 durable-ack semantics (no ack before commit); correct redelivery/recovery after crash or DB outage; dedup + idempotency; privacy classification & fail-closed visibility; atomicity of packet + packet_path writes; correct node/observer/statistics updates; ordering where state depends on prior messages; bounded queues/backpressure/graceful shutdown; deadlock/hot-row protection; existing WebSocket + live-map behavior. Never trade correctness for benchmark numbers.

Run (at minimum, and document results in the PR):
- `npm run typecheck`, `npm test`, `npm run test:ingest-integration`, `npm run build` (in `backend/`); `npm run test:analysis-integration` if anything it touches changes; contract checks only if routes/contracts change (prefer not to touch them).
- NEW tests for whatever you change (flush behavior incl. immediate-flush + drain + retry paths; the metrics additions get at least a no-crash/format check).
- The S4/S7/S8/S9 scenarios double as regression tests — record their outcomes as evidence.
- Watch the PR's CI (`/home/ben/bin/gh pr checks <n> --watch` or poll) until green.

---

## 7. Deliverables & checkpoints

- Commits on `perf/mqtt-ingest-20261011`, conventional style (`perf(mqtt): …`, `test(mqtt): …`, `docs: …`), small and coherent. Do NOT commit `TASK-BRIEF.md` or bench outputs; do not `git add -A` blindly.
- Push the branch, open the PR to `main` (title suggestion: `perf(mqtt): durable ingest — verified serialization analysis + measured efficiency improvements`). PR body must contain the **8 required sections** from the spec (§Appendix A): root-cause analysis w/ code refs → implementation & why safe → before/after benchmarks (CPU efficiency, throughput, latency, batching, memory) → tests executed + results → reliability/privacy/concurrency risks considered (including the pump-constraint analysis) → config changes + compatibility → rollback procedure → explicit production-untouched confirmation. Include raw evidence paths + repro commands. **Do not merge.**
- `FINAL-REPORT.md` in the bench dir: what you did, measured results, surprises, an honest mistake inventory, open questions, proposed future work (incl. the concurrency-constraint discussion).
- Keep the session available for follow-up questions (Ben may ask the agent directly later); evidence stays on disk.

---

## 8. PROGRESS LOG (append entries; newest last — keep ≤10 lines each)

**Format:** `### <ts BST> — <phase> — <one-line status>` then bullets. Also add an `OPEN QUESTIONS` section at the bottom if any; continue past any non-blocking question with a safe default.

- [x] Phase 0 — verify semantics + F1-F6 against HEAD (source + minimal experiments)
- [ ] Phase 1 — bench harness built + smoke-proven (incl. WS fanout check)
- [ ] Phase 2 — baseline runs + raw results
- [ ] Phase 3 — implementation + tests
- [ ] Phase 4 — optimized runs + comparison
- [ ] Phase 5 — chaos/reliability scenarios
- [ ] Phase 6 — PR pushed + CI green
- [ ] Phase 7 — FINAL-REPORT + final message

### 2026-10-11 01:59 BST — Phase 0 — source findings verified; benchmark measurements remain in Phase 2
- `npm ci` succeeded; installed `mqtt@5.16.0`; npm reported three dependency audit advisories.
- MQTT.js pump and QoS1 handler source confirm one-at-a-time callback flow; durable-handler tests pass 3/3.
- Verified enqueue/flush path, fixed 50 ms timer, single active flush, unused concurrency/queue settings, pool defaults, and eight SQL write targets.
- Valid decoder fixture experiment produced `metadataValid=true` and canonical packet IDs; histogram, pool, and SQL plan measurements remain for baseline runs.
- Referenced `scripts/ci-smoke-repro.sh` is absent at HEAD; adapt current `.github/workflows/ci.yml` smoke recipe.

---

## Appendix A — Original task text (authoritative; Ben's words)

> # UKMesh — MQTT Ingestion Performance Investigation & Optimization PR
>
> ## Objective
> Investigate the MQTT ingestion pipeline in `https://github.com/gadgethd/ukmesh`, identify opportunities to improve CPU efficiency, batching, throughput and multicore utilization, and implement justified improvements in a dedicated GitHub pull request.
>
> **ABSOLUTE RULE: DO NOT CHANGE THE LIVE SYSTEM.** This is a code investigation, development, testing and PR task ONLY. The production UKMesh deployment must remain completely untouched.
>
> ## Production safety constraints
> - Do not restart, stop, redeploy, reconfigure or otherwise modify any production service.
> - Do not execute database migrations, SQL writes, configuration changes or maintenance operations against production.
> - Do not modify the running MQTT broker, Redis instance, PostgreSQL/TimescaleDB instance or Docker containers.
> - Do not run benchmarks, packet replays or load tests against production.
> - Do not merge the PR, push directly to the default branch or activate deployment workflows.
> - Work exclusively in an isolated development branch/worktree. Use synthetic fixtures and disposable test infrastructure.
> - Read-only production metrics may be inspected if already available, but must not require changing production configuration.
> - Inspect GitHub Actions before pushing the branch to ensure the PR cannot accidentally trigger production deployment. If this cannot be guaranteed, stop before pushing and report the blocker.
> - Production behavior and data must remain unchanged until I explicitly approve a deployment in a separate task.
>
> ## Investigation
> Review the current implementation, particularly: `backend/src/mqtt/client.ts`, `backend/src/mqtt/durableHandler.ts`, `backend/src/db/packetBatch.ts`, `backend/src/db/index.ts`, `backend/src/platform/config/database.ts`, relevant metrics, tests and Docker Compose configuration.
> Previous inspection suggested the following potential issues. **Verify each finding against current HEAD rather than assuming it is correct.**
> 1. MQTT.js may be processing incoming messages sequentially because `handleMessage` waits for durable persistence before completing its callback.
> 2. Packet batching currently uses a maximum batch size of 50 and a 50ms flush interval. Determine whether sequential message processing prevents batches from filling efficiently.
> 3. `packetBatch.ts` appears to permit only one active database batch flush at a time.
> 4. `MQTT_INGEST_CONCURRENCY=8` appears in Compose configuration but may not actually control ingestion concurrency.
> 5. PostgreSQL has multiple available connections, but the ingestion design may not be utilizing them effectively.
> 6. Complex packet insertion SQL performs multiple upserts and aggregate updates, potentially creating CPU overhead or contention.
> Determine whether these are genuine bottlenecks and whether improving them would reduce total CPU consumption, not merely spread the same work across more cores.
>
> ## Performance goals
> The VPS has 8 vCPU and 24GB RAM. The primary objective is **resource efficiency**, allowing more unrelated services to run alongside UKMesh. Optimize for:
> - Lower CPU time consumed per successfully persisted MQTT message.
> - Higher sustainable ingestion throughput.
> - Better batch utilization and fewer unnecessary SQL executions.
> - Lower processing latency.
> - Bounded memory consumption and predictable backpressure.
> - Useful multicore utilization when workloads justify it.
> - No unnecessary increase in idle resource consumption.
>
> Investigate controlled concurrency, potentially starting with 2–4 concurrent operations, but do not assume additional parallelism automatically improves efficiency.
> Consider alternative approaches such as more effective batching, reduced write amplification, more efficient SQL and separating computationally expensive processing from the ingestion critical path.
> Do not introduce unnecessary services or complexity.
>
> ## Critical correctness requirements
> MQTT reliability and privacy take priority over performance.
> Preserve QoS 1 durable acknowledgement semantics: a message must not be acknowledged before its required persistence work is committed successfully.
> Do not achieve concurrency by acknowledging MQTT messages early and relying on a volatile in-memory queue.
> Investigate MQTT.js internal message handling before proposing modifications. If its callback model prevents safe concurrent processing, design around that constraint rather than bypassing durability guarantees.
> Maintain:
> - Correct redelivery and recovery after crashes or database outages.
> - Existing packet deduplication and idempotency.
> - Privacy classification and fail-closed visibility rules.
> - Atomicity of related packet and packet-path database writes.
> - Correct node, observer and statistics updates.
> - Appropriate ordering where state changes depend on prior messages.
> - Bounded queues, backpressure and graceful shutdown.
> - Protection against database deadlocks and hot-row contention.
> - Existing WebSocket and live-map behavior.
> Never sacrifice correctness to improve a benchmark.
>
> ## Testing and benchmarking
> Establish a reproducible baseline using synthetic MQTT traffic and an isolated PostgreSQL/TimescaleDB instance.
> Measure: Messages processed per second. Average and p95/p99 persistence latency. CPU time per 1,000 successfully persisted messages. Average packet batch size. Number of SQL operations per 1,000 messages. Database connection utilization. Peak memory usage. Behavior during bursts, reconnects and database failures.
> Test multiple traffic patterns, including low-volume traffic, ordinary sustained traffic, burst ingestion, duplicate messages, multiple observers and periods of database congestion.
> Verify that the new design does not increase resource consumption unnecessarily under low traffic.
> Include tests for packet loss, duplicates, partial failures, shutdown during in-flight processing, retry behavior and privacy transitions.
> Run relevant existing tests and regression checks. Run new benchmarks against both baseline and optimized implementations under equivalent conditions.
> Never use live MQTT traffic or production databases for these experiments.
>
> ## Implementation
> If the investigation identifies an improvement supported by evidence, implement the smallest maintainable solution that meaningfully improves efficiency.
> Prefer configurable, bounded concurrency and appropriate batching over unrestricted parallel processing.
> Preserve compatibility with the existing deployment configuration. Any new settings must have safe defaults and be documented.
> Add metrics where necessary to distinguish time spent waiting for incoming messages, processing, queueing, writing to PostgreSQL and waiting for database connections.
> Avoid unrelated refactors.
> If the proposed optimization introduces unacceptable correctness risks or cannot demonstrate meaningful improvements, do not force it through. Document the findings and propose a safer alternative.
>
> ## Deliverable
> Create a dedicated branch and submit a GitHub pull request to `gadgethd/ukmesh`.
> The PR description must contain:
> 1. Root-cause analysis, with code references.
> 2. Explanation of the proposed implementation and why it is safe.
> 3. Before-and-after benchmark results, including CPU efficiency, throughput, latency, batching and memory consumption.
> 4. Details of tests executed and their results.
> 5. Reliability, privacy and concurrency risks considered.
> 6. Configuration changes, if any, and compatibility implications.
> 7. A rollback procedure for a future deployment.
> 8. Explicit confirmation that production was not modified.
> If performance claims could not be benchmarked, state that clearly. Do not invent speedups.
>
> **Final instruction:** Push the development branch and open the PR only after confirming this cannot trigger production deployment. Leave the PR unmerged and the live system completely untouched.
> Report the PR URL, the findings and the measured improvement. Do not deploy anything. Wait for my review.
