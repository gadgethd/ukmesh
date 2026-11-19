# UKMesh DEV qualification handoff

The lean runtime is the DEV default. The current evidence proves MQTT delivery and persistence for the full 10× replay. Strict full-stack qualification remains open. This handoff records the measurements, remaining work and safeguards for the next qualification run. [Runtime overview](lean-runtime.md)

## Scope

- Keep every service and feature available on UKMesh, including CoreScope, Beacon, Health Check, monitoring, logging, alerting and both Standard and Precision HopReach coverage. The DEV stack uses a mock external GPU worker and keeps its public tunnel off. F2, real GPU qualification and VPS deployment are deferred.
- Use the four-core DEV VM with about 3.8 GiB of usable RAM. Retain the main configuration's resource defaults and the documented overrides for bigger hosts.
- Preserve public APIs, URLs, preference keys, history, cache freshness, response limits, privacy, intended database roles, connection reserves and durability. Keep CoreScope's analyzer database read-only in the web process and writable by its separate ingestor.
- Use the `mqtt-first` qualification profile. The fixed-demand 80 requests/second and mixed-soak read latency are advisory in this profile. Canonical matched read latency, MQTT acknowledgements and persistence, resident health, swaps, disk, recovery and the other existing gates are required.
- Record observations and structural decisions before editing. Keep SQL and privacy rules with the owning applications. Preserve the original stores and archives. Get an independent review of each change before advancing. [Untangle skill](https://github.com/caldoai/skills/blob/main/skills/untangle-codebase/SKILL.md)

## Verified results

The last full 40-minute run used UKMesh `308d467`, CoreScope `610cba2d` and fixture `20261118215522_9c0a37`, which held 461,125 CoreScope transmissions at the start.

| Check | Result |
| --- | --- |
| MQTT publication | 72,000 QoS 1 acknowledgements at 30 packets/s in 2,400.25 seconds, zero publish failures, 240 acknowledged auxiliary messages |
| Live delivery | All 72,000 expected UKMesh WebSocket frames |
| Drain | 16.12 seconds, within 120 seconds |
| UKMesh independent reconciliation | 72,160 observations, 72,160 ledger rows, 72,000 paths and 72,000 facts, zero hash mismatches within 30 seconds |
| CoreScope, Beacon and Health Check | All 23 persistence assertions passed. CoreScope and Beacon each recorded all 72,000 ACK packets and observations. |
| Matched reads on `db13527` | Three complete pairs at each scale. Worst C8 p95 was 85.42 ms at 1× and 142.40 ms at 10×. |
| 10× individual reads on `db13527` | First map page 92.61 ms, short search 85.37 ms and populated observer map 63.42 ms. Both companions completed the paired runs with zero read errors. |
| Hosted checks on `308d467` | All seven CI jobs and the mobile workflow passed |
| Original DEV recovery | All 43 service checks passed with original data stores. CoreScope was healthy at 512 MiB and the public tunnel was off. |

The SQLite indexed total in CoreScope `610cba2d` reduced the median exact count from 51.89 to 1.83 ms on 480,129 retained transmissions. The sampled WAL reached 7.3 MiB before the latest web outage. An earlier disk-cutoff run with the old count observed about 896 MiB. Preserve exact filtered, expanded, legacy-null and empty-field behavior. [UKMesh CI](https://github.com/gadgethd/ukmesh/actions/runs/44491412661) · [Mobile CI](https://github.com/gadgethd/ukmesh/actions/runs/44491412534)

## Open acceptance failures

- The CoreScope web process was OOM-killed at 512 MiB around transmission 503,664. Its separate ingestor completed the full MQTT replay. The web outage caused 2,053 CoreScope read failures. Beacon recorded zero read failures.
- The VM exceeded the sustained swap-traffic gate. Peak swap growth was 54.07 MiB, within its separate limit. Runtime peak was 2.642 GiB, available memory exceeded 384 MiB in all steady samples, and disk reached a minimum of 10.106 GiB. Resident coverage failed, so these measurements cannot establish a full-stack pass.
- The steady log check flagged 42 backend lines around chart query-budget failures and 18 PostgreSQL lines. Several database records are genuine lost clients during the spam analysis query. Some other lines are multiline SQL text containing the status literal `'failed'`. Compare the saved log records with their timestamps and application callers before changing query or log behavior.
- The canonical restore correctly rejected a switch away from the stopped fixture consumer. Its canonical restart also hit the default cgroup limit. After the timed run ended, an ownership-checked temporary 640 MiB limit let that same fixture web container reach readiness. The canonical lease and intake checks then restored the original stores and recreated the original web service at 512 MiB. The temporary recovery did not change acceptance or the defaults.

On the qualification VM, the private evidence is under `plans/lean-runtime/qualification/10x-default.uUnShyyF`. Open `report.md`, then `guarded-indexed-core-soak.nsY0CJe6` for the native publication, delivery, drain and reconciliation receipts, companion persistence, resource samples, logs and original-store restoration. `guarded-scoped-reads.735A2Al4/reads-native.json` records the matched trials. Keep evidence with service configuration, raw traffic and original-store details private. The published results above are redacted.

## 1. Complete CoreScope's retained-history memory proof

The selected-observation change stores a reference to the winning `StoreObs` instead of duplicating the observer labels, signal, path and direction in every `StoreTx`. It keeps longest-path selection, first-on-tie behavior, nil handling, late observations and immutable response snapshots. The two Go suites passed in 44.96 and 189.79 seconds.

The candidate reached full startup at the 512 MiB default on a verified 533,245-transmission clone. It measured 409.04 MiB of Go heap at readiness and 494.77 MiB peak working memory. It served 1,264 reads with zero errors and p95 11.14 ms during the partial interval. The reference needed a 640 MiB diagnostic cap on the same data and measured 471.92 MiB heap at readiness and 566.68 MiB peak working memory. The candidate test stopped after 165.66 seconds. A repeat cold start at 512 MiB on that same 533,245-row fixture was OOM-killed. The Chromium journey passed at 512 MiB on the separate 480,129-row retained fixture. The unfiltered API returned the exact total and 20 entries, and Chromium rendered the selected packet without script errors. Resolving the grown-fixture startup peak, completing both scheduled graph refreshes, checking matched payload parity and rerunning the exact ingest soak remain required. The local `core-selected-observation.exfofW` directory contains the probe, archive verification, reference results and candidate result.

1. Keep original DEV targets selected. Make a fresh private evidence directory and take a verified read-only-sourced copy of an owned retained fixture under the target lease. Run one isolated diagnostic at a time with the default 512 MiB hard limit and no additional cgroup swap. Record binary hash, complete SQLite row count and digest, Go settings, anonymous and working memory, OOM/restart state, and graph refresh timing.
2. Finish the full interval, including both graph cache shapes. Compare raw and grouped packet payloads on the same frozen clone. Exercise pagination, details including observations, analytics and late live delivery. Rebuild or restart through the normal launcher only when needed. Recheck the original inventory afterwards.
3. If memory rises too far during a refresh, profile it. The neighbor graph copies roughly 15 MiB of observation headers at 503,000 transmissions and parses paths repeatedly. Reuse the existing bounded immutable parser where possible. Consider a smaller snapshot or holding the existing read lock only for collection, with measured effects on CoreScope's SQLite-to-WebSocket cursor and late observations. Test the steady in-memory growth separately. Keep full history, exact edge semantics and trust/privacy rules.
4. Run both Go suites for a new CoreScope change. The server parity suite locates goldens through `runtime.Caller` and needs untrimmed test paths. Use CGO for SQLite. Validate the populated packets route in Chromium and compare exact API outputs on the reference dataset. Pin only a published commit in UKMesh. [CoreScope PR](https://github.com/gadgethd/CoreScope/pull/1)

## 2. Reproduce and fix the remaining load failures

- **Charts.** Inspect the 60-second chart refresh bursts, SQL execution and analytics-pool waits under overlapping live traffic. Start with `backend/src/stats/statsRepository.ts` and `statsService.ts`. The repository query budget includes semaphore wait, and its outer timeout is 120 seconds. Preserve both. Identify slow fact and packet queries by their actual timings. Compare before/after results for broad and observer-scoped windows. Keep refresh completeness, visibility-generation fences, feature switches and freshness.
- **Spam analysis.** Trace the existing `WITH logical_messages` and signature query through the heartbeat and abort path. The earlier advert eligibility fix addresses a different query. Measure the spam query under its actual role and buffered workload. Preserve signatures, duplicate weighting, time and visibility rules. Fix real lost clients before changing the log detector. Use the saved SQL continuation and ERROR/FATAL lines to test a parser change.
- **Swap.** Correlate the VM's swap-rate counters, application anonymous/file pages, harness and Docker process overhead, and overlapping region, graph, stats, spam and link work. Major faults alone cannot establish swap causality. Test measurable reductions to allocations and duplicate scans with identical resource settings. Keep the sampling cadence and the host swap policy.
- **Disk.** Confirm capacity for the retained fixture, another 72,000 packets and all auxiliary writes. Reclaim only verified, inactive generated data or a redundant diagnostic copy with an independently preserved digest. Preserve stores, backups, active paths and all retained fixture data. Record reclaimed bytes.

## 3. Qualify the final source as one consistent run

Start from the repository root. The canonical fixture controller validates ownership, guards the 10 GiB disk reserve, and restores original stores after any stop. Use fresh private evidence directories and inspect each restoration receipt before moving to the next phase. Select fixtures by their manifests and measure the actual current retained counts. Previous matched fixtures were `20261118162003_7e7e37` for 1× and `20261118215522_9c0a37` for 10×. A separate older 10× fixture `20261114013258_ec16a1` was last measured at 480,129 CoreScope transmissions. The previously matched newer 10× fixture now has 533,245 valid transmissions. Prepare or reuse the exact owned data through the existing preflight.

```bash
set -e
fixture_session=scripts/full-dev/qualification/fixture_session.py
read_run=$(mktemp -d "$PWD/plans/lean-runtime/qualification/matched-next.XXXXXX")
python3 "$fixture_session" 20261118162003_7e7e37 "$read_run" \
  reads --paired --acceptance-profile mqtt-first
python3 "$fixture_session" 20261118215522_9c0a37 "$read_run" \
  reads --paired --acceptance-profile mqtt-first
```

Collect three matched pairs per scale across C1/C8 and warm/cache-miss modes, including populated observer map at 10×, with active companion readers. Run plans outside timed windows. Both scales need new sealed evidence on the final source and one consistent profile. Use the report's intended closure directory layout before collecting official stages. The following shows the soak argument shape for a prepared fixture.

```bash
python3 scripts/full-dev/qualification/fixture_session.py "$verified_10x_token" "$closure_run" \
  soak --count 72000 --reuse-prepared --acceptance-profile mqtt-first
bash scripts/qualify-full-stack.sh report --run "$closure_run" --acceptance-profile mqtt-first
```

Run the authenticated access matrix, full source parity, paired operations, projection, privacy and recovery, useful-work and interface checks, verified backups and test evidence required by the report. Test classification, regions, rematerialization, learning, links, mock Standard and Precision RF, Health Check, companion traffic, metrics and local alert delivery. Verify migrations complete, all 43 service checks pass, intended role privileges and cross-store denials hold, PostgreSQL durability stays enabled, and each instance reserves at least ten connections. Verify frontend builds and browser journeys on the actual candidate.

Keep the existing limits. C8 p95 is at most 100 ms at 1× and 250 ms at 10×. Other operations retain their deadlines and matched 10% regression limit. Runtime working memory is at most 3.0 GiB, host memory is at least 384 MiB available for 95% of steady samples, swap growth is at most 128 MiB, and swap traffic must stay below the existing 1 MiB/s sustained limit. Disk free space must be at least 10 GiB. Require exact 72,000 ACKs and live frames, companion persistence, zero hash mismatches, drain within 120 seconds, full resident coverage, and no OOM, unplanned restart, pool timeout or unhandled error.

## Restore and report

Stop only owned diagnostic workloads and let accepted traffic drain. Use the canonical fixture-session restoration. Preserve the lease, active consumer identity, source checks and intake/quiescence checks. If a failed consumer blocks switching targets, diagnose it under the same guard. Verify restoration by inventory, the original UKMesh, owner, Beacon and CoreScope targets, the CoreScope 512 MiB cap and the public tunnel state. Leave DEV running on original stores.

Preserve failed and interrupted receipts, make the next revision's CI results explicit and publish the redacted evidence. Use the existing `report` action to validate the final evidence. A complete qualification requires every required receipt on the correct source, profile, run and fixture context. [UKMesh PR](https://github.com/gadgethd/ukmesh/pull/246)
