# UKMesh visibility convergence — 2026-10-23

## Basis and merged-fix verification

Read the complete `BRIEF-ukmesh-converge.md` and HermesMind #672 before edits.
The item was retrieved read-only from `/api/mind/todos/672` on the local backlog
service. It identifies empty FK-cascade prefix statements advancing desired
generation, leaving materialization stale even with an empty queue, and causing
public-read failures plus expensive downstream ledger replay.

Branch `burn/ukmesh-converge-20261023` started clean at current fetched
`origin/main`, `73ab7d4fc63e4968a5fd46af022696ad69d16d9b`. That history includes
merged PR #143 (`0a8cb97e`). Inspected the merged migration 062, convergence
implementation/CLI, rematerialization worker, admission policy and health code.

Migration 062 has separate INSERT/UPDATE/DELETE transition-table triggers.
Empty transition tables return without a bump; real changes and TRUNCATE retain
the fail-closed generation fence. Alias changes and scoped worker completion
advance materialization only if its prior generation was already current.
The owned local TimescaleDB fixture reproduced the old empty cascade changing
**1/1 → 2/1**, then verified migration 062 leaves the same cascade at **1/1**.
These are source and isolated-fixture findings, not assertions about the live
migration ledger or running services.

## Changes and behavior

Implementation commit: `bcdb647908d10ffa79aa99e2a80235e46a6712d4`.

- Added `backend/src/privacy/generationHealth.ts`: one SELECT with singleton
  desired/materialized generation reads and queue-presence EXISTS. No retained
  packet, prefix or derived-snapshot scan; no convergence repair invocation.
- The observation has a two-second response bound, aborts the application's
  query on timeout and coalesces concurrent health/scrape calls. If pool
  acquisition is still draining, its failed result remains in force and no
  second underlying read is enqueued. A late result cannot restore healthy
  status; recovery requires a fresh successful observation.
- A single unlabelled gauge, `meshcore_privacy_generation_healthy`, is `1` only
  for complete, matching generations with an inactive queue. Divergence in
  either direction, missing/invalid evidence, active work or a read failure
  returns `0`. Comparisons use validated decimal strings and BigInt within the
  public reader's supported integer range.
- Each observation emits JSON event `privacy_generation_health` with the same
  boolean, a bounded reason, the generation triple and queue-active flag.
  Errors contain no dependency address, raw exception, packet or node data.
- Registered the bounded collector on existing metrics scrapes, independently
  of slower dashboard and worker snapshot queries. The existing health overview
  also reports critical incident `privacy_generation_unhealthy` and private
  detail `privacy_visibility`. Public health retains only incident code/severity.
- Added `PrivacyGenerationUnhealthy` to the existing Prometheus rules: a zero
  **or absent** backend gauge sustained for five minutes alerts. Successful
  observation clears it; brief queued work does not page. Existing backend
  scrape and rule-file wiring suffice; no new CLI is needed.
- Extended `docs/privacy-convergence.md` with signal definitions, wiring,
  boundedness, alert handling and the operator runbook.

### Meaning of applied generation

This repository persists **two** privacy generation records. Desired is
`public_visibility_state.generation`; materialized is
`packet_visibility_materialization_state.visibility_generation`. Applied here
means the generation admissible to `loadCurrentPublicPrivacyPolicy`'s global
read gate: materialized only when it equals desired and no pending, processing
or failed rematerialization exists; otherwise null.

That definition is explicit in code and the runbook. It is not an independently
persisted CoreScope cursor, cache generation or chart artefact. The comparator
also rejects an independently inconsistent applied-ahead input defensively;
the real SQL refuses to admit an ahead certificate at all. Node-scoped SQL
packet gates can still admit unaffected rows while global batch admission is
blocked by queued work.

## Verification

All commands below exited **0**. No tests were skipped.

From `backend/`:

```sh
npm run build
node --import tsx --test src/privacy/generationHealth.test.ts src/privacy/convergence.test.ts src/health/statusIntegrity.test.ts src/health/snapshot.test.ts src/metrics.test.ts src/db/packetVisibilityFence.test.ts src/db/publicVisibilityMigration.test.ts src/api/utils/networkFilters.test.ts
```

Build passed. **29/29 unit tests passed**, covering no difference, desired ahead,
materialized ahead, applied ahead/behind, missing artefacts, integer precision,
active queues, metric/log agreement, redaction, timeouts, coalescing and recovery,
alongside the existing fence and health regressions.

From the worktree root:

```sh
bash scripts/test-privacy-fence.sh
```

**17/17 integration tests passed** after the change (baseline: 16/16).
The script created a temporary, loopback-only, tmpfs-backed container using the
CI-pinned TimescaleDB image and removed that owned fixture on exit. It did not
load application `.env` or Compose. The added case ran the actual probe SQL as
a SELECT-only role with `default_transaction_read_only=on`; an UPDATE was
rejected. It verified healthy state, empty-queue desired-ahead drift, an ahead
certificate, missing state rows, pending/processing/failed work, recovery and a
missing relation. Snapshot comparisons proved observations left packet flags,
queue contents, generations and timestamps unchanged.

The existing suite also verified old-trigger reproduction, fixed empty
cascades, real mutations, worker/alias safeguards, rollback and competing-writer
behavior, and **100,001 compressed historical rows in each packet store**.
Its existing repair tests call the repair API only in the disposable fixture;
the only convergence CLI execution in the suite is the default SELECT-only
dry-run. No `privacy:converge --apply` invocation occurred.

From the worktree root:

```sh
docker run --rm -v "$PWD/logging/rules:/rules:ro" --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules /rules/meshcore.test.yml
docker run --rm -v "$PWD/logging/prometheus.yml:/etc/prometheus/prometheus.yml:ro" -v "$PWD/logging/rules:/etc/prometheus/rules:ro" --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 check config /etc/prometheus/prometheus.yml
git diff --check
```

Rule tests reported **SUCCESS**. Added cases cover healthy state, alert holdoff,
drift, recovery, brief work and absent telemetry. Config validation succeeded
with **28 rules**. Whitespace validation passed.

## Deliberately excluded, open questions and remaining work

No deployment, restart/reload of existing services, VPS access, live DB work,
live configuration changes or agent spawning was performed. The merged trigger
fix, repair command, generation writers and privacy policy remain unchanged.
The only service lifecycle operations belonged to disposable local test containers.
No branch push, PR publication or backlog mutation was needed for this handoff.

The probe certifies generation/admission agreement, not the correctness of every
historical flag; the existing read-only inventory provides that deeper audit.
It performs no automatic repair and does not reset processing claims or advance
generations. Existing liveness and dependency readiness semantics are unchanged.

Normal, separately authorized release work must activate the backend/health
code and Prometheus rule, then verify telemetry in the deployed environment.
That operational validation was deliberately not attempted under this envelope.
If “applied” must instead refer to an independent downstream artefact, that
consumer needs an authoritative read-only generation interface; none is present
in this repository. #672's remaining writer investigation and CoreScope replay
cost/follow-only behavior remain separate follow-ups, requiring their own scope.
