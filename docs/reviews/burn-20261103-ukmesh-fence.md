# Lane H — public visibility fence — 2026-11-03

## STATUS

Implemented and locally verified the row-aware fix for HermesMind #672 on
`burn/ukmesh-fence-20261103`, based on `449bd478`. All source changes are confined
to `/home/ben/worktrees/ukmesh/fence-20261103`. Production investigation consisted
of read-only service/file/log inspection over SSH. No production SQL, deployment,
merge, tag, service restart, configuration change or additional agent was used.

The starting tree already contained migration 066's transition-table mitigation
for empty statements and a functioning generation observer/alert. Migration
071 strengthens that mitigation with `FOR EACH ROW`, policy-value comparison
and transaction deduplication. Historical migrations retain their checksums.

### Implementation and choice

- `backend/src/db/migrations/071_prefix_visibility_row_fence.sql` retains the
  prefix FK, replaces 066's INSERT/UPDATE/DELETE statement triggers with a row
  trigger, and preserves conservative TRUNCATE invalidation. Empty cascades
  cannot invoke a row trigger. Value-identical UPDATE/upsert operations also
  leave the generation unchanged, including updates to `updated_at` alone.
- A nullable `public_visibility_state.prefix_fence_xid` uses `XID8` and
  `pg_current_xact_id()` to invalidate once per transaction while materialization
  remains stale. The marker rolls back with the transaction/savepoint. If a
  certificate becomes current within the transaction, the next policy edit
  invalidates it again. Concurrent transactions publish independent bumps.
- Nested prefix maintenance on a surviving parent retains 056's queue ownership.
  A cascading deletion that actually removes prefixes has an absent parent and
  still invalidates, including a manually prefixed public node. This preserves
  066's conservative treatment of real policy changes and referential integrity.
  Dropping the FK would change orphan-prefix semantics unnecessarily.
- `backend/src/db/rollback/071_prefix_visibility_row_fence.sql` transactionally
  restores 066 and removes the marker. It leaves flags, queue and generations
  intact. A failed forward migration restores the complete previous trigger
  layout. Installing the migration never certifies a pre-existing gap.

### Generation observer and alert

The existing read-only `createPrivacyGenerationReconciler` in
`backend/src/privacy/generationHealth.ts` reads the generation pair and queue
presence in one MVCC snapshot. With `mat != gen` and an empty queue it reports
`healthy: false`, `reason: desired_ahead` (or `materialized_ahead`),
`queueActive: false`, and `appliedGeneration: null`. It publishes the boolean
`meshcore_privacy_generation_healthy` gauge and a bounded structured event.
The health-worker scrape drives this independently of cleanup/dashboard use.

`PrivacyGenerationUnhealthy` in `logging/rules/meshcore.yml` pages after five
minutes of zero **or absent** health-worker telemetry; its selector is scoped to
`job="meshcore-workers", role="health"`. Public health exposes only the critical
incident code. The integration reproduction now checks this observer against
the actual empty-cascade latch and healthy post-migration deletion. Existing
SELECT-only-role integration coverage proves observation changes no stored
flags, generations, queue entries or timestamps. Unit tests verify metric/log
publication, timeout/error handling and public redaction. Promtool verifies
holdoff, recovery, brief work and absent telemetry. No automatic counter copying
or historical repair was added.

### Remaining writers and VPS evidence

Read-only inspection on 2026-11-03 around 22:12–22:16 UTC used
`systemctl show/cat`, file reads, `docker ps`, and filtered `docker logs`.
The watcher script was inspected as text; it was never executed by this lane.

| Writer / evidence | Finding |
| --- | --- |
| `vis-watch.service` → `/home/ben/vis-watch.sh` → `/tmp/vis-watch.log` | Unit active/running. Script polls the generation pair and samples `pg_stat_activity` every two seconds; it contains SELECTs only. The watcher itself does not stamp or bump. |
| Watcher history | Repeated equal-pair increments remain visible, often +3. Recent entries include 915/915 at Nov 2 11:23:05, 918/918 at 17:22:59, 924/924 at Nov 3 12:57:09, and 927/927 at 21:32:42 UTC. These samples do not provide a transaction-level writer audit. |
| `node_identity_aliases_visibility_generation` (039), `bump_visibility_for_identity_table` (066) | Still a statement trigger: empty INSERT/UPDATE/DELETE and metadata-only UPDATE statements bump desired and carry an already-current certificate forward. These are remaining spurious stamping-bump paths. When mappings change, the automatic refresh's DELETE + INSERT can produce two bumps in one transaction. 071 intentionally scopes dedup to prefixes. |
| `refreshNodeIdentityAliases`, `backend/src/db/nodeIdentity.ts` | Automatic refresh already compares semantic source→canonical mappings and skips equivalent rewrites; metadata/evidence changes alone do not cause this application's routine alias rewrite. Startup and a 30-minute timer invoke it. Manual/other SQL writers still encounter the statement-trigger behavior above. |
| Runtime alias refresh | Backend logs at 20:32:43 show `aliasesWritten: 0, aliasRewriteSkipped: true`; at 21:32:41 they show 64 aliases written and `aliasRewriteSkipped: false`, shortly before watcher 927/927. This supports alias refresh as a contributor to that change, but cannot account for every increment or establish that the mapping change was spurious. |
| `cleanupNodeRecords`, `backend/src/maintenance/staleMqttObservers.ts` | Retains the old workaround that copies desired to materialized only when the fence was current before deleting revalidated, public, unprefixed candidates under locks. With 066/071, an empty prefix cascade has no bump to copy. An existing gap is preserved. Health logs show cleanup at 02:33, 08:33, 14:33 and 20:33 UTC; these times do not identify the later watcher bumps. |
| `processNextPrivacyRematerialization` and `convergePacketVisibility` | Intentional publication writers: each completed node/target pass advances the pair only from a current certificate; a verified full repair publishes one new pair. Two node targets can generate two replay invalidations. A no-drift full repair is inert. |

Health-worker logs at 22:16:04 reported 927/927, empty queue and healthy admission.
This is existing log evidence, not a fresh production database query or proof
that the proposed migration is installed. The remaining +3 increments cannot
be fully attributed without stronger writer telemetry.

### Replay-cost proposals (design only)

1. Give alias mutations the same changed-row/transaction semantics, comparing
   fields consumed by public projections. Publish one generation for a mapping
   replacement, and no generation for empty statements or audit-metadata edits.
2. Separate privacy-policy epochs from identity/display/cache epochs. CoreScope
   should invalidate its privacy replay only when the public visibility policy
   changes. Preserve fail-closed publication for privacy tightening.
3. Publish bounded privacy deltas keyed by epoch and affected identity/prefix,
   with a durable consumer checkpoint and idempotent replay. A complete retained
   delta chain permits targeted reclassification; missing/expired deltas require
   full replay. Test crashes, concurrent policy changes and epoch transitions.
4. Aggregate node `packets`/`packet_paths` completion behind one verified policy
   publication. Any batched coordinator needs durable claims, generation checks
   and complete readback before certification. Include writer/reason/transaction
   metadata so recurring equal-pair changes can be attributed without sampling.

These require separate design and correctness/performance review. Existing
privacy checks and the reviewed full-convergence repair remain available.

### PR #186 verdict — assess separately; do not merge

[PR #186](https://github.com/gadgethd/ukmesh/pull/186) remains **OPEN**, unmerged,
**MERGEABLE**, and its aggregate `mergeStateStatus` is **UNSTABLE** at head
`9d43341fdc61c57e5c458c799f98a928c201bc0e`. Both runs below use that exact head;
the failures are not merely checks on an earlier commit.

| CI run | Event / time (UTC) | Backend | Frontend | Workers and Compose | Secret scan |
| --- | --- | --- | --- | --- | --- |
| [41337102091](https://github.com/gadgethd/ukmesh/actions/runs/41337102091) | push, Nov 1 18:31:58 | pass | pass | fail | fail |
| [41337132446](https://github.com/gadgethd/ukmesh/actions/runs/41337132446) | pull_request, Nov 1 18:32:18 | pass | pass | pass | pass |

The failing push's Secret scan found no leaks in the four changed commits,
then failed `gitleaks git --redact --no-banner --log-opts=--all`: **43 findings
across 948 commits**. Its Workers job failed the empty-volume stack smoke because
`db-migrate` exited 1. Available job logs do not establish the migration's root
cause. The later PR run passed both checks, including its full-history step,
but does not retract the earlier failures or prove that their scopes matched.
Therefore the accurate status is successful PR-event CI with unresolved failed
push checks still attached to the same head, rather than an unconditional
all-green rollup.

The four commits (`f8b6fead`, `710f9c90`, `b8ae66fe`, `9d43341f`) change owner
ACL reconciliation draining, authenticated readiness diagnostics, retained
baseline evidence and repair plans. Their numeric global-fence coordinator is
explicitly deferred documentation. The source diff introduces no prefix fence
migration. Those owner fixes are absent from this lane's base and are **not
superseded** by 071; they should be reviewed/landed separately, with fresh checks
and disposition of the push failures. The shared `docs/privacy-convergence.md`
will need to retain both sets of documentation when integrating. No PR action
or merge was performed.

## COMMITS

- Implementation: `2ab2f5d42fec4776c4999f7b52838158cc87ab5e` — `fix(privacy): deduplicate row-aware prefix fence invalidations`.
- Review: this document in the following review commit.
- Branch: `burn/ukmesh-fence-20261103`.
- Compare: https://github.com/gadgethd/ukmesh/compare/main...burn/ukmesh-fence-20261103

## PROOF

`PROOF: migration=backend/src/db/migrations/071_prefix_visibility_row_fence.sql / test=backend/src/db/visibilityConvergence.integration.test.ts::reproduces the shipped no-row FK cascade, then proves rollback and reapply / suite=backend 761 pass, 3 skipped, 0 fail; privacy integration 20 pass, 0 skipped, 0 fail; Prometheus 21 cases pass / branch=pushed 2ab2f5d42fec4776c4999f7b52838158cc87ab5e (implementation; review commit follows)`

| Command / scope | Result |
| --- | --- |
| `cd backend && npm ci` | Committed dependencies installed in this worktree. |
| `cd backend && npm test` | 764 Node tests: **761 pass, 3 skipped, 0 fail**; synthetic alert receiver shell fixture passed. The three skips are optional live EXPLAIN tests, deliberately left without a live database. Includes migration-tree/checksum unit tests and observer/metric/public-health tests. |
| `cd backend && npm run typecheck` | Exit 0. |
| `bash scripts/test-privacy-fence.sh` | **20/20 pass, 0 skipped, 0 fail** (19 subtests plus parent). CI-pinned TimescaleDB in an owned, loopback-only, tmpfs-backed container; complete base schema and migration chain. |
| Promtool `test rules /rules/meshcore.test.yml` using CI-pinned Prometheus image | **21 cases pass**, including four privacy-generation alert cases. |
| Promtool `check config /etc/prometheus/prometheus.yml` with repository config/rules mounted read-only | Exit 0; **37 rules** validated. |
| `git diff --check` | Exit 0. |

The reproduction measures a **depth=1, zero-row prefix statement** after deleting
a public node; the original fence moves **1/1 → 2/1**, and the actual observer
reports an empty-queue closed gate. The populated 066→071 migration preserves
existing data and the old gap. After reset, the same node deletion leaves
**1/1**, with healthy admission. Additional regression subtests prove:

- 066's value-identical UPDATE creates a gap; 071's corresponding UPDATE and
  conflict upsert remain inert.
- Six prefix inserts plus later UPDATE/DELETE/TRUNCATE coalesce to one bump;
  savepoint/full rollback preserves dedup correctness; later certification and
  a subsequent edit re-fence; later/concurrent transactions bump independently.
- Real direct/cascading removals stay fenced; node queue/alias completion cannot
  certify an unrelated gap; read-only observation never repairs one.
- Repair readback/failure atomicity, competing writers, and **100,001 compressed
  historical rows in each packet store**, including 300-day-old data, still pass.

Local full command output is retained under ignored
`.ukmesh-tools/fence-20261103/`; no runtime logs or credentials are committed.

## OPEN QUESTIONS

- Which additional writer accounts for every recurring equal-pair increment?
  The sampled watcher and available runtime logs do not establish full causality.
  Add durable writer/reason attribution in a separate change.
- Before landing #186, resolve its same-head push failures and run current-base
  CI. The successful PR run supplies useful evidence but leaves that rollup mixed.

## RESIDUAL RISK

- 071 is branch-only. Applying it and any repair of an existing gap require the
  normal separate release/maintenance process; no live application was attempted.
- A real direct prefix change, actual prefix-removing node cascade or TRUNCATE
  still creates a fail-closed gap until verified full convergence. Removing a
  private identity can relax its policy; preserving deleted consent needs the
  separate tombstone design already documented by the repository.
- Node/alias publications still invalidate downstream CoreScope cursors. The
  proposed epoch/delta improvements and global automatic recovery are unimplemented.
- Local tests do not establish production lock duration or CoreScope replay
  throughput. The new singleton column/row-trigger layout and rollback need
  normal migration review. The migration bounds lock acquisition to five seconds
  and statements to thirty seconds and performs no historical backfill.
