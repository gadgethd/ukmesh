# UKMesh fix review — 2026-10-02

Branch: `fix/ukmesh-burn-20261002`. Base: `73ee004` (`ukmesh-w5`).
Scope: the UKMesh burn brief, including the stretch queue. All edits and local
verification stay in this worktree. No deployment, container restart, migration,
live-tree access, or environment change is authorized or performed.

## Baseline and evidence

- Read `knowledge.md`, `AI_MEMORY.md`, and the full supplied brief, and consulted
  OpenViking. Tokensave tools and `.tokensave/tokensave.db` are absent here, so
  structural investigation uses scoped source searches and Git objects.
- The initial worktree was clean. `git symbolic-ref refs/remotes/origin/HEAD`
  fails because the local symbolic ref is absent. `git ls-remote --symref origin
  HEAD` and `gh repo view gadgethd/ukmesh --json defaultBranchRef,nameWithOwner`
  both identify `main` (remote HEAD `74d3ff1`). The requested integration base
  remains `ukmesh-w5`; changing it would change the brief's scope.
- Dependencies were missing; `npm ci` in `backend` and `frontend` installed the
  committed lockfiles without changing them. Initial backend `npm test` after
  installation: **327/327 pass, 0 fail, 0 skipped**.
- The brief describes additional untracked live-checkout code. Neither
  `runState.test.ts` nor `ownerLastHopPrewarm.ts` exists on the initial branch.
  Git history contains the former and its full policy in `d834ba8`, and an
  earlier feed measurement implementation in `333f180`. No live checkout was
  inspected or copied.
- `push.md` is ignored but absent from this worktree and all Git history. Its
  location has been requested; push validation and disposition will be recorded
  before publishing.
- Issue numbers in the brief are not assumed to be GitHub issue IDs:
  `gh issue view 438 --repo gadgethd/ukmesh` finds no such issue. The supplied
  brief is the requirements source.

## 1. Unit suite — #536 / #437 / #301

Decision: option (c), restore the complete intended retired-workload policy,
including the missing export and production callers, from the isolated prior
fix `d834ba8`. The initial `runState.ts` exactly matches that commit's parent.
This is not a dummy export to make a test load: migration 044 explicitly removed
the path-history cache, API and worker; `beginAnalysisRun` must reject a new
retired lease, and `getAnalysisWorkloadStates` must describe retirement while
preserving historical failures for audit. Supported workloads and unexpected
active retired runs continue to expose their actual status and errors.

Evidence: after restoring only the historical test and `workloadPolicy.ts`,
`cd backend && node --import tsx --test src/analysis/runState.test.ts` fails with
`SyntaxError: ... does not provide an export named 'normalizeRetiredAnalysisState'`.
After restoring the source policy wiring, the same command passes **3/3**, with
no database needed. `cd backend && npm test` now passes **330/330**, 0 fail,
0 skipped; `cd backend && npm run typecheck` passes. Final-HEAD rerun pending.

## 2. Owner last-hop prewarm — #438

The named file is absent from the integration base. Recreated a prewarmer around
the existing `ownerService.getOwnerLastHopStrength` cache and wired it after
owner-auth initialization, with lifecycle shutdown draining. This branch exposes
one rolling seven-day result (`ownerRepository.fetchLastHopStrength`), not the
live brief's two separate windows; no speculative window API was added.

Each pass enumerates active, verified owner grants, deduplicates nodes per owner,
and refreshes at most two owner/node pairs concurrently. Optional
`OWNER_LAST_HOP_PREWARM_CONCURRENCY=1` selects sequential work; values above two
remain capped at two. The next pass starts 30 minutes after completion, so cold
passes cannot overlap. Start/end logs and every ten completed nodes report
owners/nodes completed and total, refreshed and failed. A refresh warns at 20s
while still in flight; failures are counted and do not stop the pass.

Forced warm refreshes retain incremental last-hop merging. Foreground reads and
warm refreshes share in-flight work. Cache keys include all owned nodes because
that exclusion set changes the last-hop query; warming a shared node for one
owner must not serve another owner's exclusion scope.

Validation: `cd backend && node --import tsx --test
src/owner/ownerLastHopPrewarm.test.ts src/owner/ownerService.test.ts`:
**8/8 pass**, covering the concurrency cap, progress/failure/owner accounting,
single-flight, deduplication, an in-flight slow failure, shutdown admission,
cache ownership scope and retry. `cd backend && npm run typecheck` passes;
`cd backend && npm test`: **338/338 pass**, 0 fail, 0 skipped.
Production cold-pass time and DB load are not measured.

## 3. Heartbeat alert semantics — #543 / #507

Confirmed `currentWorkers` set `worker="link"` from `MAX(itm_computed_at)`.
Replaced that source with Redis `meshcore:link:v3:worker_heartbeat`. The current
worker's independent thread writes Unix seconds every 10s with a **45s TTL**
(the brief's 39s is a live observation, not the source's configured TTL).
`last_activity_at` still reports ITM write activity separately.

The Prometheus gauge collects Redis on each scrape, rather than freezing the
age until the five-minute health pass. Reads time out at 2s; missing, invalid,
excessively future or unreadable timestamps publish `-1`, replacing any earlier
healthy value. The queue alert now handles `< 0` as well as `> 180`, with the
existing three-minute holdoff. No threshold increase is needed for a genuine
ten-second heartbeat. Empty queues do not trigger this alert.

Validation: `cd backend && node --import tsx --test
src/health/workerHeartbeat.test.ts`: **5/5 pass**, including the real Redis key,
timestamp validation, expiry/failure, bounded timeout, repeated real metric
scrapes and recovery. `npm run typecheck` passes; `npm test`: **343/343 pass**,
0 fail, 0 skipped. `.ukmesh-tools/promtool check rules
logging/rules/meshcore.yml`: **24 rules valid**. `.ukmesh-tools/promtool test rules
logging/rules/meshcore.test.yml`: **7 scenario groups pass**, including batch
gaps, missing heartbeats, holdoff, recovery, idle queues and existing stale cases.

Standalone promtool 3.15.0 was downloaded from the [official release listing](https://prometheus.io/download/?trk=direct)
into an ignored worktree-local tools directory, without starting a container.
Archive SHA256: `2a542df32eac02ee17b9d844fb2aa1de00dafa5476579ba8a3ba862e9d572ea0`,
matching the official listing. Live metric/alert behavior remains deploy-gated.

## 4. UK feed virtualizer — #303

Implementation and validation pending.

## Stretch queue

- #424 Compose adoption guard: pending reproduction and local guard verification.
- #447 stale node archive/delete: pending implementation and tests.
- Full backend/frontend suites on final HEAD: pending.
- Other proxy heartbeat signals and alert consumers: pending audit.

## Deployment gate, limitations and open questions

The operator must separately review, merge and deploy code/rules. Local tests
cannot prove live DB load, cold-pass wall time, Redis freshness in production,
alert recovery or container adoption. This session will not perform those
operations. `push.md` is the outstanding process-document question.
