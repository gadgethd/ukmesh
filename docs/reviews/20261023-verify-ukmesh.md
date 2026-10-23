# Independent ukmesh burn verification — 2026-10-23

Checks ran **16:20:42–16:50:47 BST**. All checks were run independently in `/home/ben/worktrees/ukmesh/verify-20261023`. The first ten branches were checked out and tested in the brief’s priority order; broker, journal and pr127 were then included because the brief scopes all `burn/*20261023` branches. No source fixes, merges, deployments, service restarts or database migrations were performed. The current goal’s **17:30 BST** deadline supersedes the brief’s older 16:15 BST deadline.

Runtime: Node **v22.22.2**, npm **10.9.7**. Backend CI documents Node 20 and frontend CI Node 22; these local runs used the installed Node 22 for both. Before every branch, `git fetch --all` exited 0, then the branch was checked out in this scratch worktree. Branches owned by other worktrees were checked out with `git checkout --detach <branch>`; their branch refs and worktrees were untouched.

The clone’s fetch refspec is `+refs/heads/main:refs/remotes/origin/main`. GitHub burn tips were inspected with `git ls-remote --heads origin 'burn/*20261023'`; the recorded local commit is the verification target. The onboard local branch was ahead of its published GitHub tip at the initial inspection.

Command sources read on each branch: `README.md`, `backend/package.json`, `frontend/package.json`, and `.github/workflows/ci.yml`. There were no tracked `AGENTS.md` or `CONTRIBUTING.md` files. Every branch received fresh `npm ci` installations in both directories.

## Merge matrix

`SAFE TO MERGE` means the documented backend/frontend unit, contract, typecheck and build checks in this report passed at the recorded commit. It is an individual-branch verdict; it does not certify the combined merge result or unexecuted infrastructure/integration gates. Skipped tests are not counted as passes.

| Branch | Tested commit | Verdict | Backend passed / skipped | Frontend passed / skipped | Failing tests | Notes |
|---|---|---|---:|---:|---|---|
| `burn/ukmesh-ingest-20261023` | `a542111fc7656be203c3a19e68230038750a6a77` | **SAFE TO MERGE** | 582 / 3 | 149 / 6 | None | Required priority 1; PR #151 |
| `burn/ukmesh-ci-20261023` | `ec51e459dc9c3e06a0a29f7a5d9806ea7234cc29` | **SAFE TO MERGE** | 573 / 3 | 149 / 6 | None | Required priority 2; PR #150 |
| `burn/ukmesh-readyz-20261023` | `a5bfa546af220804ce76ec7dd99b27a7671daf7f` | **SAFE TO MERGE** | 583 / 3 | 149 / 6 | None | Required priority 3; PR #149 |
| `burn/ukmesh-converge-20261023` | `7e157bb9988ad79938bbd3747902353754446c71` | **SAFE TO MERGE** | 581 / 3 | 149 / 6 | None | Required priority 4 |
| `burn/ukmesh-regions-20261023` | `2116667845144289f42e6ce0c3066a58ddbe102d` | **SAFE TO MERGE** | 579 / 3 | 153 / 6 | None | Required priority 5 |
| `burn/ukmesh-data-20261023` | `320e6c6ca79b6564811ebb4a063c845c771b9198` | **SAFE TO MERGE** | 577 / 3 | 149 / 6 | None | Required priority 6 |
| `burn/ukmesh-alerts-20261023` | `0522ef4f40effe1d04e0f3ea12ec96c359370c52` | **SAFE TO MERGE** | 573 / 3 | 149 / 6 | None | Required priority 7 |
| `burn/ukmesh-preheat-20261023` | `acddc0478b6bc58d73238896d79aa9bcc2b4952b` | **SAFE TO MERGE** | 586 / 3 | 149 / 6 | None | Required priority 8 |
| `burn/ukmesh-shm-20261023` | `4330e704cb437ce4ba7dfff27774dae516bb5a9a` | **SAFE TO MERGE** | 573 / 3 | 149 / 6 | None | Required priority 9 |
| `burn/ukmesh-onboard-20261023` | `f43aa0033a0d80dc07287da93aa77032c88cc203` | **SAFE TO MERGE** | 583 / 3 | 151 / 6 | None | Required priority 10; local tip ahead of published tip |
| `burn/ukmesh-broker-20261023` | `c0eac55e03923ea4ade18a27edde63d0f2247f76` | **SAFE TO MERGE** | 585 / 3 | 149 / 6 | None | Additional branch in scope |
| `burn/ukmesh-journal-20261023` | `73ab7d4fc63e4968a5fd46af022696ad69d16d9b` | **SAFE TO MERGE** | 573 / 3 | 149 / 6 | None | Additional branch in scope; unchanged from base |
| `burn/ukmesh-pr127-20261023` | `73ab7d4fc63e4968a5fd46af022696ad69d16d9b` | **SAFE TO MERGE** | 573 / 3 | 149 / 6 | None | Additional branch in scope; unchanged from base; local-only branch |

## Exact commands and exit codes

Working directories below are relative to the scratch repository. All commands completed without a harness timeout (300-second per-command limit). Backend `npm test` also runs `bash ../scripts/test-alert-receiver.sh` after its Node test suite.

| Branch suffix (`burn/ukmesh-…-20261023`) | Directory | Exact command | Exit | Failing tests | Result / notes |
|---|---|---|---:|---|---|
| ingest | `backend` | `npm ci` | 0 | None | 61.34s |
| ingest | `backend` | `npm run typecheck` | 0 | None | 7.51s |
| ingest | `backend` | `npm run contract:check` | 0 | None | 1.65s |
| ingest | `backend` | `npm test` | 0 | None | 585 discovered; 582 passed; 3 skipped; 0 failed; 68.65s |
| ingest | `backend` | `npm run build` | 0 | None | 7.48s |
| ingest | `frontend` | `npm ci` | 0 | None | 103.37s |
| ingest | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 26.36s |
| ingest | `frontend` | `npm run build` | 0 | None | 18.00s |
| ci | `backend` | `npm ci` | 0 | None | 10.67s |
| ci | `backend` | `npm run typecheck` | 0 | None | 10.26s |
| ci | `backend` | `npm run contract:check` | 0 | None | 2.09s |
| ci | `backend` | `npm test` | 0 | None | 576 discovered; 573 passed; 3 skipped; 0 failed; 73.36s |
| ci | `backend` | `npm run build` | 0 | None | 4.92s |
| ci | `frontend` | `npm ci` | 0 | None | 54.28s |
| ci | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 24.38s |
| ci | `frontend` | `npm run build` | 0 | None | 18.41s |
| readyz | `backend` | `npm ci` | 0 | None | 12.69s |
| readyz | `backend` | `npm run typecheck` | 0 | None | 12.74s |
| readyz | `backend` | `npm run contract:check` | 0 | None | 3.26s |
| readyz | `backend` | `npm test` | 0 | None | 586 discovered; 583 passed; 3 skipped; 0 failed; 66.14s |
| readyz | `backend` | `npm run build` | 0 | None | 6.31s |
| readyz | `frontend` | `npm ci` | 0 | None | 56.11s |
| readyz | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 20.54s |
| readyz | `frontend` | `npm run build` | 0 | None | 17.82s |
| converge | `backend` | `npm ci` | 0 | None | 7.45s |
| converge | `backend` | `npm run typecheck` | 0 | None | 6.46s |
| converge | `backend` | `npm run contract:check` | 0 | None | 1.37s |
| converge | `backend` | `npm test` | 0 | None | 584 discovered; 581 passed; 3 skipped; 0 failed; 69.25s |
| converge | `backend` | `npm run build` | 0 | None | 3.86s |
| converge | `frontend` | `npm ci` | 0 | None | 34.65s |
| converge | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 26.39s |
| converge | `frontend` | `npm run build` | 0 | None | 13.28s |
| regions | `backend` | `npm ci` | 0 | None | 4.59s |
| regions | `backend` | `npm run typecheck` | 0 | None | 5.22s |
| regions | `backend` | `npm run contract:check` | 0 | None | 0.84s |
| regions | `backend` | `npm test` | 0 | None | 582 discovered; 579 passed; 3 skipped; 0 failed; 55.90s |
| regions | `backend` | `npm run build` | 0 | None | 3.25s |
| regions | `frontend` | `npm ci` | 0 | None | 34.01s |
| regions | `frontend` | `npm test` | 0 | None | 159 discovered; 153 passed; 6 skipped; 0 failed; 23.88s |
| regions | `frontend` | `npm run build` | 0 | None | 10.19s |
| data | `backend` | `npm ci` | 0 | None | 4.16s |
| data | `backend` | `npm run typecheck` | 0 | None | 4.50s |
| data | `backend` | `npm run contract:check` | 0 | None | 0.73s |
| data | `backend` | `npm test` | 0 | None | 580 discovered; 577 passed; 3 skipped; 0 failed; 46.28s |
| data | `backend` | `npm run build` | 0 | None | 5.23s |
| data | `frontend` | `npm ci` | 0 | None | 27.03s |
| data | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 15.60s |
| data | `frontend` | `npm run build` | 0 | None | 11.73s |
| alerts | `backend` | `npm ci` | 0 | None | 10.31s |
| alerts | `backend` | `npm run typecheck` | 0 | None | 12.47s |
| alerts | `backend` | `npm run contract:check` | 0 | None | 2.44s |
| alerts | `backend` | `npm test` | 0 | None | 576 discovered; 573 passed; 3 skipped; 0 failed; 48.36s |
| alerts | `backend` | `npm run build` | 0 | None | 2.18s |
| alerts | `frontend` | `npm ci` | 0 | None | 48.39s |
| alerts | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 15.46s |
| alerts | `frontend` | `npm run build` | 0 | None | 7.00s |
| preheat | `backend` | `npm ci` | 0 | None | 4.25s |
| preheat | `backend` | `npm run typecheck` | 0 | None | 3.45s |
| preheat | `backend` | `npm run contract:check` | 0 | None | 0.61s |
| preheat | `backend` | `npm test` | 0 | None | 589 discovered; 586 passed; 3 skipped; 0 failed; 53.58s |
| preheat | `backend` | `npm run build` | 0 | None | 5.11s |
| preheat | `frontend` | `npm ci` | 0 | None | 27.00s |
| preheat | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 22.28s |
| preheat | `frontend` | `npm run build` | 0 | None | 11.55s |
| shm | `backend` | `npm ci` | 0 | None | 6.13s |
| shm | `backend` | `npm run typecheck` | 0 | None | 6.43s |
| shm | `backend` | `npm run contract:check` | 0 | None | 1.04s |
| shm | `backend` | `npm test` | 0 | None | 576 discovered; 573 passed; 3 skipped; 0 failed; 53.01s |
| shm | `backend` | `npm run build` | 0 | None | 4.51s |
| shm | `frontend` | `npm ci` | 0 | None | 32.80s |
| shm | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 19.19s |
| shm | `frontend` | `npm run build` | 0 | None | 8.99s |
| onboard | `backend` | `npm ci` | 0 | None | 5.78s |
| onboard | `backend` | `npm run typecheck` | 0 | None | 5.10s |
| onboard | `backend` | `npm run contract:check` | 0 | None | 0.74s |
| onboard | `backend` | `npm test` | 0 | None | 586 discovered; 583 passed; 3 skipped; 0 failed; 47.01s |
| onboard | `backend` | `npm run build` | 0 | None | 3.24s |
| onboard | `frontend` | `npm ci` | 0 | None | 26.63s |
| onboard | `frontend` | `npm test` | 0 | None | 157 discovered; 151 passed; 6 skipped; 0 failed; 19.88s |
| onboard | `frontend` | `npm run build` | 0 | None | 9.60s |
| broker | `backend` | `npm ci` | 0 | None | 5.21s |
| broker | `backend` | `npm run typecheck` | 0 | None | 3.76s |
| broker | `backend` | `npm run contract:check` | 0 | None | 0.54s |
| broker | `backend` | `npm test` | 0 | None | 588 discovered; 585 passed; 3 skipped; 0 failed; 75.08s |
| broker | `backend` | `npm run build` | 0 | None | 4.16s |
| broker | `frontend` | `npm ci` | 0 | None | 46.60s |
| broker | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 27.95s |
| broker | `frontend` | `npm run build` | 0 | None | 9.63s |
| journal | `backend` | `npm ci` | 0 | None | 4.68s |
| journal | `backend` | `npm run typecheck` | 0 | None | 4.47s |
| journal | `backend` | `npm run contract:check` | 0 | None | 0.63s |
| journal | `backend` | `npm test` | 0 | None | 576 discovered; 573 passed; 3 skipped; 0 failed; 56.66s |
| journal | `backend` | `npm run build` | 0 | None | 5.57s |
| journal | `frontend` | `npm ci` | 0 | None | 28.01s |
| journal | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 19.26s |
| journal | `frontend` | `npm run build` | 0 | None | 13.23s |
| pr127 | `backend` | `npm ci` | 0 | None | 5.47s |
| pr127 | `backend` | `npm run typecheck` | 0 | None | 3.96s |
| pr127 | `backend` | `npm run contract:check` | 0 | None | 0.64s |
| pr127 | `backend` | `npm test` | 0 | None | 576 discovered; 573 passed; 3 skipped; 0 failed; 48.95s |
| pr127 | `backend` | `npm run build` | 0 | None | 6.13s |
| pr127 | `frontend` | `npm ci` | 0 | None | 28.18s |
| pr127 | `frontend` | `npm test` | 0 | None | 155 discovered; 149 passed; 6 skipped; 0 failed; 17.90s |
| pr127 | `frontend` | `npm run build` | 0 | None | 13.60s |

Final reference audit at `2026-10-23T15:52:35.385748+00:00`: all local burn tips still matched the tested SHAs, and every published burn branch was represented. Onboard’s published tip was `73ab7d4fc63e4968a5fd46af022696ad69d16d9b`, behind its tested local tip; pr127 had no published remote branch.

## Base comparison and limits

All tested branches share merge base `73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.

The default backend/frontend commands produced no failures, so the brief’s failure-triggered base comparison was not needed for those commands. Journal and pr127 independently ran the same complete command set at the shared base commit.

Dedicated PostgreSQL/Redis integration scripts, RAM operational checks, Docker application/worker image builds and full CI infrastructure jobs are outside the default command set above and are not certified by these results. No database fixture or migration command was run. Targeted browser and auxiliary checks are recorded separately below.

Existing unit skips: backend has three live read-only database EXPLAIN checks (RF repository, history, and node-export SQL). Frontend has six owner/topology recovery tests (owner HTTP 503, timeout and invalid-schema recovery; owner refresh/stale response; topology HTTP 503 and timeout recovery).

## Auxiliary checks

Targeted commands come from the relevant branch’s review document, package scripts or CI workflow. Browser runs used isolated local ports; operator scripts used their mocked Docker fixtures. Full logs remain under `/tmp/ukmesh-verify-20261023/`; this report records commands, exits and summaries.

| Branch suffix | Directory | Exact command | Exit | Failing tests | Result |
|---|---|---|---:|---|---|
| ingest | `.` | `docker run --rm -v /home/ben/worktrees/ukmesh/verify-20261023/logging/rules:/rules:ro --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules /rules/meshcore.test.yml` | 0 | None | Prometheus rule fixtures passed; 25.59s |
| ci | `.` | `python3 -m unittest discover -s scripts -p test_ci_retry.py -v` | 0 | None | 12 retry cases passed; 3.10s |
| converge | `.` | `docker run --rm -v /home/ben/worktrees/ukmesh/verify-20261023/logging/rules:/rules:ro --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules /rules/meshcore.test.yml` | 0 | None | Prometheus rule fixtures passed; 31.53s |
| regions | `backend` | `npm run test:regions` | 0 | None | 36 passed; 0 skipped; 0 failed; 10.00s |
| regions | `frontend` | `env PLAYWRIGHT_PORT_BASE=46930 npx playwright test regions.spec.ts --project=dashboard-desktop --project=dashboard-mobile --workers=1 --repeat-each=2` | 0 | None | 12 browser cases passed; 109.36s |
| data | `frontend` | `env PLAYWRIGHT_PORT_BASE=46930 npm run test:e2e -- --grep 'former node names' --workers=1` | 0 | None | 3 browser cases passed; 24.08s |
| alerts | `.` | `docker run --rm -v /home/ben/worktrees/ukmesh/verify-20261023/logging/rules:/rules:ro --entrypoint /bin/promtool prom/prometheus@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 test rules /rules/meshcore.test.yml` | 0 | None | Prometheus rule fixtures passed; 93.10s |
| alerts | `frontend` | `env PLAYWRIGHT_PORT_BASE=46930 npx playwright test dashboard.spec.ts --project=dashboard-desktop --project=dashboard-mobile --grep 'RF coverage remains available with 3D terrain' --workers=1 --retries=0 --repeat-each=2` | 0 | None | 4 browser cases passed; 122.64s |
| preheat | `.` | `bash scripts/test-check-compose-adoption.sh` | 0 | None | 9 mocked Compose adoption/repair drills passed; 0.68s |
| shm | `.` | `python3 -m unittest discover -s scripts -p test_postgres_shm.py -v` | 0 | None | 10 unittest cases passed; 2.14s |
| onboard | `.` | `bash scripts/test-newuser.sh` | 0 | None | Mocked newuser regression suite passed; 1.08s |
| onboard | `frontend` | `env PLAYWRIGHT_PORT_BASE=46930 node node_modules/@playwright/test/cli.js test owner.spec.ts --project=public-desktop --workers=1 --timeout=60000 --grep 'unlinked owner\|linked owner sees a recent'` | 0 | None | 2 browser cases passed; 9.18s |

## One-line verdicts

- burn/ukmesh-ingest-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at a542111fc7656be203c3a19e68230038750a6a77.
- burn/ukmesh-ci-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at ec51e459dc9c3e06a0a29f7a5d9806ea7234cc29.
- burn/ukmesh-readyz-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at a5bfa546af220804ce76ec7dd99b27a7671daf7f.
- burn/ukmesh-converge-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 7e157bb9988ad79938bbd3747902353754446c71.
- burn/ukmesh-regions-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 2116667845144289f42e6ce0c3066a58ddbe102d.
- burn/ukmesh-data-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 320e6c6ca79b6564811ebb4a063c845c771b9198.
- burn/ukmesh-alerts-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 0522ef4f40effe1d04e0f3ea12ec96c359370c52.
- burn/ukmesh-preheat-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at acddc0478b6bc58d73238896d79aa9bcc2b4952b.
- burn/ukmesh-shm-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 4330e704cb437ce4ba7dfff27774dae516bb5a9a.
- burn/ukmesh-onboard-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at f43aa0033a0d80dc07287da93aa77032c88cc203.
- burn/ukmesh-broker-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at c0eac55e03923ea4ade18a27edde63d0f2247f76.
- burn/ukmesh-journal-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 73ab7d4fc63e4968a5fd46af022696ad69d16d9b.
- burn/ukmesh-pr127-20261023: SAFE TO MERGE — documented backend/frontend unit and build gates at 73ab7d4fc63e4968a5fd46af022696ad69d16d9b.

The PROOF receipt identifies the matrix content commit; a subsequent report-only commit records that known hash.

PROOF: matrix committed at `docs/reviews/20261023-verify-ukmesh.md` on branch `verify/ukmesh-20261023` at sha 2a37774492f783ca7f7ce8c6b463cd17f0a0f8f0, with one verdict line per branch
