# B1 owner baseline auto-rebaseline handoff

## Status

Partial: implementation and unit tests are ready, but the refresh script needed
by the wrapper is not in this branch's base. `HEAD` is based on `d44ef5e`, where
`scripts/refresh-owner-baseline.sh` is absent. The script exists in historical
commit `4be9542` on `security/remediation-20260912`; this branch does not contain
that commit. The VPS `--check` run is complete and confirms the current
baseline is healthy. An actual refresh validation pass and live timer operation
remain gated on the refresh script dependency and Ben's installation of the
user timer.

## Branch and commits

- Branch: `fix/baseline-auto-rebaseline`
- Base at start: `d44ef5e5720f688789717498b3b2e4f0993d11b7`
- Commit hashes: base `d44ef5e5720f688789717498b3b2e4f0993d11b7`; implementation `4325f98` (`feat(ops): auto-rebaseline owner ACL inventory`).
- Push state: not pushed; coordinator relays the branch

## Files changed

- `backend/src/owner/autoRebaseline.ts` — readiness diff decisions and strict refresh validation parsing.
- `backend/src/owner/autoRebaseline.test.ts` — generation detection and abort-path tests.
- `backend/src/tools/autoRebaseline.ts` — read-only `--check` and timer wrapper.
- `deploy/systemd/user/owner-baseline-autorebaseline.service` — journald logging and `flock` overlap lock.
- `deploy/systemd/user/owner-baseline-autorebaseline.timer` — ten-minute schedule.
- `INSTALL.md` — VPS install, verify, and disable steps for Ben.
- `evidence/owner-baseline-autorebaseline-check.txt` — the read-only VPS check output.
- `HANDOFF.md` — this report.

## Tests and evidence

- Focused backend test command: `cd backend && node --import tsx --test src/owner/autoRebaseline.test.ts` — passed, 6 tests.
- Backend type check: `npm --prefix backend run typecheck` — passed.
- Backend build: `npm --prefix backend run build` — passed.
- VPS check-mode command: `node backend/dist/tools/autoRebaseline.js --check` — passed with `action=skip`, `reason=owner-baseline-current`, `mismatches=[]`, `applied=false`. It reported `refreshScriptAvailable=false`; script validation was not run in check mode.
- Dry-run evidence: `evidence/owner-baseline-autorebaseline-check.txt`.
- No `--apply` invocation, service/container start/restart, timer installation, or timer enablement was performed.

## Gated items for Ben

- Ensure the reviewed `scripts/refresh-owner-baseline.sh` is merged into this branch and the VPS checkout before installing/enabling these units.
- Review this branch and the exact check output before enabling the timer.
- Install and enable only using the commands in `INSTALL.md`; no unit was installed or enabled by this task.

## Open questions

- The historical refresh script commit `4be9542` is on `security/remediation-20260912`; merge it before this branch or add the script as a separate dependency commit.
- The required no-apply refresh-script validation pass starts one-shot Docker containers and writes its candidate under a temporary directory. It was not executed because the VPS hard rules prohibit container starts and writes outside this worktree.
