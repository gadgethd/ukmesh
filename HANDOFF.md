# B1 owner baseline auto-rebaseline handoff

## Status

The refresh-script dependency is resolved. The reviewed script and operations
documentation from `4be9542` are now on this branch as cherry-pick `7f8f4b8`.
The script MD5 is `5479d7aa1fee57d4f81d35e4fdf69699`, matching the requested
deployed version. The parser's expected summary format matches the checked-in
script's validation output emitter, and a fixture-based parser test covers that
format. The fixture uses synthetic values; the full refresh validation pass has
not been run.

## Branch and commits

- Branch: `fix/baseline-auto-rebaseline` (initial B1 base: `d44ef5e`).
- Prior B1 commits: `4325f98` (`feat(ops): auto-rebaseline owner ACL inventory`)
  and `ead7ee9` (`docs(ops): handoff owner baseline autorebaseline`).
- Dependency commit: `7f8f4b8`, cherry-picked from reviewed source commit
  `4be9542` (`ops: add reviewed owner-authorization baseline refresh script`).
- This B1b handoff, evidence, and parser fixture update is committed separately.
- Push state: not pushed; coordinator relays the branch.

## Files changed

- `backend/src/owner/autoRebaseline.ts` — readiness decisions and refresh
  validation summary parsing.
- `backend/src/tools/autoRebaseline.ts` — read-only check and timer wrapper.
- `deploy/systemd/user/owner-baseline-autorebaseline.service` and
  `deploy/systemd/user/owner-baseline-autorebaseline.timer` — user service and
  ten-minute timer units.
- `INSTALL.md` — VPS install, verification, and disable instructions for Ben.
- `scripts/refresh-owner-baseline.sh` — reviewed validation and optional apply
  script; executable bit preserved.
- `docs/operations.md` — reviewed owner-baseline refresh procedure.
- `backend/src/owner/autoRebaseline.test.ts` — fixture-backed validation output
  parser test.
- `backend/src/owner/fixtures/refresh-owner-baseline-output.txt` — sample
  stdout matching the script's summary emitter; fixture values are synthetic.
- `evidence/owner-baseline-autorebaseline-check.txt` — latest read-only check
  output.
- `HANDOFF.md` — this report.

## Tests and evidence

- Focused test: `cd backend && node --import tsx --test src/owner/autoRebaseline.test.ts` — passed, 6 tests.
- Backend type check: `npm --prefix backend run typecheck` — passed.
- Backend build: `npm --prefix backend run build` — passed.
- Read-only check: `node backend/dist/tools/autoRebaseline.js --check` — passed
  with `action=skip`, `reason=owner-baseline-current`, `mismatches=[]`,
  `refreshScriptAvailable=true`, `scriptValidation=not-run-in-check-mode`, and
  `applied=false`.

Exact `--check` output:

```json
{
  "mode": "check",
  "action": "skip",
  "reason": "owner-baseline-current",
  "mismatches": [],
  "refreshScriptAvailable": true,
  "scriptValidation": "not-run-in-check-mode",
  "applied": false
}
```

No refresh script invocation, `--apply`, service/container action, timer
installation, or timer enablement was performed.

## Ben-gated next steps

- Run the full no-apply refresh validation pass and review its result. This was
  not run here because the brief gates it to Ben; the script's validation
  requires starting a one-shot container and writes a candidate baseline.
- After review, install and enable the user timer using the instructions in
  `INSTALL.md`. No unit was installed or enabled by this task.
