# UKMesh readiness and public-feed ACL changes — 2026-10-23

Issue: https://github.com/gadgethd/ukmesh/issues/146
Branch: `burn/ukmesh-readyz-20261023`.
Implementation commit: `526adac9af1b32293f887411bb3731b46fa8f46e`.

## What changed and why

Self-service public-feed enable/disable changes the owner ACL generation even
when owner write permissions remain unchanged. `/readyz` now has a narrowly
scoped inventory-baseline exception for that sanctioned drift.

The existing `owner_acl_artifacts` table supplies the baseline and current
rendered ACLs, so no database or baseline-file schema change is needed. Only
artifacts recorded as applied in apply mode with successful validation qualify.
The readiness validator checks their content hashes and generation markers,
requires matching renderer versions, and verifies that the current ACL file
exactly matches the current applied artifact. Both baseline and current
desired/rendered/applied generations must be settled with no ACL error.

The classifier compares rendered directives in their user scope, including
unmanaged users and global directives. Only additions/removals of the exact
`topic read public/+/+/packets` grant qualify. It uses no username allowlist.
Other permissions, owner inventory/configuration changes, invalid or missing
evidence, and unexplained generation drift keep the original mismatch list and
fail closed. An unchanged baseline passes as before without artifact queries.
Every evaluated baseline decision has a `reason` in the readiness response and
is logged as `[readyz] owner inventory baseline`.

The strict inventory validator, inventory CLI, refresh script, and timer are
unchanged. A feed-only delta remains a mismatch for strict inventory validation;
the exemption applies only to readiness. Other readiness gates still apply.

## Evidence

Commands ran locally in this worktree, with no production connections.
From `backend/`:

```sh
mkdir -p .ukmesh-tools/tmp
TMPDIR="$PWD/.ukmesh-tools/tmp" node --import tsx --test src/owner/ownerInventoryReadiness.test.ts src/owner/ownerInventoryBaseline.test.ts src/owner/autoRebaseline.test.ts src/mqtt/aclManager.test.ts
npm run build
```

- Targeted runner: exit **0**, **31 passed**, **0 failed**, **0 skipped**.
- Backend compile/build: exit **0**, TypeScript `tsc` completed successfully.
- Temporary test files stayed under the worktree via `TMPDIR`.
- `git diff --check` and `git diff --check 73ab7d4f HEAD`: exit **0**.
- `git diff 73ab7d4f HEAD -- scripts/refresh-owner-baseline.sh backend/src/owner/ownerInventoryBaseline.ts backend/src/tools/inventoryOwnerAuthorization.ts backend/src/owner/autoRebaseline.ts deploy/systemd/user/owner-baseline-autorebaseline.service deploy/systemd/user/owner-baseline-autorebaseline.timer`: exit **0**, empty output.

The tests exercise the readiness inventory validator using actual ACL renderer
output and retained-artifact fixtures. Feed enable and disable pass; a mixed
feed/non-feed change fails; revoking a broker uptime read fails even when the
node-grant readback is unchanged; an empty delta passes. Further cases cover
test-network write revocation, broader public reads/writes, scoped reader
stanzas, malformed/duplicate stanzas, every existing inventory mismatch class,
unsettled generations, corrupt/missing/unreadable artifacts, renderer changes,
and current-file tampering. The existing strict-baseline and auto-rebaseline
suites also remain green. No live `/readyz` behavior is claimed.

## Deliberately left out

No deployment, service restart/stop, VPS access, live database/config/secret
access, migration, or baseline rewrite was performed. No agents were spawned.
The Docker Hub CI-flake stretch item remains a separate lane.

## Open questions and remaining work

- Review and merge this PR; rollout remains an operator action outside this lane.
- The supplied brief reports that PR #97's timer is merged but not deployed.
  That production status was not independently checked. Once this fix is
  deployed, the timer is not needed solely to repair public-feed-only readiness
  drift. Whether to deploy it for its existing broader rebaseline behavior is
  Ben's call; this change neither installs it nor changes its genuine-mismatch
  behavior.
- At rollout, the baseline generation's applied ACL artifact must still exist.
  Missing evidence intentionally fails closed; this lane does not reconstruct
  historical ACLs or automatically rebaseline.
