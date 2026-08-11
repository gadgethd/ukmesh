# GitHub cleanup status

Status: **blocked before push** (2026-08-03).

## Completed

- Consulted shared project memory and the tokensave code index before repository inspection.
- Built `cleanup/main-rewrite` from `origin/main` without the three RF planning commits or `IMPLEMENTATION-REPORT.md`.
- Prepared 13 clean commits: six main feature commits, six logical pathing commits, and the `.tokensave/` ignore rule. The branch is at `bca8219` and is 13 commits ahead of `origin/main` (`3c631e8`). It has not been renamed to `main` or pushed.
- Backend gate passed: `npm install`, `npm run build`, and `npm test` (249 passed, 0 failed).

## Blocking failure

The required frontend gate stopped at `cd frontend && npm install`:

```text
npm ERR! EACCES: permission denied, mkdir
/home/ben/meshcore-analytics/frontend/node_modules/playwright/node_modules/fsevents
```

The nested Playwright directories are owned by `root:root`, while the checkout is being used by `ben`. The VM is also running Node `v18.19.1` / npm `9.2.0`; the install reported dependencies requiring Node 22 or newer. Frontend build and tests were not run. Required next action: repair the dependency-directory ownership/install environment and use the repository-supported Node version, then rerun `npm install`, `npm run build`, and `npm test`. No ownership or dependency cleanup was guessed or performed.

## PII/secret scan

- Final prepared push diff (`origin/main..cleanup/main-rewrite`): no email addresses, `/home` or `/Users` paths, private IPs, credential-shaped tokens, API keys, or passwords found.
- The original nine-commit main range contained the VM address in the RF planning/review docs. Those docs, plus `IMPLEMENTATION-REPORT.md`, were excluded from the prepared history and remain untracked locally.
- The pathing integration diff contained no new PII/secret matches. Its five wave summary/baseline docs were excluded from the prepared history and remain untracked locally.
- The in-flight `wip/iata-upgrade-integration` snapshot commit contains operational private-network addresses and `/home/ben/meshcore-releases` paths in its unique patch; that branch was not pushed because the scan found those matches.
- The tracked baseline tree still contains pre-existing test/Compose/nginx private-network fixtures and deployment paths, a third-party font-license email, and the documented MeshCore public-channel key. No credential-shaped secret was found; the public-channel key is explicitly documented as protocol material rather than an application credential. These matches are outside the prepared push diff and were not altered.

## Not performed because of the blocker

- Nothing was pushed; `origin/main` is unchanged.
- No local or remote branches were deleted. The in-flight branches were not pushed.
- No worktrees were removed or pruned.
- The cleanup branch has not been renamed or fast-forwarded to `main`.

Local-only planning, summary, baseline, review, and implementation-report files are retained as untracked files in this VM checkout. This summary is also local-only and must not be committed.
