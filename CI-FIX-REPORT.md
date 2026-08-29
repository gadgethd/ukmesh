# UKMesh CI Fix Report

Date: 2026-08-29

## Scope

This work fixes the pre-existing CI failures on `main` at commit
`9fc5c9989838e48b4a273a2894f7068dcb414e8b`. The local-only security branches
`fix/w3-ukmesh-mediums`, `fix/auth-security`, and `fix/shell-hardening` were not
checked out, modified, merged, or pushed.

Baseline evidence: GitHub Actions run
[`33247675751`](https://github.com/gadgethd/ukmesh/actions/runs/33247675751)
failed in the Backend and Workers and Compose jobs.

## Findings

### E2E failure

The failing step exited before Playwright discovered any tests:

```text
> playwright test --project=public-desktop --project=dashboard-desktop --project=dashboard-mobile
sh: 1: playwright: not found
```

Commit `1888c2863c5c4eebeb118875e3cb4fbf3a4d209b` moved E2E execution from the
Frontend job to the Workers and Compose job so the backend would be available.
The destination job did not run `npm ci` in `frontend`, so the package script
could not resolve the lockfile-declared `@playwright/test` executable. Running
`npx playwright install` downloaded browser assets but did not populate the
project's `node_modules/.bin` for the following step.

The move also introduced a masked lifecycle problem. The Compose smoke step
registered `trap cleanup EXIT`, which removed the uniquely named stack when that
shell step ended. The later E2E step therefore could not actually run against
the stack it was intended to test once the missing executable was fixed.

### Contract failure

The Backend job reported:

```text
docs/openapi.yaml is stale; run npm run contract:generate
```

`backend/src/api/contracts.ts` and the runtime router both include the public
`GET /api/nodes/top-adverts` route, but the tracked generated OpenAPI document
did not. Regeneration adds that one endpoint (81 generated lines) and makes the
contract check current at 63 API routes and 11 operator routes.

## Changes

- The Workers and Compose job now runs `npm ci` in `frontend` before installing
  Chromium, so E2E uses the exact locked Playwright dependency.
- The smoke step no longer destroys its Compose project before E2E.
- A dedicated `if: always()` step removes the uniquely named Compose project,
  volumes, and orphans after E2E, including when build, install, or tests fail.
- `docs/openapi.yaml` was regenerated from the authoritative API contracts.
- No dependencies were added or changed.

## Local verification

The following checks passed:

- `actionlint .github/workflows/ci.yml`
- Backend Node 20.20.2: `npm run contract:check` (63 API, 11 operator routes)
- Backend Node 20.20.2: `npm test` (320 passed)
- Backend Node 20.20.2: `npm run test:analysis-integration` (1 passed)
- Backend Node 20.20.2: `npm run test:ingest-integration` (2 passed)
- Backend Node 20.20.2: `npm run build`
- Frontend: `npm test` (97 passed)
- Frontend: `npm run build`
- Local Playwright executable check: version 1.62.1 from the installed lockfile

The integration tests used temporary, isolated TimescaleDB and Redis containers
matching the CI image digests and environment variables. Both containers were
removed after verification.

A full local Playwright run could not safely bind its hard-coded ports because
the host already had unrelated long-running listeners on ports 3000 and
4173-4175 (including the Codex app server). Those processes were left untouched.
The full E2E/live-Compose path is therefore verified by the clean, isolated
GitHub Actions runner after this fix is pushed.

## Acceptance

The fix is accepted when the new `main` CI run passes Backend, Frontend, Secret
scan, and Workers and Compose, including `Run E2E tests against live stack` and
all backend steps that were previously skipped behind `contract:check`.
