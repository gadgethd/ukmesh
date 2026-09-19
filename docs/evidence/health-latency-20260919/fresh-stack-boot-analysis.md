# Fresh-stack boot failure — analysis (Hermes watch, 2026-09-19)

**Symptom (CI).** Run 35440275286, job "Workers and Compose": the backend container entered repeated Error states ~2s after start and never served; `up --wait` failed with `dependency failed to start: container meshcore-ci-35440275286-1-backend-1 is unhealthy` after ~2 min. (This stage was only reachable after the two earlier inherited gaps — build list, Dockerfile — see `smoke-failure-analysis.md`.)

**Reproduction (VPS, throwaway stack).** Fresh timescaledb (base.sql as initdb + empty volume), redis, `dist/tools/migrate.js`, then the backend image built from the branch head, with CI-shaped env (`ci-test` credentials, shadow owner auth).

*Before the fix* — the process dies at module load, before any server starts:

```
Error: contracts without routes: GET /owner/packet-sharing, POST /owner/packet-sharing
    at assertContractCoverage (file:///app/dist/api/contracts.js:335:15)
    at file:///app/dist/api/routes.js:213:1
```

*After the fix* — the full boot chain completes:

```
[db] base schema initialised, migrations applied: 001_ml_tables.sql ... 051_reset_readiness.sql
[node-identity] refreshed { ... }
[owner-auth] created database meshcore_owner_auth
[owner-auth] schema initialised
[app] listening on http://0.0.0.0:3000
```

**Root cause.** Commit 1d86e04 (tags) swept in `backend/src/api/contracts.ts` entries for `/owner/packet-sharing` (response schema + GET/POST contract blocks + list registrations) whose route handlers were never committed — they exist only in the main checkout's uncommitted packet-share WIP. `assertContractCoverage` is a module-level assert at import time in `routes.ts`, so the backend process is killed before it can listen on **every** fresh stack. GitHub `main` (Aug 31 content) contains no packet-sharing references at all, and these commits had never been pushed before today — so the CI smoke stack had never attempted this code path.

**Fix.** Drop the stray contract entries (`backend/src/api/contracts.ts`, −83 lines) and regenerate `docs/openapi.yaml` (−403 lines). `npm run contract:check` is green (64 API + 11 operator routes, down from 66). The packet-sharing feature stays on its own workstream; its contracts return when its routes land.

**Deploy-day note.** The branch's backend image now boots clean on an empty-volume stack; the smoke step should proceed past the backend health gate.
