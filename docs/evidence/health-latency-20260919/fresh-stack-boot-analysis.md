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

## Update — run 4 (contracts fix in): final inherited gap

Run 4 (`45af592a`) brought the full stack up — backend Healthy, every service healthy except `alert-receiver`, which `up --wait` aborted on (12:06Z).

**Cause:** `a8921b8` also flipped alert-receiver's compose healthcheck from `/healthz` to `/readyz` (undocumented in that commit, whose message covers only the region-probe list). The worker's `/readyz` deliberately returns **503 in archive-only mode** (`ALERT_FORWARD_URL` unset — exactly the CI configuration), and the worker source documents `/healthz` as the compose check ("/healthz always 200 so the compose healthcheck (wget -qO-) never restarts the container for degraded delivery"). Consequence: any deployment without alert forwarding configured reports permanently unhealthy.

**Fix:** revert the healthcheck to `/healthz` (compose only; the worker's liveness/readiness split is untouched and remains correct for operators and monitoring).

## Update — run 5: WS fanout failure -> message_tags migration

Run 5 (`ba46e141`) passed `up --wait` with every service healthy (alert-receiver fix confirmed) and died at the final assertion: "timed out waiting for MQTT packet WebSocket fanout" (12:22Z). The DB readback in the same block passed, so MQTT ingest and packet writes were fine.

**Cause:** the WS initial-state queries now JOIN `message_tags` (tags work, `1d86e04`), and `message_tags` was created only by `tagger_worker.py` startup DDL -- which sits after its `TYPESAFE_API_KEY` gate and exits 2 when the key is unset (CI's, and any fresh, configuration). No migration created the table. Every WS connect therefore failed in `fetchInitialState`, the server closed the client (`1013 initial state is temporarily unavailable`), and the fanout check timed out.

**Fix:** `055_message_tags.sql` -- tagger schema (message_tags, tagger_state, tagger_retry) now lives in the migration ledger (idempotent; no-op where the worker already created the tables; 052-054 left free for the packet-share workstream). Verified on a fresh stack: migrations apply through `055_message_tags.sql` and the feed/WS queries execute without the missing-relation error.
