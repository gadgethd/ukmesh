# Handoff — Mission B2 · MQTT Will fake telemetry (#332)

## Status

Done. Offline Will payloads are skipped for status samples, ordinary telemetry stores its top-level status, and the schema migration is included but unapplied.

## Branch and commits

- Branch: `fix/mqtt-will-telemetry`
- Base: `d44ef5e` (`origin/main`)
- Implementation: `ad473779ce0f1ff250f6a9494d8abe9abba7a9f1` — `fix(mqtt): skip offline status telemetry samples`
- Push state: not pushed; coordinator relays this branch.

## Files changed

- `backend/src/mqtt/client.ts`
- `backend/src/mqtt/statusTelemetry.ts`
- `backend/src/mqtt/statusTelemetry.test.ts`
- `backend/src/db/index.ts`
- `backend/src/db/schema/base.sql`
- `backend/src/db/migrations/052_node_status_sample_status.sql`

## Checks

Commands were run from `backend/`:

- `node --import tsx --test src/mqtt/statusTelemetry.test.ts` — passed, 2 tests.
- `npm test` — passed, 326 tests, 0 failures. The script excludes `*.integration.test.ts`.
- `npm run typecheck` — passed.

## Gated items for Ben

- Apply migration `052_node_status_sample_status.sql` through the normal deployment process, then deploy the ingest change. No live DB writes, migration application, or service restarts were performed in this worktree session.
- Historical rows and retained broker messages were not modified; clearing retained messages is out of scope for #332.

## Open questions

None.

## Evidence paths

- `BRIEF.md` — mission and constraints.
- `backend/src/mqtt/statusTelemetry.test.ts` — offline Will skip, ordinary sample storage, and top-level status coverage.
- `backend/src/db/migrations/052_node_status_sample_status.sql` — additive status column migration.
