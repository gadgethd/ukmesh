# Contributing

## Structural expectations

- routes should stay thin
- services own orchestration
- repositories own SQL
- worker RF math should stay separate from queue orchestration
- frontend map builders should stay separate from `MapLibreMap.tsx`

## Repository hygiene

Do not commit:
- secrets
- credentials
- private database dumps
- personal or personally identifiable information
- operational traces containing sensitive infrastructure details
- artifacts that could be used for abuse or malicious access

If in doubt, keep it out of Git and add it to `.gitignore`.

## DB changes

- `backend/src/db/schema/base.sql` represents a fresh database. Any schema
  change required by an existing deployment must be added as a new, ordered
  migration as well; do not edit a migration that may already be recorded.
- whole-history or destructive fixes go in explicit maintenance scripts, never
  migrations or startup
- apply pending migrations with `docker compose run --rm db-migrate`, not by
  running individual migration files or inserting migration-ledger rows by hand

## Testing expectations

Before finishing a refactor or behavior change:
- use Node 20 and run `cd backend && npm ci && npm run typecheck && npm test && npm run build`
- run `cd frontend && npm ci && npm run build`
- if worker code changed, run `python3 -m py_compile viewshed-worker/worker.py viewshed-worker/backfill_profiles.py viewshed-worker/rf/*.py`
- run `docker compose config --quiet`
- rebuild affected containers and check `http://localhost:3000/healthz`
