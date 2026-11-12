# Contributing

## Structural expectations

- routes should stay thin
- services own orchestration
- repositories own SQL
- worker RF math should stay separate from queue orchestration
- frontend map builders should stay separate from `MapLibreMap.tsx`
- reusable frontend models and parsers belong to their feature, below pages and
  renderers; hooks must not import page implementations
- process entrypoints compose services and own their start/close lifecycle;
  registering HTTP routes must not install a hidden service registry

## Shared definitions

Public channel labels/slugs, stats section membership and the shared link-v3
admission protocol live in [`contracts/`](../contracts/README.md). Generate local
bindings with `node scripts/generate-shared-contracts.mjs` from the repository
root, and commit the source and generated outputs together. Check drift without
rewriting files with `node scripts/generate-shared-contracts.mjs --check`.

Package and container builds consume their checked-in local adapters. Keep
channel keys and confidential runtime channels in the backend; browser metadata
must contain public names only. A shared queue definition does not make producer
serialization, identity generation, configuration parsing or heartbeat policies
interchangeable.

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
- no migration may assume a relation that no migration creates. A fresh
  database replays the whole band from `base.sql`, so an object that only exists
  on older deployments must be guarded (`IF to_regclass('public.x') IS NOT
  NULL`) with a `NOTICE` when the guard skips, and both paths need a test: the
  band must reach head on a base-schema-only database, and the guarded statement
  must still run unchanged where the object is present.
- whole-history or destructive fixes go in explicit maintenance scripts, never
  migrations or startup
- apply pending migrations with `docker compose run --rm db-migrate`, not by
  running individual migration files or inserting migration-ledger rows by hand

## Testing expectations

Before finishing a refactor or behavior change:
- use Node 26 (see `.nvmrc`, matching the container runtime) and run `cd backend && npm ci && npm run typecheck && npm test && npm run build`
- run `cd frontend && npm ci && npm test && npm run build && npm run test:e2e`
- if worker code changed, build its image and run its tests there so GDAL and
  `osgeo` exactly match production
- run `docker compose config --quiet`
- rebuild affected containers and check `http://localhost:3000/healthz`

For worker protocol changes, `scripts/test-worker-fixtures.sh` provisions owned
PostgreSQL and Redis fixtures, runs the real GDAL image suite and RF budgets,
then removes those temporary services. Build the image first and set
`RF_WORKER_TEST_IMAGE` to its local tag. `scripts/test-link-queue-contract.sh`
checks TypeScript/Python producer-consumer behavior against owned Redis using
the configured local backend and worker images. These commands do not read the
project `.env` or use the retained sample database.

For public-page lifecycle changes, run `npm run test:browser-contracts` from
`frontend/`. It builds an isolated production preview, starts an owned loopback
server and runs the owner, topology and regions browser contracts with zero
skips. The Playwright development and regions lanes also exercise interrupted
renders, scope changes and late socket callbacks.
