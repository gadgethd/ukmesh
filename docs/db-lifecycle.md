# DB Lifecycle

## Rules

- `backend/src/db/schema/base.sql`
  - only cheap, idempotent base schema work
- `backend/src/db/migrations.ts`
  - runs additive versioned migrations
- historical backfills must not run on backend startup

## Use the right layer

### Base schema
Use for:
- table creation
- index creation
- safe `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`

Do not use for:
- whole-table `UPDATE` backfills
- historical recomputation
- data repair

### Migrations
Use for:
- additive schema changes that need one-time application
- constraints or indexes that belong to a versioned rollout

### Backfills / maintenance jobs
Use for:
- recomputing derived packet fields
- rebuilding link tables
- recalculating historical summaries

## Startup guarantee

Backend startup should be safe against a production-sized database. If a change can lock or scan large tables, it does not belong in startup schema init.
