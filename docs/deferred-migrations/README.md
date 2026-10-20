# Deferred migrations

Migrations in this directory are **not** part of the applied migration chain and are not copied into
the runtime image's migration directory. They are kept here so contributor work is not lost while
the live deployment remains the schema of record.

## `057_node_status_sample_status.sql`

- Source: `main` (PR #114, re-landed 2026-10-03).
- Content: `ALTER TABLE node_status_samples ADD COLUMN IF NOT EXISTS status TEXT;`
- Why deferred: the live database (60-row ledger) does **not** have this migration applied and the
  `node_status_samples.status` column does not exist. Including it at 057 would have collided with
  `057_region_scope_participation.sql` and changed the schema on the next deploy.
- Re-introduction: as a new migration numbered after 060, with explicit approval, in Stage 2.
