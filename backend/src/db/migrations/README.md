# Migrations

Place additive, versioned SQL migrations here. Do not put whole-table historical backfills in this folder.

The immutable `016_private_prefixes.sql` has a PostgreSQL ambiguity in its
historical rollup query. Environments that already applied it retain and verify
its checksum. A completely empty database records an explicit
`superseded-empty` disposition and receives the schema-only replacement in
`026_private_visibility_schema.sql`. A non-empty database with 016 pending
fails closed unless it has the shipped sibling
`016_stale_mqtt_observer_cleanup.sql`, both reviewed replacements are present,
and the operator supplies the exact one-time approval:

```text
MIGRATION_016_PRIVATE_PREFIXES_APPROVAL=supersede-016-and-017-with-authoritative-privacy-and-026
```

That path records `superseded-existing` for the two historical rewrite files
and installs the prefix trigger/schema replacement in 026 before new code can
start. Authoritative public query predicates validate path framing and consult
the small current private-prefix table for both old and new packets, so no
compressed Timescale history is rewritten. The original all-row topic rewrite
is also intentionally not run; request filtering derives the legacy topic
prefix from `packets.topic` when the materialized prefix is empty. The runner
never silently skips the migration. The resulting disposition for both 016
files, 017, and 018 is recorded in `schema_migration_compatibility`.

## Post-move supersession

`074_prefix_visibility_row_fence.sql` ships on `main` and is merged into the
schema-split branch, where the privacy cluster has already moved: `087` moved
`public_visibility_state` to `ops`, `113` moved `private_node_prefixes` to
`core`, and `108`/`128` left read-only views at the old public names. On such a
database the historical file cannot execute (`ALTER TABLE` and `CREATE TRIGGER`
against a view both fail), while `134_reassert_prefix_visibility_fence.sql`
installs the same fence on the post-move relations.

The runner therefore records `superseded-post-move` for 074 — but only when the
catalog proves the move happened: `ops.public_visibility_state` and
`core.private_node_prefixes` are tables, and `public.public_visibility_state` /
`public.private_node_prefixes` exist as views. A pre-move database (the public
names are still the real tables) still runs 074 exactly as shipped, and a
half-moved database runs it too rather than assuming the fence is in place. If
the post-move shape is present but 134 is absent from the tree, the run fails
closed instead of skipping the fence.

Why it was skipped is stored in
`schema_migration_compatibility.evidence` (reason, observed relation kinds,
replacement), next to the `superseded-post-move` ledger row written in the same
transaction. `src/db/migrationSupersession.integration.test.ts` proves both
paths against a real database.
