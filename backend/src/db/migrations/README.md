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
