# Shared definitions

These files own definitions consumed by more than one runtime. They do not
replace the backend HTTP contracts or OpenAPI schema.

- `public-data.json` contains public channel labels/slugs and stats section
  membership. Crypto keys and confidential runtime channels stay in the backend.
- `link-v3.json` owns shared Redis keys, admission key order and limit metadata.
  `link-v3-admit.lua` owns admission transitions and the positional argument and
  return protocol. Producers retain argument packing, payload serialization,
  identity generation, configuration parsing and heartbeat policy.
- `initial-state-columns.ts` owns the optional `columns-v1` bootstrap transport.
  Its encoder accepts trusted PostgreSQL DTO records with ordinary data properties,
  native Dates and parsed JSON, without accessors or custom JSON hooks. The final
  wire stringify preserves JSON value semantics. Its decoder validates parsed wire
  input and restores the existing row envelopes before application handlers run.
  Database scope, privacy, freshness, admission and byte limits remain server-owned.

Run `node scripts/generate-shared-contracts.mjs` after editing these sources.
Commit the generated package-local adapters with the source change. CI checks
their exact contents with `--check`; ordinary package/container builds need no
repository-root lookup, additional build context or generator at runtime.
