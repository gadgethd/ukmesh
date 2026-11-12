# Shared definitions

These files own definitions consumed by more than one runtime. They do not
replace the backend HTTP contracts or OpenAPI schema.

- `public-data.json` contains public channel labels/slugs and stats section
  membership. Crypto keys and confidential runtime channels stay in the backend.
- `link-v3.json` and `link-v3-admit.lua` own shared Redis keys, admission bindings,
  Lua and limit metadata. Producers retain their own payload serialization,
  identity generation, configuration parsing and heartbeat policy.

Run `node scripts/generate-shared-contracts.mjs` after editing these sources.
Commit the generated package-local adapters with the source change. CI checks
their exact contents with `--check`; ordinary package/container builds need no
repository-root lookup, additional build context or generator at runtime.
