#!/usr/bin/env bash
# Exercise startup ordering and fixture preservation without Docker or a server.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d)
trap 'rm -rf -- "$test_root"' EXIT

init_fixture() {
  local name=$1
  fixture="$test_root/$name with spaces"
  mkdir -p "$fixture/bin" "$fixture/repo/scripts" "$fixture/repo/backend/node_modules" \
    "$fixture/repo/frontend/node_modules" "$fixture/repo/plans/dev-stack" "$fixture/data"
  cp "$repo/scripts/dev-stack.sh" "$fixture/repo/scripts/"
  touch "$fixture/repo/scripts/bootstrap-backend-roles.sql" "$fixture/repo/scripts/bootstrap-reader-roles.sql"
  printf 'snapshot\n' > "$fixture/repo/plans/dev-stack/fixture-ready"
  printf '%s\n' "$fixture/data" > "$fixture/repo/plans/dev-stack/data-dir"
  printf 'preserve this fixture\n' > "$fixture/data/marker"
  printf 'DOTENV_SENTINEL=must-not-be-read\n' > "$fixture/repo/.env"
  : > "$fixture/calls"
  cat > "$fixture/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
[[ $1 != info ]] || exit 0
[[ $1 == compose && $2 == -f ]] || exit 90
case $4 in
  up) printf 'INFRA_UP\n' >> "$root/calls" ;;
  ps) ;;
  exec) cat >/dev/null; printf 'BOOTSTRAP_ROLE\n' >> "$root/calls" ;;
  *) echo 'Unexpected infrastructure mutation' >&2; exit 91 ;;
esac
MOCK
  cat > "$fixture/bin/node" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
for variable in DATABASE_URL MESHCORE_HTTP_DATABASE_URL MESHCORE_INGEST_DATABASE_URL MESHCORE_WORKER_DATABASE_URL MESHCORE_MIGRATE_DATABASE_URL; do
  [[ ${!variable} == postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore ]] || exit 92
done
[[ ${OWNER_DATABASE_URL:-} == postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore_owner_auth ]] || exit 93
[[ ! ${DOTENV_SENTINEL+x} && ! ${UNTRUSTED_INHERITED_SETTING+x} ]] || exit 94
case "$*" in
  *backend/src/tools/migrate.ts)
    printf 'MIGRATE %s\n' "${MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL:-unset}" >> "$root/calls"
    [[ ! -f $root/fail-migration ]] || { echo 'fixture migration failed'; exit 42; } ;;
  *backend/dist/index.js) printf 'BACKEND\n' >> "$root/calls" ;;
  *backend/dist/workers/region-snapshot.js) printf 'REGION\n' >> "$root/calls" ;;
  *frontend/node_modules/vite/bin/vite.js*) printf 'FRONTEND\n' >> "$root/calls" ;;
  *) echo 'Unexpected fixture load or replay' >&2; exit 95 ;;
esac
MOCK
  cat > "$fixture/bin/npm" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
[[ $* == *' run build' ]] || exit 96
printf 'BUILD\n' >> "$root/calls"
MOCK
  cat > "$fixture/bin/curl" <<'MOCK'
#!/usr/bin/env bash
printf 'meshcore_region_snapshot_healthy 1\n'
MOCK
  chmod +x "$fixture/bin/"*
}

run_fixture() {
  # The script's env -i must discard both external settings and remote DSNs.
  env PATH="$fixture/bin:$PATH" UNTRUSTED_INHERITED_SETTING=discard \
    DATABASE_URL=postgresql://untrusted.invalid/not-local \
    bash "$fixture/repo/scripts/dev-stack.sh" "$1" > "$fixture/output" 2>&1
}

init_fixture restart
run_fixture start
for ((attempt=0; attempt<100; attempt++)); do
  grep -qx FRONTEND "$fixture/calls" && break
  sleep 0.01
done
node --input-type=module - "$fixture" <<'JS'
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
const root = process.argv[2], read = name => readFileSync(join(root, name), 'utf8');
const calls = read('calls').trim().split('\n');
assert.equal(calls.filter(line => line === 'MIGRATE unset').length, 1);
assert.ok(calls.indexOf('MIGRATE unset') < calls.indexOf('BUILD'));
assert.ok(calls.indexOf('BUILD') < calls.indexOf('BACKEND'));
for (const stage of ['BACKEND', 'REGION', 'FRONTEND']) assert.ok(calls.includes(stage));
assert.ok(!calls.some(line => line.startsWith('BOOTSTRAP')));
assert.equal(read('data/marker'), 'preserve this fixture\n');
assert.equal(read('repo/plans/dev-stack/fixture-ready'), 'snapshot\n');
JS

init_fixture failed-restart
touch "$fixture/fail-migration"
if run_fixture start; then echo 'Migration failure did not stop startup' >&2; exit 1; fi
grep -qx 'MIGRATE unset' "$fixture/calls"
! grep -Eq 'BUILD|BACKEND|REGION|FRONTEND|BOOTSTRAP' "$fixture/calls"
grep -q 'fixture migration failed' "$fixture/output"
test -f "$fixture/data/marker"
test -f "$fixture/repo/plans/dev-stack/fixture-ready"

init_fixture fresh-bootstrap
rm "$fixture/repo/plans/dev-stack/fixture-ready"
touch "$fixture/fail-migration"
if run_fixture up; then echo 'Migration failure did not stop fresh startup' >&2; exit 1; fi
test "$(grep -c '^BOOTSTRAP_ROLE$' "$fixture/calls")" -eq 2
grep -qx 'MIGRATE bootstrap-ledger-from-base-schema' "$fixture/calls"
! grep -Eq 'BUILD|BACKEND|REGION|FRONTEND' "$fixture/calls"
echo 'DEV startup migration, fail-closed startup and fresh-bootstrap contracts passed.'
