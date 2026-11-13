#!/usr/bin/env bash
# Resets ONLY a named owned ingest fixture, never the normal DEV stack.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOOKUP_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOOKUP_ENV_CLEAN=1 bash "$0" "$@"
fi
cd "$repo"
source "$repo/scripts/local-sample/lookup-profile.sh"
kind=${1:-}; label=${2:-}; data_dir=${3:-}
[[ $kind == baseline || $kind == candidate ]] || exit 2
[[ $label =~ ^[a-z0-9][a-z0-9-]*$ && -f $data_dir/fixture.json ]] || { echo 'Usage: benchmark-lookup-ingest.sh baseline|candidate label synthetic-directory [speed]' >&2; exit 2; }
speed=${4:-0}
[[ $speed =~ ^[0-9]+$ ]] || exit 2
state="$repo/plans/database-lookups"
output="$state/$label"
[[ ! -e $output ]] || { echo 'Evidence already exists.' >&2; exit 1; }
mkdir -p "$output"
bash scripts/lookup-stack-local.sh stop
bash scripts/lookup-stack-local.sh services
docker exec "$LOOKUP_REDIS_CONTAINER" redis-cli FLUSHALL > "$output/redis-reset.log"
docker restart "$LOOKUP_MQTT_CONTAINER" > "$output/broker-reset.log"
name="lookup_ingest_$kind"
docker exec "$LOOKUP_DB_CONTAINER" psql -X -U meshcore -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS $name WITH (FORCE)" > "$output/drop.log"
docker exec "$LOOKUP_DB_CONTAINER" createdb -U meshcore "$name"
docker exec -i "$LOOKUP_DB_CONTAINER" psql -X -U meshcore -d "$name" -v ON_ERROR_STOP=1 \
  < backend/src/db/schema/base.sql > "$output/base.log"
source_root="$repo/backend"
source_ref="$(git rev-parse HEAD)+$(git diff HEAD | sha256sum | cut -d ' ' -f 1)"
if [[ $kind == baseline ]]; then
  source_root="$state/baseline-source/backend"
  source_ref=$LOOKUP_BASELINE_REF
fi
export DATABASE_URL="$(lookup_database_url "$name")"
tsx=(node --import "$repo/backend/node_modules/tsx/dist/loader.mjs")
MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL=bootstrap-ledger-from-base-schema \
  "${tsx[@]}" "$source_root/src/tools/migrate.ts" > "$output/migrate.log" 2>&1
docker exec "$LOOKUP_DB_CONTAINER" psql -X -U meshcore -d "$name" -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' > "$output/extensions.log"
"${tsx[@]}" backend/src/tools/loadSampleDataset.ts --data-dir "$data_dir" --omit-packets --fast > "$output/load.log" 2>&1
"${tsx[@]}" backend/src/tools/prepareSampleDataset.ts > "$output/prepare.log" 2>&1
npm --prefix "$source_root" run build > "$output/build.log" 2>&1
trap 'bash scripts/lookup-stack-local.sh stop' EXIT
bash scripts/lookup-stack-local.sh start "$name" "$kind" > "$output/start.log" 2>&1
for ((attempt=0; attempt<60; attempt++)); do
  if curl --fail --silent --max-time 2 "$LOOKUP_METRICS_URL" | rg -q '^meshcore_mqtt_delivery_probe_healthy 1$'; then break; fi
  [[ $attempt != 59 ]] || { echo 'Owned ingest delivery probe did not become healthy' >&2; exit 1; }
  sleep 1
done
sleep 10
sha256sum "$data_dir"/*.csv.gz > "$output/source.sha256"
docker exec "$LOOKUP_DB_CONTAINER" cat /sys/fs/cgroup/cpu.stat > "$output/cpu-before.txt"
"${tsx[@]}" backend/src/tools/benchmarkIngest.ts --data-dir "$data_dir" --limit 2000 --speed "$speed" \
  --broker "$LOOKUP_BROKER_URL" --metrics-url "$LOOKUP_METRICS_URL" \
  --server-source-ref "$source_ref" --output "$output" > "$output/ingest.log" 2>&1
docker exec "$LOOKUP_DB_CONTAINER" cat /sys/fs/cgroup/cpu.stat > "$output/cpu-after.txt"
echo "Owned ingest comparison: $output"
