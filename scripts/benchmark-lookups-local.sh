#!/usr/bin/env bash
# Isolated lookup qualification. Never reads .env or changes the normal DEV stack.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOOKUP_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOOKUP_ENV_CLEAN=1 bash "$0" "$@"
fi
cd "$repo"
state="$repo/plans/database-lookups"
mkdir -p "$state"
action=${1:-help}
kind=${2:-candidate}
baseline=5c780e66ae2f079f15cf0007c4979dabd22d0d50
tsx=(node --import "$repo/backend/node_modules/tsx/dist/loader.mjs")
db() { docker exec -i ukmesh-lookup-db psql -X -U meshcore -d "$1" -v ON_ERROR_STOP=1 -P pager=off; }
tool() {
  local name=$1 file=$2; shift 2
  DATABASE_URL="postgresql://meshcore:meshcore@127.0.0.1:5434/$name" "${tsx[@]}" "$file" "$@"
}
case "$action" in
  prepare)
    [[ ! -e "$state/prepared" ]] || { echo 'Lookup fixtures already prepared; use a new isolated checkout to rebuild.' >&2; exit 1; }
    [[ -d data/ukmesh-sample-2026-11-10 ]] || { echo 'The retained local archive is required.' >&2; exit 1; }
    ! docker inspect ukmesh-lookup-db >/dev/null 2>&1 || { echo 'Owned lookup container already exists; refusing to overwrite it.' >&2; exit 1; }
    mkdir -p "$state/baseline-source"
    git archive "$baseline" backend scripts | tar -x -C "$state/baseline-source"
    ln -s "$repo/backend/node_modules" "$state/baseline-source/backend/node_modules"
    docker run -d --name ukmesh-lookup-db --cpus 2 --memory 2g --shm-size 1g \
      -p 127.0.0.1:5434:5432 -e POSTGRES_USER=meshcore -e POSTGRES_PASSWORD=meshcore -e POSTGRES_DB=lookup_1x \
      -v "$repo/backend/src/db/schema/base.sql:/docker-entrypoint-initdb.d/010-base.sql:ro" \
      timescale/timescaledb@sha256:22e8a5ae7aef121d1537afe946dd7cc5deeeb63ab36ce19849d671bd3b663509 \
      postgres -c shared_preload_libraries=timescaledb,pg_stat_statements -c max_parallel_workers_per_gather=0 -c max_connections=100
    for ((attempt=0; attempt<120; attempt++)); do
      if docker exec ukmesh-lookup-db pg_isready -h 127.0.0.1 -U meshcore -d lookup_1x >/dev/null; then break; fi
      sleep 1
    done
    db lookup_1x < scripts/bootstrap-backend-roles.sql > "$state/roles.log"
    db lookup_1x < scripts/bootstrap-reader-roles.sql >> "$state/roles.log"
    MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL=bootstrap-ledger-from-base-schema \
      tool lookup_1x "$state/baseline-source/backend/src/tools/migrate.ts" > "$state/migrate-baseline.log" 2>&1
    db lookup_1x <<< 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' > "$state/extensions.log"
    tool lookup_1x backend/src/tools/loadSampleDataset.ts --data-dir data/ukmesh-sample-2026-11-10 --fast --redact-content \
      --shift-to "$(node -p 'new Date().toISOString()')" > "$state/load.log" 2>&1
    tool lookup_1x backend/src/tools/prepareSampleDataset.ts > "$state/prepare-1x.log" 2>&1
    docker exec ukmesh-lookup-db createdb -U meshcore -T lookup_1x lookup_10x
    tool lookup_10x backend/src/tools/scaleLookupFixture.ts --owned-fixture > "$state/scale.log" 2>&1
    for name in lookup_1x lookup_10x; do
      tool "$name" backend/src/tools/backfillStatsRollups.ts --apply --pause-ms 1 > "$state/$name-rollups.log" 2>&1
      tool "$name" backend/src/tools/rebuildRegionParticipation.ts > "$state/$name-regions.log" 2>&1
      db "$name" <<< 'VACUUM (ANALYZE)' > "$state/$name-vacuum.log"
    done
    sha256sum data/ukmesh-sample-2026-11-10/*.csv.gz > "$state/source.sha256"
    docker inspect ukmesh-lookup-db --format '{{.Image}} {{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}' > "$state/resources.txt"
    date -u +%FT%TZ > "$state/prepared"
    ;;
  measure)
    [[ $kind == baseline || $kind == candidate ]] || { echo 'Use measure baseline|candidate [label]' >&2; exit 2; }
    label=${3:-$(date -u +%Y%m%dT%H%M%S)}
    [[ $label =~ ^[a-zA-Z0-9_-]+$ ]] || exit 2
    directory="$state/$kind-$label"
    [[ ! -e $directory ]] || { echo 'Evidence directory already exists.' >&2; exit 1; }
    mkdir -p "$directory"
    source_root="$repo/backend/src"
    if [[ $kind == baseline ]]; then source_root="$state/baseline-source/backend/src"; fi
    for scale in 1x 10x; do
      name="lookup_$scale"
      budget=100
      [[ $scale == 1x ]] || budget=250
      budget_args=()
      if [[ $kind == candidate ]]; then budget_args=(--budget-ms "$budget"); fi
      if [[ $kind == candidate ]]; then tool "$name" backend/src/tools/migrate.ts > "$directory/$scale-migrate.log" 2>&1; fi
      docker exec ukmesh-lookup-db cat /sys/fs/cgroup/cpu.stat > "$directory/$scale-cpu-before.txt"
      tool "$name" backend/src/tools/benchmarkLookups.ts --source-root "$source_root" --samples 200 --trials 3 --concurrency 8 \
        --cache miss --plans "${budget_args[@]}" --output "$directory/$scale-c8" > "$directory/$scale-c8.log" 2>&1
      docker exec ukmesh-lookup-db cat /sys/fs/cgroup/cpu.stat > "$directory/$scale-cpu-after.txt"
      tool "$name" backend/src/tools/benchmarkLookups.ts --source-root "$source_root" --samples 20 --trials 3 --concurrency 1 \
        --cache miss --output "$directory/$scale-c1" > "$directory/$scale-c1.log" 2>&1
      tool "$name" backend/src/tools/benchmarkLookups.ts --source-root "$source_root" --samples 200 --trials 3 --concurrency 8 \
        --cache warm --cases node-links --output "$directory/$scale-warm" > "$directory/$scale-warm.log" 2>&1
    done
    echo "Lookup qualification evidence: $directory"
    ;;
  *) echo 'Usage: scripts/benchmark-lookups-local.sh prepare | measure baseline|candidate [label]' ;;
esac
