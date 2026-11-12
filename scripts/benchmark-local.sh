#!/usr/bin/env bash
# Existing prepared snapshot only. No reset, migration, replay or remote targets.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOCAL_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOCAL_ENV_CLEAN=1 \
    PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-}" \
    bash "$repo/scripts/benchmark-local.sh" "$@"
fi
label=${1:-baseline}
[[ $label =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo 'Use a lowercase measurement label' >&2; exit 2; }
cd "$repo"
output="$repo/plans/measurements/$label"
[[ ! -e $output ]] || { echo "Evidence already exists: $output" >&2; exit 1; }
[[ $(cat plans/dev-stack/fixture-ready) == snapshot ]] || { echo 'Prepare the snapshot profile first' >&2; exit 1; }
exec 9>plans/measurements/benchmark.lock
flock -n 9 || { echo 'Another local benchmark is running' >&2; exit 1; }
mkdir -p "$output"
export DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore
export UKMESH_DEV_API_TARGET=http://127.0.0.1:3100
export VITE_BUILD_TARGET=public-website VITE_APP_HOSTNAME=app.ukmesh.com VITE_SITE=ukmesh
docker inspect ukmesh-sample-db ukmesh-sample-redis ukmesh-sample-mqtt \
  --format '{{.Name}} {{.Image}} {{json .HostConfig.Memory}} {{json .HostConfig.NanoCpus}}' > "$output/images.txt"
data_dir=$(cat plans/dev-stack/data-dir)
sha256sum "$data_dir"/*.csv.gz > "$output/dataset.sha256"
cp plans/dev-stack/prepare.log "$output/fixture-prepare.log"
cp plans/dev-stack/rollups.log "$output/fixture-rollups.log"
if [[ -f plans/dev-stack/regions.log ]]; then
  # Keep node/prefix diagnostics out of the measurement index.
  tail -1 plans/dev-stack/regions.log > "$output/fixture-regions.json"
fi
git status --porcelain > "$output/git-status.txt"
git rev-parse HEAD > "$output/commit.txt"
curl --fail --silent --max-time 20 http://127.0.0.1:9091/metrics > "$output/metrics-before.txt"
npm --prefix backend run bench:api -- --output "$output/api" > "$output/api.log" 2>&1
(
  cd frontend
  /usr/bin/time -v npm run build > "$output/build.log" 2>&1
  npm run budget > "$output/budget.log" 2>&1
  node node_modules/vite/bin/vite.js preview --host 127.0.0.1 --port 4173 --strictPort \
    > "$output/preview.log" 2>&1 &
  preview_pid=$!
  trap 'kill "$preview_pid" 2>/dev/null || true' EXIT
  for ((attempt=0; attempt<60; attempt++)); do
    if curl --fail --silent http://127.0.0.1:4173/ > /dev/null; then break; fi
    sleep 1
  done
  npm run bench:website -- --output "$output/website" --runs 3 > "$output/website.log" 2>&1
)
curl --fail --silent --max-time 20 http://127.0.0.1:9091/metrics > "$output/metrics-after.txt"
echo "Sequential API/build/Lighthouse evidence: $output"
