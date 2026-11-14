#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ ${UKMESH_BEACON_ACCESS_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_BEACON_ACCESS_CLEAN=1 bash "$repo/scripts/companion-dev/beacon-access.sh" "$@"
fi
[[ $# == 1 ]] || { echo 'usage: scripts/companion-dev/beacon-access.sh DATABASE' >&2; exit 2; }
database=$1
[[ $database == beacon_dev || $database =~ ^lean_beacon_[a-z0-9_]+$ ]] || { echo 'Unowned Beacon database' >&2; exit 2; }
docker=(docker --host unix:///var/run/docker.sock)
container=ukmesh-companions-beacon-db-1
project=$("${docker[@]}" inspect "$container" --format '{{index .Config.Labels "com.docker.compose.project"}}')
[[ $project == ukmesh-companions ]] || { echo 'Beacon database container ownership differs' >&2; exit 1; }
if [[ $database == lean_* ]]; then
  marker=$("${docker[@]}" exec "$container" psql -X -U beacon_dev -d postgres -At -v ON_ERROR_STOP=1 \
    -c "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname='$database'")
  [[ $marker == "ukmesh-owned-fixture:${database#lean_beacon_}" ]] || { echo 'Beacon fixture ownership marker differs' >&2; exit 1; }
fi
"${docker[@]}" exec -i "$container" psql -X -U beacon_dev -d "$database" -v ON_ERROR_STOP=1 -v "beacon_database=$database" \
  < "$repo/third_party/beacon-server/deploy/ukmesh-dev/access.sql"
