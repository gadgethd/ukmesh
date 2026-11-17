#!/usr/bin/env bash
# Operator-only provisioning for the local sample cluster and owned databases.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ ${UKMESH_LOCAL_ACCESS_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOCAL_ACCESS_CLEAN=1 bash "$repo/scripts/full-dev/access.sh" "$@"
fi
[[ $# == 2 ]] || { echo 'usage: scripts/full-dev/access.sh UKMESH_DATABASE OWNER_DATABASE' >&2; exit 2; }
database=$1 owner_database=$2
fixture_token=
if [[ $database =~ ^lean_(fixture|1x|10x|soak)_([a-z0-9_]{1,38})$ ]]; then
  fixture_token=${BASH_REMATCH[2]}
  [[ $owner_database == "lean_owner_$fixture_token" ]] || { echo 'Fixture database targets must share one owner' >&2; exit 2; }
else
  [[ $database == meshcore && $owner_database == meshcore_owner_auth ]] || { echo 'Unowned database targets' >&2; exit 2; }
fi
[[ $database != "$owner_database" ]] || { echo 'Main and owner stores must be separate' >&2; exit 2; }
docker=(docker --host unix:///var/run/docker.sock)
project=$("${docker[@]}" inspect ukmesh-sample-db --format '{{index .Config.Labels "com.docker.compose.project"}}')
[[ $project == ukmesh-sample ]] || { echo 'Local sample database ownership not proved' >&2; exit 1; }
for target in "$database" "$owner_database"; do
  if [[ $target == lean_* ]]; then
    marker=$("${docker[@]}" exec ukmesh-sample-db psql -U meshcore -d postgres -At -v ON_ERROR_STOP=1 \
      -c "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname='$target'")
    [[ $marker == "ukmesh-owned-fixture:$fixture_token" ]] || { echo 'Fixture database ownership marker differs' >&2; exit 1; }
  fi
done
"${docker[@]}" exec -i ukmesh-sample-db psql -U meshcore -d "$database" \
  -v ON_ERROR_STOP=1 -v "ukmesh_database=$database" -v "owner_database=$owner_database" \
  < "$repo/scripts/full-dev/access.sql"
if [[ -n $fixture_token ]]; then
  "${docker[@]}" exec -i ukmesh-sample-db psql -X -U meshcore -d "$database" -v ON_ERROR_STOP=1 \
    < "$repo/scripts/full-dev/corescope-reader.sql"
fi
