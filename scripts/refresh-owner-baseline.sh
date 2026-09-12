#!/usr/bin/env bash
#
# refresh-owner-baseline.sh
#
# Regenerate and validate the owner-authorization inventory baseline after an
# approved owner grant/config change.
#
# The running backend fails readiness closed when the configured baseline no
# longer matches the live database/ACL (counts, grants, generations). Use this
# script ONLY after reviewing and approving the grant change, then repoint the
# baseline and recreate the backend.
#
# Usage:
#   scripts/refresh-owner-baseline.sh                 # generate + validate only
#   scripts/refresh-owner-baseline.sh --apply         # also repoint .env (backup kept)
#   scripts/refresh-owner-baseline.sh --apply --recreate
#   scripts/refresh-owner-baseline.sh --help
#
# Overrides:
#   OWNER_BASELINE_CONTAINER   backend container name (default meshcore-analytics-backend-1)
#   OWNER_BASELINE_EXPORT_DIR  host directory for the new baseline
#                              (default: OWNER_AUTH_INVENTORY_HOST_DIR from .env)
#   OWNER_BASELINE_NETWORK     docker network (default: first network of the container)
#   OWNER_BASELINE_MOSQUITTO   mosquitto config dir (default: <repo>/mosquitto)

set -euo pipefail
umask 077

mesh_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
mesh_repo_dir="$(cd -- "$mesh_script_dir/.." && pwd -P)"
mesh_container="${OWNER_BASELINE_CONTAINER:-meshcore-analytics-backend-1}"
mesh_compose_files=(-f docker-compose.yml -f docker-compose.live.yml)
mesh_apply=false
mesh_recreate=false

usage() {
  sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --apply) mesh_apply=true ;;
    --recreate) mesh_recreate=true ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
  shift
done

for command in docker python3; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required command is unavailable: $command" >&2
    exit 69
  }
done

if ! docker inspect "$mesh_container" >/dev/null 2>&1; then
  echo "backend container not found: $mesh_container" >&2
  exit 66
fi
if ! docker inspect "$mesh_container" --format '{{.State.Running}}' | grep -q '^true$'; then
  echo "backend container is not running: $mesh_container" >&2
  exit 66
fi

mesh_env_file="$mesh_repo_dir/.env"
if [ "$mesh_apply" = true ] && [ ! -f "$mesh_env_file" ]; then
  echo ".env not found in $mesh_repo_dir (required for --apply)" >&2
  exit 66
fi
if [ "$mesh_recreate" = true ] \
  && { [ ! -f "$mesh_repo_dir/docker-compose.yml" ] || [ ! -f "$mesh_repo_dir/docker-compose.live.yml" ]; }; then
  echo "compose files not found in $mesh_repo_dir (required for --recreate)" >&2
  exit 66
fi

mesh_image="$(docker inspect "$mesh_container" --format '{{.Config.Image}}')"
mesh_network="${OWNER_BASELINE_NETWORK:-$(docker inspect "$mesh_container" \
  --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' | head -n1)}"
if [ -z "$mesh_network" ]; then
  echo "could not determine the backend container network" >&2
  exit 66
fi

mesh_mounted_export="$(docker inspect "$mesh_container" \
  --format '{{range .Mounts}}{{if eq .Destination "/owner-auth-inventory"}}{{.Source}}{{end}}{{end}}')"
mesh_mounted_mosquitto="$(docker inspect "$mesh_container" \
  --format '{{range .Mounts}}{{if eq .Destination "/mosquitto/config"}}{{.Source}}{{end}}{{end}}')"

mesh_mosquitto_dir="${OWNER_BASELINE_MOSQUITTO:-${mesh_mounted_mosquitto:-$mesh_repo_dir/mosquitto}}"
if [ ! -d "$mesh_mosquitto_dir" ]; then
  echo "mosquitto config directory not found: $mesh_mosquitto_dir" >&2
  exit 66
fi

mesh_export_dir="${OWNER_BASELINE_EXPORT_DIR:-$mesh_mounted_export}"
if [ -z "$mesh_export_dir" ] && [ -f "$mesh_env_file" ]; then
  mesh_export_dir="$(grep -m1 '^OWNER_AUTH_INVENTORY_HOST_DIR=' "$mesh_env_file" 2>/dev/null | cut -d= -f2-)"
fi
mesh_export_dir="${mesh_export_dir:-$mesh_repo_dir/owner-auth-inventory}"
if [ ! -d "$mesh_export_dir" ]; then
  echo "export directory not found: $mesh_export_dir" >&2
  echo "create it or set OWNER_BASELINE_EXPORT_DIR" >&2
  exit 66
fi
mesh_export_dir="$(cd -- "$mesh_export_dir" && pwd -P)"
mesh_export_uid="$(stat -c '%u' "$mesh_export_dir")"
mesh_export_gid="$(stat -c '%g' "$mesh_export_dir")"

mesh_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
mesh_basename="owner-grants-${mesh_stamp}-rebaseline.json"
mesh_export_path="$mesh_export_dir/$mesh_basename"
if [ -e "$mesh_export_path" ]; then
  echo "refusing to overwrite existing baseline: $mesh_export_path" >&2
  exit 73
fi

mesh_tmp_env="$(mktemp /tmp/owner-baseline-env.XXXXXX)"
mesh_tmp_out="$(mktemp /tmp/owner-baseline-out.XXXXXX)"
mesh_tmp_check="$(mktemp /tmp/owner-baseline-check.XXXXXX)"
mesh_mosquitto_gid="$(stat -c '%g' "$mesh_mosquitto_dir")"
cleanup() {
  rm -f "$mesh_tmp_env" "$mesh_tmp_out" "$mesh_tmp_check"
}
trap cleanup EXIT

python3 - "$mesh_container" "$mesh_tmp_env" <<'PY'
import json
import subprocess
import sys

container, destination = sys.argv[1], sys.argv[2]
raw = subprocess.check_output(["docker", "inspect", container]).decode()
envs = json.loads(raw)[0]["Config"]["Env"]
skip = {"PATH", "HOSTNAME", "HOME", "NODE_VERSION", "YARN_VERSION", "PWD", "SHLVL", "TERM", "container"}
kept = [entry for entry in envs if entry.split("=", 1)[0] not in skip]
with open(destination, "w", encoding="utf-8") as handle:
    handle.write("\n".join(kept) + "\n")
PY

run_tool() {
  # shellcheck disable=SC2068
  docker run --rm \
    --user "${mesh_export_uid}:${mesh_export_gid}" \
    --group-add "$mesh_mosquitto_gid" \
    --network "$mesh_network" \
    --env-file "$mesh_tmp_env" \
    -v "$mesh_mosquitto_dir:/mosquitto/config:ro" \
    -v "$mesh_export_dir:/owner-auth-inventory:rw" \
    "$mesh_image" \
    node dist/tools/inventoryOwnerAuthorization.js "$@"
}

echo "Generating owner baseline from ${mesh_image} (container ${mesh_container})"
if ! run_tool "--export=/owner-auth-inventory/${mesh_basename}" >"$mesh_tmp_out" 2>&1; then
  cat "$mesh_tmp_out" >&2
  echo "baseline generation failed" >&2
  exit 1
fi

echo "Validating the generated baseline"
if ! run_tool "--baseline=/owner-auth-inventory/${mesh_basename}" >"$mesh_tmp_check" 2>&1; then
  cat "$mesh_tmp_check" >&2
  echo "baseline validation command failed; new file kept at $mesh_export_path" >&2
  exit 1
fi

python3 - "$mesh_tmp_out" "$mesh_tmp_check" "$mesh_export_path" <<'PY'
import json
import sys

generated = json.load(open(sys.argv[1], encoding="utf-8"))
validation = json.load(open(sys.argv[2], encoding="utf-8"))
baseline_check = validation.get("baselineValidation") or {}
summary = {
    "exportPath": sys.argv[3],
    "generatedAt": generated.get("generatedAt"),
    "counts": generated.get("counts"),
    "configuredGeneration": (generated.get("configuredGeneration") or "")[:12],
    "aclDesiredGeneration": ((generated.get("aclState") or {}).get("desiredGeneration") or "")[:12],
    "contentSha256": (generated.get("contentSha256") or "")[:12],
    "baselineOk": baseline_check.get("ok"),
    "mismatches": baseline_check.get("mismatches"),
}
print(json.dumps(summary, indent=2))
if baseline_check.get("ok") is not True:
    sys.exit(1)
PY

echo
echo "Baseline validated. It is NOT active until OWNER_AUTH_INVENTORY_BASELINE_PATH"
echo "points at it and the backend is recreated."

if [ "$mesh_apply" != true ]; then
  echo
  echo "To activate:"
  echo "  1. review the summary above and confirm the grant changes are approved"
  echo "  2. set OWNER_AUTH_INVENTORY_BASELINE_PATH=/owner-auth-inventory/${mesh_basename} in .env"
  echo "  3. docker compose ${mesh_compose_files[*]} up -d --no-build --no-deps backend"
  echo "  4. curl -fsS http://127.0.0.1:3000/readyz"
  echo
  echo "Or re-run with --apply (and optionally --recreate)."
  exit 0
fi

mesh_env_backup="${mesh_env_file}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
cp "$mesh_env_file" "$mesh_env_backup"
chmod 600 "$mesh_env_backup"

python3 - "$mesh_env_file" "/owner-auth-inventory/${mesh_basename}" <<'PY'
import re
import sys

path, new_value = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    content = handle.read()
updated, count = re.subn(
    r"^OWNER_AUTH_INVENTORY_BASELINE_PATH=.*$",
    f"OWNER_AUTH_INVENTORY_BASELINE_PATH={new_value}",
    content,
    count=1,
    flags=re.MULTILINE,
)
if count != 1:
    sys.exit("OWNER_AUTH_INVENTORY_BASELINE_PATH not found in .env")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(updated)
PY
chmod 600 "$mesh_env_file"
echo "Repointed OWNER_AUTH_INVENTORY_BASELINE_PATH (backup: $mesh_env_backup)"

if [ "$mesh_recreate" = true ]; then
  mesh_image_env="BACKEND_IMAGE=$mesh_image"
  ( cd "$mesh_repo_dir" && env "$mesh_image_env" docker compose "${mesh_compose_files[@]}" \
      up -d --no-build --no-deps backend )
  echo "Backend recreated. Verify readiness:"
  echo "  curl -fsS http://127.0.0.1:3000/readyz"
else
  echo "Recreate the backend to activate:"
  echo "  cd $mesh_repo_dir"
  echo "  BACKEND_IMAGE=$mesh_image docker compose ${mesh_compose_files[*]} up -d --no-build --no-deps backend"
fi
