#!/usr/bin/env bash
#
# refresh-owner-baseline.sh
#
# Regenerate and validate the owner-authorization inventory baseline after an
# approved owner grant/config change.
#
# The running backend fails readiness closed when the configured baseline no
# longer matches the live database/ACL (counts, grants, generations). Use this
# script after reviewing and approving the grant change. With a live baseline
# path configured, activation replaces the mounted file atomically.
#
# Usage:
#   scripts/refresh-owner-baseline.sh                 # generate + validate only
#   scripts/refresh-owner-baseline.sh --activate-live # activate mounted current.json
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
mesh_activate_live=false

usage() {
  sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --apply) mesh_apply=true ;;
    --recreate) mesh_recreate=true ;;
    --activate-live) mesh_activate_live=true ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
  shift
done
if [ "$mesh_activate_live" = true ] && { [ "$mesh_apply" = true ] || [ "$mesh_recreate" = true ]; }; then
  echo '--activate-live cannot be combined with --apply or --recreate' >&2
  exit 64
fi

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
if [ "$mesh_activate_live" = true ]; then
  mesh_image="$(docker inspect "$mesh_container" --format '{{.Image}}')"
fi
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

if [ "$mesh_activate_live" = true ]; then
  command -v flock >/dev/null 2>&1 || { echo 'live activation requires flock' >&2; exit 69; }
  exec {mesh_activation_lock}>>"$mesh_export_dir/.owner-baseline-activation.lock"
  flock -x "$mesh_activation_lock"
fi

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

python3 - "$mesh_container" "$mesh_tmp_env" "$mesh_activate_live" "$mesh_export_dir" "$mesh_mosquitto_dir" <<'PY'
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys

container, destination, live, export_dir, mosquitto_dir = sys.argv[1:]
raw = subprocess.check_output(["docker", "inspect", container]).decode()
metadata = json.loads(raw)[0]
envs = metadata["Config"]["Env"]
if live == "true":
    expected = "OWNER_AUTH_INVENTORY_BASELINE_PATH=/owner-auth-inventory/current.json"
    if [entry for entry in envs if entry.startswith("OWNER_AUTH_INVENTORY_BASELINE_PATH=")] != [expected]:
        sys.exit("live baseline activation requires the running backend to use /owner-auth-inventory/current.json")
    mounts = [mount for mount in metadata["Mounts"] if mount["Destination"] == "/owner-auth-inventory"]
    if (len(mounts) != 1 or mounts[0]["Type"] != "bind" or mounts[0]["RW"]
            or Path(mounts[0]["Source"]).resolve() != Path(export_dir)):
        sys.exit("live baseline directory must match the backend's read-only bind mount")
    if any(mount["Destination"] == "/owner-auth-inventory/current.json" for mount in metadata["Mounts"]):
        sys.exit("current.json must be read through the directory mount, not a file mount")
    acl_mounts = [mount for mount in metadata["Mounts"] if mount["Destination"] == "/mosquitto/config"]
    if (len(acl_mounts) != 1 or acl_mounts[0]["Type"] != "bind"
            or Path(acl_mounts[0]["Source"]).resolve() != Path(mosquitto_dir).resolve()):
        sys.exit("live baseline validation requires the running backend's Mosquitto config mount")
    acl_dir = PurePosixPath("/mosquitto/config")
    acl = PurePosixPath(next((entry.split("=", 1)[1] for entry in envs
                              if entry.startswith("MOSQUITTO_ACL_PATH=")), "/mosquitto/config/acl"))
    if acl == acl_dir or not acl.is_relative_to(acl_dir) or ".." in acl.parts:
        sys.exit("live baseline validation requires the ACL in the Mosquitto config directory")
    for mount in metadata["Mounts"]:
        nested = PurePosixPath(mount["Destination"])
        if nested != acl_dir and nested.is_relative_to(acl_dir) and acl.is_relative_to(nested):
            sys.exit("the ACL must be read through the Mosquitto config directory mount")
    active = Path(export_dir) / "current.json"
    if active.is_symlink() or not active.is_file():
        sys.exit("live baseline activation requires an existing regular current.json")
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

if [ "$mesh_activate_live" = true ]; then
  python3 "$mesh_script_dir/lib/owner-baseline-promote.py" "$mesh_export_path" \
    "$mesh_export_dir/owner-grants-${mesh_stamp}-previous.json"
  read -r mesh_candidate_hash _ < <(sha256sum -- "$mesh_export_path")
  read -r mesh_active_hash _ < <(docker exec "$mesh_container" sha256sum /owner-auth-inventory/current.json)
  if [ "$mesh_candidate_hash" != "$mesh_active_hash" ]; then
    echo 'The running backend does not see the promoted baseline. Check its mount and permissions.' >&2
    exit 1
  fi
  echo 'Validated baseline activated at /owner-auth-inventory/current.json.'
  exit 0
fi

echo
echo 'Baseline validated and ready for activation.'

if [ "$mesh_apply" != true ]; then
  echo
  echo "To activate:"
  echo "  1. review the summary above and confirm the grant changes are approved"
  echo "  2. set OWNER_AUTH_INVENTORY_BASELINE_PATH=/owner-auth-inventory/${mesh_basename} in .env"
  echo "  3. docker compose ${mesh_compose_files[*]} up -d --no-build --no-deps backend"
  echo "  4. curl -fsS http://127.0.0.1:3000/readyz"
  echo
  echo 'With current.json already configured, use --activate-live for in-place activation.'
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
