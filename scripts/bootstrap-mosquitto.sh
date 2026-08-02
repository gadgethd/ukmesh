#!/usr/bin/env bash
set -euo pipefail

mesh_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
mesh_repo_dir="$(cd -- "$mesh_script_dir/.." && pwd -P)"
mesh_config_dir="${MOSQUITTO_CONFIG_DIR:-$mesh_repo_dir/mosquitto}"
mesh_username="${MQTT_USERNAME:-backend}"
mesh_password="${MQTT_PASSWORD:-}"
mesh_image="${MOSQUITTO_IMAGE:-eclipse-mosquitto@sha256:9cfdd46ad59f3e3e5f592f6baf57ab23e1ad00605509d0f5c1e9b179c5314d87}"
mesh_backend_uid="${MOSQUITTO_BACKEND_UID:-1000}"
mesh_broker_gid="${MOSQUITTO_BROKER_GID:-1883}"

mesh_normalize_permissions() {
  local mesh_names=(passwd acl)
  local mesh_paths=(/mosquitto/config/passwd /mosquitto/config/acl)
  if [[ -e "$mesh_config_dir/acl.lkg" ]]; then
    mesh_names+=(acl.lkg)
    mesh_paths+=(/mosquitto/config/acl.lkg)
  fi
  for mesh_name in "${mesh_names[@]}"; do
    if [[ -L "$mesh_config_dir/$mesh_name" ]]; then
      echo "refusing symlink: $mesh_config_dir/$mesh_name" >&2
      exit 1
    fi
  done
  docker run --rm \
    --user 0:0 \
    --cap-drop ALL \
    --cap-add CHOWN \
    --cap-add DAC_OVERRIDE \
    --entrypoint chown \
    -v "$mesh_config_dir:/mosquitto/config" \
    "$mesh_image" \
    "$mesh_backend_uid:$mesh_broker_gid" /mosquitto/config "${mesh_paths[@]}"
  docker run --rm \
    --user "$mesh_backend_uid:$mesh_broker_gid" \
    --cap-drop ALL \
    --entrypoint chmod \
    -v "$mesh_config_dir:/mosquitto/config" \
    "$mesh_image" \
    2750 /mosquitto/config
  docker run --rm \
    --user "$mesh_backend_uid:$mesh_broker_gid" \
    --cap-drop ALL \
    --entrypoint chmod \
    -v "$mesh_config_dir:/mosquitto/config" \
    "$mesh_image" \
    640 "${mesh_paths[@]}"
}

if [[ -z "$mesh_password" ]]; then
  echo "MQTT_PASSWORD is required" >&2
  exit 1
fi
if [[ ! "$mesh_username" =~ ^[A-Za-z0-9._-]{1,128}$ ]]; then
  echo "MQTT_USERNAME contains unsupported characters" >&2
  exit 1
fi

install -d -m 700 "$mesh_config_dir"
for mesh_name in passwd acl; do
  mesh_path="$mesh_config_dir/$mesh_name"
  if [[ -L "$mesh_path" ]]; then
    echo "refusing symlink: $mesh_path" >&2
    exit 1
  fi
done

mesh_passwd="$mesh_config_dir/passwd"
mesh_acl="$mesh_config_dir/acl"
if [[ -e "$mesh_passwd" || -e "$mesh_acl" ]]; then
  if [[ ! -s "$mesh_passwd" || ! -s "$mesh_acl" ]]; then
    echo "existing Mosquitto bootstrap files are incomplete; refusing overwrite" >&2
    exit 1
  fi
  mesh_normalize_permissions
  echo "Mosquitto credentials already exist; permissions verified"
  exit 0
fi

mesh_passwd_tmp="$mesh_config_dir/.passwd.bootstrap.$$"
mesh_acl_tmp="$mesh_config_dir/.acl.bootstrap.$$"
mesh_cleanup() {
  rm -f -- "$mesh_passwd_tmp" "$mesh_acl_tmp"
}
trap mesh_cleanup EXIT

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$mesh_config_dir:/mosquitto/config" \
  "$mesh_image" \
  mosquitto_passwd -b -c "/mosquitto/config/$(basename "$mesh_passwd_tmp")" \
  "$mesh_username" "$mesh_password"

{
  printf 'user %s\n' "$mesh_username"
  printf 'topic read meshcore/#\n'
  printf 'topic read ukmesh/#\n'
  printf 'topic read meshcore-test/#\n'
} > "$mesh_acl_tmp"

chmod 640 "$mesh_passwd_tmp" "$mesh_acl_tmp"
mv "$mesh_passwd_tmp" "$mesh_passwd"
mv "$mesh_acl_tmp" "$mesh_acl"
trap - EXIT
mesh_normalize_permissions
echo "Created least-privilege Mosquitto backend credentials"
