#!/usr/bin/env bash

meshcore_infra_compose() {
  local project_name="${MESHCORE_INFRA_PROJECT_NAME:-meshcore-infra}"
  local project_dir="${MESHCORE_INFRA_PROJECT_DIR:-${MESHCORE_INFRA_DIR:-}}"
  local compose_file="${MESHCORE_INFRA_COMPOSE_FILE:-}"

  if [ -z "$compose_file" ]; then
    if [ -z "$project_dir" ]; then
      echo 'MESHCORE_INFRA_PROJECT_DIR or MESHCORE_INFRA_COMPOSE_FILE is required' >&2
      return 64
    fi
    compose_file="$project_dir/docker-compose.yml"
  fi
  if [ -z "$project_dir" ]; then
    project_dir="$(dirname -- "$compose_file")"
  fi
  if [ ! -f "$compose_file" ]; then
    echo "infrastructure Compose file is missing: $compose_file" >&2
    return 66
  fi

  docker compose \
    --project-directory "$project_dir" \
    --project-name "$project_name" \
    -f "$compose_file" \
    "$@"
}

resolve_infra_container() {
  local service="${1:-}"
  case "$service" in
    timescaledb|redis|mosquitto) ;;
    *)
      echo "unsupported infrastructure service: ${service:-<empty>}" >&2
      return 64
      ;;
  esac

  local raw_ids
  if ! raw_ids="$(meshcore_infra_compose ps -q "$service")"; then
    echo "could not resolve infrastructure service: $service" >&2
    return 69
  fi

  local -a ids=()
  local id
  while IFS= read -r id; do
    id="${id//$'\r'/}"
    if [ -n "${id//[[:space:]]/}" ]; then
      ids+=("$id")
    fi
  done <<<"$raw_ids"
  if [ "${#ids[@]}" -ne 1 ]; then
    echo "expected one infrastructure container for $service; found ${#ids[@]}" >&2
    return 69
  fi

  local state
  if ! state="$(docker inspect --format '{{.State.Status}}' "${ids[0]}")"; then
    echo "could not inspect infrastructure container for $service" >&2
    return 69
  fi
  if [ "$state" != 'running' ]; then
    echo "infrastructure container for $service is not running" >&2
    return 69
  fi

  printf '%s\n' "${ids[0]}"
}
