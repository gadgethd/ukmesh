#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage:
  replace-container.sh SERVICE --image=NAME@sha256:DIGEST \
    --backend-image=NAME@sha256:DIGEST --source-revision=40_HEX

Required environment:
  RESTORE_RECEIPT_PATH
  RESTORE_RECEIPT_VERIFY_KEY

Signature trust (choose one):
  COSIGN_PUBLIC_KEY
  or both COSIGN_CERTIFICATE_IDENTITY_REGEXP and
  COSIGN_CERTIFICATE_OIDC_ISSUER

The command verifies signed images and a fresh restore receipt, runs migrations,
tests the prior backend against the resulting schema, deploys without building,
waits for readiness, runs smoke/metric checks, and writes a signed release
receipt. It will only roll back automatically to a previously signed digest.
EOF
}

if [ "$#" -lt 1 ]; then
  usage >&2
  exit 64
fi

service="$1"
shift
desired_image=""
backend_image=""
source_revision=""
bootstrap_approval=""
for arg in "$@"; do
  case "$arg" in
    --image=*) desired_image="${arg#*=}" ;;
    --backend-image=*) backend_image="${arg#*=}" ;;
    --source-revision=*) source_revision="${arg#*=}" ;;
    --bootstrap-immutable=*) bootstrap_approval="${arg#*=}" ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; usage >&2; exit 64 ;;
  esac
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "${script_dir}/.." && pwd)"
project_name="${COMPOSE_PROJECT_NAME:-meshcore-analytics}"
release_dir="${RELEASE_STATUS_DIR:-/home/ben/meshcore-releases}"
cosign_key="${COSIGN_PUBLIC_KEY:-}"
cosign_identity_regexp="${COSIGN_CERTIFICATE_IDENTITY_REGEXP:-}"
cosign_oidc_issuer="${COSIGN_CERTIFICATE_OIDC_ISSUER:-}"
receipt_path="${RESTORE_RECEIPT_PATH:?RESTORE_RECEIPT_PATH is required}"
receipt_signature="${RESTORE_RECEIPT_SIGNATURE:-${receipt_path}.sig}"
receipt_verify_key="${RESTORE_RECEIPT_VERIFY_KEY:?RESTORE_RECEIPT_VERIFY_KEY is required}"
compatibility_timeout="${COMPATIBILITY_TIMEOUT_SECONDS:-90}"
if ! [[ "$compatibility_timeout" =~ ^[1-9][0-9]*$ ]] \
  || [ "$compatibility_timeout" -gt 600 ]; then
  echo "COMPATIBILITY_TIMEOUT_SECONDS must be an integer from 1 to 600" >&2
  exit 64
fi

declare -A image_variables=(
  [backend]=BACKEND_IMAGE
  [db-migrate]=BACKEND_IMAGE
  [path-learning-worker]=BACKEND_IMAGE
  [path-history-worker]=BACKEND_IMAGE
  [health-worker]=BACKEND_IMAGE
  [synthetic-monitor]=BACKEND_IMAGE
  [link-backfill-worker]=BACKEND_IMAGE
  [alert-receiver]=BACKEND_IMAGE
  [link-worker]=RF_WORKER_IMAGE
  [viewshed-worker]=RF_WORKER_IMAGE
  [app-ukmesh]=APP_IMAGE
  [website-ukmesh]=WEBSITE_IMAGE
  [website-dev]=WEBSITE_DEV_IMAGE
  [mesh-health-check]=HEALTHCHECK_IMAGE
  [mosquitto-reloader]=MOSQUITTO_RELOADER_IMAGE
)
image_variable="${image_variables[$service]:-}"
if [ -z "$image_variable" ]; then
  echo "service is not deployable through the immutable release path: $service" >&2
  exit 65
fi

digest_pattern='^[-a-zA-Z0-9._/:]+@sha256:[a-f0-9]{64}$'
if [[ ! "$desired_image" =~ $digest_pattern ]]; then
  echo "--image must be an immutable registry digest" >&2
  exit 64
fi
if [[ ! "$backend_image" =~ $digest_pattern ]]; then
  echo "--backend-image must be an immutable registry digest" >&2
  exit 64
fi
if [[ ! "$source_revision" =~ ^[a-f0-9]{40}$ ]]; then
  echo "--source-revision must be a full Git commit" >&2
  exit 64
fi
for command in docker cosign jq openssl sha256sum git curl; do
  command -v "$command" >/dev/null || {
    echo "required command is unavailable: $command" >&2
    exit 69
  }
done
if [ -n "$cosign_key" ]; then
  test -r "$cosign_key" || {
    echo "Cosign public key is not readable: $cosign_key" >&2
    exit 66
  }
elif [ -z "$cosign_identity_regexp" ] || [ -z "$cosign_oidc_issuer" ]; then
  echo "configure either COSIGN_PUBLIC_KEY or exact keyless identity and issuer trust" >&2
  exit 66
fi
for path in "$receipt_path" "$receipt_signature" "$receipt_verify_key"; do
  test -r "$path" || {
    echo "required verification input is not readable: $path" >&2
    exit 66
  }
done

cd "$project_dir"
docker compose --project-name "$project_name" config -q
if ! docker compose --project-name "$project_name" config --services | grep -Fxq -- "$service"; then
  echo "unknown Compose service: $service" >&2
  exit 65
fi
if [ "$(git rev-parse HEAD)" != "$source_revision" ]; then
  echo "working checkout does not match the release source revision" >&2
  exit 65
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "immutable releases require a clean tracked working tree" >&2
  exit 65
fi

verify_cosign_signature() {
  local image="$1"
  if [ -n "$cosign_key" ]; then
    cosign verify --key "$cosign_key" "$image" >/dev/null
  else
    cosign verify "$image" \
      --certificate-identity-regexp "$cosign_identity_regexp" \
      --certificate-oidc-issuer "$cosign_oidc_issuer" >/dev/null
  fi
}

verify_image() {
  local image="$1"
  verify_cosign_signature "$image"
  docker pull "$image" >/dev/null
  local label
  label="$(docker image inspect "$image" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')"
  if [ "$label" != "$source_revision" ]; then
    echo "image revision label does not match release source: $image" >&2
    return 1
  fi
}
verify_image "$backend_image"
if [ "$desired_image" != "$backend_image" ]; then
  verify_image "$desired_image"
fi

openssl dgst -sha256 -verify "$receipt_verify_key" \
  -signature "$receipt_signature" "$receipt_path" >/dev/null
test "$(jq -r '.format' "$receipt_path")" = "meshcore-restore-receipt-v1"
test "$(jq -r '.status' "$receipt_path")" = "verified"
for dataset in analytics owner_auth mosquitto redis configuration; do
  jq -e --arg dataset "$dataset" '.datasets | index($dataset) != null' \
    "$receipt_path" >/dev/null
done
now_epoch="$(date +%s)"
backup_epoch="$(date -d "$(jq -r '.backup_completed_at' "$receipt_path")" +%s)"
restore_epoch="$(date -d "$(jq -r '.restore_verified_at' "$receipt_path")" +%s)"
maximum_age="$((7 * 24 * 60 * 60))"
if [ "$((now_epoch - backup_epoch))" -gt "$maximum_age" ] \
  || [ "$((now_epoch - restore_epoch))" -gt "$maximum_age" ] \
  || [ "$backup_epoch" -gt "$((now_epoch + 300))" ] \
  || [ "$restore_epoch" -gt "$((now_epoch + 300))" ]; then
  echo "signed backup/restore evidence is stale or future-dated" >&2
  exit 65
fi

current_service_id="$(docker compose --project-name "$project_name" ps -q "$service")"
current_backend_id="$(docker compose --project-name "$project_name" ps -q backend)"
test -n "$current_service_id" && test -n "$current_backend_id" || {
  echo "current service and backend must be running before replacement" >&2
  exit 69
}
prior_image="$(docker inspect "$current_service_id" --format '{{.Config.Image}}')"
prior_backend_image="$(docker inspect "$current_backend_id" --format '{{.Config.Image}}')"
prior_is_signed="false"
if [[ "$prior_image" =~ $digest_pattern ]] \
  && verify_cosign_signature "$prior_image" >/dev/null 2>&1; then
  prior_is_signed="true"
elif [ "$bootstrap_approval" != "bootstrap-${service}-${source_revision}" ]; then
  echo "the current service is not a signed digest; use the documented one-time bootstrap approval" >&2
  exit 65
fi

release_id="release-$(date -u +%Y%m%dT%H%M%SZ)-${source_revision:0:12}-${service}"
mkdir -p "$release_dir"
release_path="$release_dir/${release_id}.json"
config_sha="$(
  docker compose --project-name "$project_name" config \
    | sha256sum | awk '{print $1}'
)"
restore_receipt_id="$(jq -r '.receipt_id' "$receipt_path")"

write_release_status() {
  local status="$1"
  local detail="${2:-}"
  local schema_version="${3:-0}"
  jq -n \
    --arg release_id "$release_id" \
    --arg service "$service" \
    --arg source_revision "$source_revision" \
    --arg image "$desired_image" \
    --arg backend_image "$backend_image" \
    --arg prior_image "$prior_image" \
    --arg prior_backend_image "$prior_backend_image" \
    --arg config_sha256 "$config_sha" \
    --arg restore_receipt_id "$restore_receipt_id" \
    --arg status "$status" \
    --arg detail "$detail" \
    --arg recorded_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson schema_version "$schema_version" \
    '{
      format:"meshcore-release-status-v1",
      release_id:$release_id,
      service:$service,
      source_revision:$source_revision,
      image:$image,
      backend_image:$backend_image,
      prior_image:$prior_image,
      prior_backend_image:$prior_backend_image,
      schema_version:$schema_version,
      config_sha256:$config_sha256,
      restore_receipt_id:$restore_receipt_id,
      status:$status,
      detail:$detail,
      recorded_at:$recorded_at
    }' >"${release_path}.partial"
  mv "${release_path}.partial" "$release_path"
  if [ -n "${RELEASE_RECEIPT_SIGNING_KEY:-}" ]; then
    openssl dgst -sha256 -sign "$RELEASE_RECEIPT_SIGNING_KEY" \
      -out "${release_path}.sig.partial" "$release_path"
    mv "${release_path}.sig.partial" "${release_path}.sig"
  fi
}
write_release_status preflight

echo "Running the required migration job with the signed backend image..."
BACKEND_IMAGE="$backend_image" \
  docker compose --project-name "$project_name" run --rm db-migrate
schema_version="$(
  docker compose --project-name "$project_name" exec -T timescaledb \
    psql -U "${POSTGRES_USER:-meshcore}" -d "${POSTGRES_DB:-meshcore}" -Atc \
    "SELECT COALESCE(MAX(((regexp_match(name, '^([0-9]+)_'))[1])::int), 0) FROM schema_migrations"
)"

compat_name="${project_name}-compat-${source_revision:0:12}-$$"
compat_env="$(mktemp)"
compat_cleanup() {
  docker rm -f "$compat_name" >/dev/null 2>&1 || true
  rm -f "$compat_env"
}
trap compat_cleanup EXIT
docker inspect "$current_backend_id" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  >"$compat_env"
network_name="$(
  docker inspect "$current_backend_id" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
    | head -n 1
)"
echo "Testing the prior backend image against the post-migration schema..."
docker run -d \
  --name "$compat_name" \
  --network "$network_name" \
  --env-file "$compat_env" \
  --volumes-from "$current_backend_id:ro" \
  -e MQTT_INGEST_ENABLED=false \
  -e OWNER_AUTHORIZATION_MODE=shadow \
  -e OWNER_ACL_MODE=shadow \
  -e PORT=3000 \
  -e METRICS_PORT=9091 \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  "$prior_backend_image" >/dev/null
compat_ready="false"
for _ in $(seq 1 "$compatibility_timeout"); do
  if docker exec "$compat_name" wget -qO- http://127.0.0.1:3000/readyz \
    2>/dev/null | jq -e '.status == "ready"' >/dev/null 2>&1 \
    && docker exec "$compat_name" wget -qO- \
      'http://127.0.0.1:3000/api/stats?network=ukmesh' \
      2>/dev/null | jq -e '.totalNodes >= 0' >/dev/null 2>&1; then
    compat_ready="true"
    break
  fi
  sleep 1
done
if [ "$compat_ready" != "true" ]; then
  docker logs "$compat_name" >&2 || true
  write_release_status stopped "prior image failed post-migration compatibility test" "$schema_version"
  exit 1
fi
docker rm -f "$compat_name" >/dev/null

smoke_service() {
  local target_service="$1"
  local container_id
  container_id="$(docker compose --project-name "$project_name" ps -q "$target_service")"
  test -n "$container_id"
  test "$(docker inspect "$container_id" --format '{{.State.Running}}')" = "true"
  local health
  health="$(docker inspect "$container_id" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}')"
  test "$health" = "healthy" || test "$health" = "none"
  docker compose --project-name "$project_name" exec -T backend \
    wget -qO- http://127.0.0.1:3000/readyz \
    | jq -e '.status == "ready"' >/dev/null
  metrics="$(docker compose --project-name "$project_name" exec -T backend \
    wget -qO- http://127.0.0.1:9091/metrics)"
  grep -q '^meshcore_process_' <<<"$metrics"
  case "$target_service" in
    backend)
      curl --fail --silent http://127.0.0.1:3000/readyz \
        | jq -e '.status == "ready"' >/dev/null
      ;;
    app-ukmesh) curl --fail --silent http://127.0.0.1:3003/ >/dev/null ;;
    website-ukmesh) curl --fail --silent http://127.0.0.1:3004/ >/dev/null ;;
    mesh-health-check)
      curl --fail --silent http://127.0.0.1:3090/api/bootstrap \
        | jq -e '.mqtt.connected == true' >/dev/null
      ;;
  esac
}

deploy_image() {
  local image="$1"
  if [ "$image_variable" = "BACKEND_IMAGE" ]; then
    BACKEND_IMAGE="$image" \
      docker compose --project-name "$project_name" up \
        --detach --no-build --wait --wait-timeout 240 "$service"
  else
    env \
      "$image_variable=$image" \
      "BACKEND_IMAGE=$backend_image" \
      docker compose --project-name "$project_name" up \
        --detach --no-build --wait --wait-timeout 240 "$service"
  fi
}

write_release_status deploying "" "$schema_version"
set +e
deploy_image "$desired_image"
deploy_status=$?
if [ "$deploy_status" -eq 0 ]; then
  smoke_service "$service"
  deploy_status=$?
fi
set -e

if [ "$deploy_status" -ne 0 ]; then
  write_release_status failed "readiness, smoke, or metric check failed" "$schema_version"
  if [ "$prior_is_signed" = "true" ]; then
    echo "Release failed; restoring the prior signed digest..."
    set +e
    deploy_image "$prior_image"
    rollback_status=$?
    if [ "$rollback_status" -eq 0 ]; then
      smoke_service "$service"
      rollback_status=$?
    fi
    set -e
    if [ "$rollback_status" -eq 0 ]; then
      write_release_status rolled_back "automatic signed-digest rollback completed" "$schema_version"
    else
      write_release_status rollback_failed "manual recovery required" "$schema_version"
    fi
  else
    write_release_status failed_no_rollback "prior image was not signed; manual recovery required" "$schema_version"
  fi
  exit 1
fi

write_release_status deployed "" "$schema_version"
ln -sfn "$(basename "$release_path")" "$release_dir/latest-${service}.json"
echo "Immutable release deployed:"
jq '{release_id,service,source_revision,image,schema_version,status,restore_receipt_id}' \
  "$release_path"
