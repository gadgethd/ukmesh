#!/usr/bin/env bash
# Local wiring only. Resource policy is inherited from the main Compose file.
unset COMPOSE_PROJECT_NAME
export POSTGRES_USER=meshcore POSTGRES_PASSWORD=meshcore POSTGRES_DB=meshcore
export REDIS_URL=redis://ukmesh-sample-redis:6379 REDIS_PASSWORD=local-unused-cache-only
export MQTT_BROKER_URL=ws://ukmesh-sample-mqtt:9001 MQTT_USERNAME= MQTT_PASSWORD=
export JWT_SECRET=local-only-development-secret-at-least-32-characters
export OWNER_COOKIE_SECRET=local-only-cookie-secret-at-least-32-characters
export OPERATOR_SITE_TOKEN=local-only-token
export ALERT_RECEIVER_TOKEN=local-full-alert-token-0123456789abcdef
export GRAFANA_ADMIN_PASSWORD=local-full-grafana-only
export ANUBIS_ED25519_PRIVATE_KEY_HEX=0101010101010101010101010101010101010101010101010101010101010101
export HEALTHCHECK_SOURCE_REF=c93af192ec7cc0ecdfb3e9d0ea163840202ccbfc
export CLOUDFLARE_TUNNEL_TOKEN= SOURCE_REVISION="$(git -C "$repo" rev-parse HEAD)"
export BACKEND_IMAGE=ukmesh-full-backend:local APP_IMAGE=ukmesh-full-app:local
export WEBSITE_IMAGE=ukmesh-full-website:local WEBSITE_DEV_IMAGE=ukmesh-full-website-dev:local
export TAGGER_WORKER_IMAGE=ukmesh-full-tagger:local RF_WORKER_IMAGE=ukmesh-full-link:local
export HOPREACH_IMAGE=ukmesh-full-hopreach:local HEALTHCHECK_IMAGE=ukmesh-full-health-check:local
export MOSQUITTO_RELOADER_IMAGE=ukmesh-full-reloader:local
export JOURNAL_GID="$(stat -c '%g' /var/log/journal)"
export UKMESH_DATABASE=meshcore OWNER_DATABASE=meshcore_owner_auth BEACON_DATABASE=beacon_dev
export CORESCOPE_DATA_DIR="$repo/plans/companion-dev/runtime/corescope"
targets="$repo/plans/lean-runtime/runtime/targets.env"
if [[ -f $targets ]]; then
  while IFS='=' read -r key value; do
    [[ -n $key && $key != \#* ]] || continue
    case "$key" in
      UKMESH_DATABASE) [[ $value == meshcore || $value =~ ^lean_[a-z0-9_]+$ ]] ;;
      OWNER_DATABASE) [[ $value == meshcore_owner_auth || $value =~ ^lean_[a-z0-9_]+$ ]] ;;
      BEACON_DATABASE) [[ $value == beacon_dev || $value =~ ^lean_[a-z0-9_]+$ ]] ;;
      CORESCOPE_DATA_DIR)
        [[ $value == "$repo/plans/companion-dev/runtime/corescope" || $value == "$repo/plans/lean-runtime/runtime/fixtures/"* ]] &&
          [[ $(realpath -m -- "$value") == "$value" ]] ;;
      *) echo "Unknown fixture target key: $key" >&2; return 1 ;;
    esac || { echo "Invalid local fixture target: $key" >&2; return 1; }
    export "$key=$value"
  done < "$targets"
fi

export FULL_DEV_LAN_APP_ORIGINS= FULL_DEV_LAN_HEALTH_ORIGINS=
if [[ $UKMESH_DATABASE == meshcore && -f $repo/plans/lean-runtime/runtime/lan/active.json ]]; then
  lan_origins=$(python3 "$repo/scripts/full-dev/lan/control.py" origins)
  mapfile -t lan_origin_lines <<< "$lan_origins"
  export FULL_DEV_LAN_APP_ORIGINS=",${lan_origin_lines[0]}"
  export FULL_DEV_LAN_HEALTH_ORIGINS=",${lan_origin_lines[1]}"
fi

# Runtime containers receive only their own role; the administrator DSN is
# interpolated exclusively into the one-shot migration service.
export POSTGRES_DB="$UKMESH_DATABASE"
export MESHCORE_HTTP_DATABASE_URL="postgresql://meshcore_api:local-full-dev-only@ukmesh-sample-db:5432/$UKMESH_DATABASE"
export MESHCORE_INGEST_DATABASE_URL="postgresql://meshcore_ingest:local-full-dev-only@ukmesh-sample-db:5432/$UKMESH_DATABASE"
export MESHCORE_WORKER_DATABASE_URL="postgresql://meshcore_worker:local-full-dev-only@ukmesh-sample-db:5432/$UKMESH_DATABASE"
export OWNER_DATABASE_URL="postgresql://meshcore_owner:local-full-dev-only@ukmesh-sample-db:5432/$OWNER_DATABASE"
export MESHCORE_MIGRATE_DATABASE_URL="postgresql://meshcore:meshcore@ukmesh-sample-db:5432/$UKMESH_DATABASE"
export POSTGRES_EXPORTER_DATABASE_URL="postgresql://meshcore_monitor:local-full-dev-only@ukmesh-sample-db:5432/$UKMESH_DATABASE?sslmode=disable"
export DATABASE_ROLE_ENFORCEMENT=true
