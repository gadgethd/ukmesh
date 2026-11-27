#!/usr/bin/env bash
# Private lifecycle adapter; corescope-public.sh owns environment and fixture gates.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/runtime.sh"
mode=${1:?mode required}
token=${2:?owned fixture token required}
attempt=${3:?owned attempt identity required}
[[ $token =~ ^[a-z0-9_]{1,38}$ && $attempt =~ ^[a-f0-9]{12}$ && $UKMESH_DATABASE == "lean_fixture_$token" ]] || {
  echo 'RAM qualification requires the selected small owned fixture' >&2; exit 1;
}
case "$mode" in
  ram)
    export MQTT_INGEST_MODE=ram
    export MQTT_RAM_STREAM_KEY="meshcore:ingest:lean:$token:corescope:$attempt"
    export MQTT_RAM_CONSUMER_GROUP="lean-corescope-$token-$attempt"
    export MQTT_RAM_CONSUMER_NAME=owned-full-mqtt-ingest
    ;;
  durable) export MQTT_INGEST_MODE=durable ;;
  *) echo 'Unknown owned RAM lifecycle action' >&2; exit 2 ;;
esac
"${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 180 backend mqtt-ingest
