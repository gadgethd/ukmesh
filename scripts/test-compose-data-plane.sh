#!/usr/bin/env bash
# The caller provisions this owned CI stack; this probe owns no long-lived service.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
project=${1:?owned CI Compose project name required}
[[ $project == meshcore-ci-* ]] || { echo 'data-plane probe requires an owned meshcore-ci project' >&2; exit 2; }
observer=${MQTT_E2E_OBSERVER:?fixture observer required}
[[ $observer =~ ^[a-fA-F0-9]{64}$ ]] || { echo 'fixture observer must be a 64-digit hex ID' >&2; exit 2; }
: "${MQTT_E2E_PUBLISHER:?fixture publisher required}" "${MQTT_E2E_PASSWORD:?fixture password required}"
compose=(docker compose -f "$repo/docker-compose.yml" -f "$repo/docker-compose.ci.yml" --project-name "$project")
"${compose[@]}" exec -T -e MQTT_E2E_OBSERVER -e MQTT_E2E_PUBLISHER -e MQTT_E2E_PASSWORD \
  backend node --input-type=module < "$repo/scripts/fixtures/compose-data-plane-probe.mjs"
readback=''
for ((attempt=0; attempt<30; attempt++)); do
  readback=$("${compose[@]}" exec -T timescaledb psql -U meshcore -d meshcore -Atc \
    "SELECT EXISTS(SELECT 1 FROM core.nodes WHERE node_id = UPPER('$observer') AND network = 'test'), EXISTS(SELECT 1 FROM mesh.packets WHERE rx_node_id = UPPER('$observer') AND network = 'test'), EXISTS(SELECT 1 FROM mesh.packet_paths WHERE rx_node_id = UPPER('$observer') AND network = 'test'), EXISTS(SELECT 1 FROM mesh.packet_hourly_stats WHERE network = 'test')")
  [[ $readback == 't|t|t|t' ]] && break
  sleep 1
done
[[ $readback == 't|t|t|t' ]]
printf 'Owned data-plane persistence and WebSocket fanout passed.\n'
