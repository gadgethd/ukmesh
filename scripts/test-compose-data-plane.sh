#!/usr/bin/env bash
# Exercise the caller's disposable CI stack and leave both roles running.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
project=${1:?owned CI Compose project name required}
[[ $project == meshcore-ci-* ]] || { echo 'data-plane probe requires an owned meshcore-ci project' >&2; exit 2; }
observer=${MQTT_E2E_OBSERVER:?fixture observer required}
[[ $observer =~ ^[a-fA-F0-9]{64}$ ]] || { echo 'fixture observer must be a 64-digit hex ID' >&2; exit 2; }
: "${MQTT_E2E_PUBLISHER:?fixture publisher required}" "${MQTT_E2E_PASSWORD:?fixture password required}"
compose=(docker compose -f "$repo/docker-compose.yml" -f "$repo/docker-compose.ci.yml" --project-name "$project")
run_id=$(cut -d- -f1 /proc/sys/kernel/random/uuid)

probe() {
  local service=$1
  shift
  "${compose[@]}" exec -T -e MQTT_E2E_OBSERVER -e MQTT_E2E_PUBLISHER -e MQTT_E2E_PASSWORD \
    "$service" node --input-type=module - "$@" < "$repo/scripts/fixtures/compose-data-plane-probe.mjs"
}

packet_counts() {
  "${compose[@]}" exec -T timescaledb psql -X -U meshcore -d meshcore -At -v ON_ERROR_STOP=1 \
    -v observer="$observer" -v hashes="$1" <<'SQL'
WITH expected AS (
  SELECT jsonb_array_elements_text(:'hashes'::jsonb) AS packet_hash
), observed AS MATERIALIZED (
  SELECT packets.packet_hash, packets.observation_id
  FROM mesh.packets JOIN expected USING (packet_hash)
  WHERE network = 'test' AND rx_node_id = UPPER(:'observer')
    AND topic = 'meshcore-test/CI/' || :'observer' || '/packets'
    AND time > NOW() - interval '30 minutes'
)
SELECT (SELECT count(*) FROM observed),
       (SELECT count(DISTINCT packet_hash) FROM observed),
       (SELECT count(*) FROM mesh.ingest_observation_ledger ledger
                         JOIN observed USING (packet_hash, observation_id)
                         WHERE ledger.network = 'test');
SQL
}

wait_for_packets() {
  local hashes=$1 count=$2 current attempt
  for ((attempt=0; attempt<30; attempt++)); do
    current=$(packet_counts "$hashes")
    [[ $current == "$count|$count|$count" ]] && return
    sleep 1
  done
  printf 'Expected %d packets, distinct hashes and ledger rows, got %s\n' "$count" "$current" >&2
  return 1
}

wait_for_health() {
  local service=$1 path=$2 attempt
  for ((attempt=0; attempt<30; attempt++)); do
    if "${compose[@]}" exec -T "$service" wget -qT 2 -O /dev/null "http://127.0.0.1:3000/$path" 2>/dev/null; then
      return
    fi
    sleep 1
  done
  printf '%s did not recover %s\n' "$service" "$path" >&2
  return 1
}

started_at() {
  docker inspect -f '{{.State.StartedAt}}' "$1"
}

stop_for_test() {
  stopped=$1
  "${compose[@]}" stop "$stopped"
  [[ $(docker inspect -f '{{.State.Status}}' "$2") == exited ]]
}

resume_after_test() {
  "${compose[@]}" start "$stopped"
  stopped=''
}

backend=$("${compose[@]}" ps -q backend)
ingest=$("${compose[@]}" ps -q mqtt-ingest)
test -n "$backend" && test -n "$ingest"
stopped=''
trap 'if [[ -n $stopped ]]; then "${compose[@]}" start "$stopped"; fi' EXIT

seed=$(probe backend fanout "$run_id" 0 1)
wait_for_packets "$seed" 1
readback=''
for ((attempt=0; attempt<30; attempt++)); do
  readback=$("${compose[@]}" exec -T timescaledb psql -X -U meshcore -d meshcore -At -v ON_ERROR_STOP=1 \
    -v observer="$observer" -v seed="$seed" <<'SQL'
SELECT EXISTS (SELECT 1 FROM core.nodes WHERE node_id = UPPER(:'observer') AND network = 'test'),
       EXISTS (SELECT 1 FROM mesh.packet_paths WHERE rx_node_id = UPPER(:'observer') AND network = 'test'
                 AND packet_hash = (:'seed'::jsonb ->> 0)),
       EXISTS (SELECT 1 FROM mesh.packet_hourly_stats WHERE network = 'test');
SQL
)
  [[ $readback == 't|t|t' ]] && break
  sleep 1
done
[[ $readback == 't|t|t' ]]

count=36
api_started=$(started_at "$backend")
ingest_started=$(started_at "$ingest")
stop_for_test backend "$backend"
api_down=$(probe mqtt-ingest publish "$run_id" 1 "$count")
wait_for_packets "$api_down" "$count"
[[ $(started_at "$ingest") == "$ingest_started" ]]
resume_after_test
[[ $(started_at "$backend") != "$api_started" ]]
wait_for_health backend api-readyz
printf '%d QoS 1 packets committed with HTTP/WebSocket stopped. MQTT ingest did not restart.\n' "$count"

api_started=$(started_at "$backend")
stop_for_test mqtt-ingest "$ingest"
probe backend api
ingest_down=$(probe backend publish "$run_id" "$((count + 1))" "$count")
[[ $(packet_counts "$ingest_down") == '0|0|0' ]]
resume_after_test
wait_for_packets "$ingest_down" "$count"
wait_for_health backend readyz
[[ $(started_at "$ingest") != "$ingest_started" ]]
[[ $(started_at "$backend") == "$api_started" ]]
printf '%d QoS 1 packets queued with ingest stopped and committed after restart. HTTP/WebSocket did not restart.\n' "$count"

recovered=$(probe backend fanout "$run_id" "$((2 * count + 1))" 1)
wait_for_packets "$recovered" 1
wait_for_packets "$api_down" "$count"
wait_for_packets "$ingest_down" "$count"
printf 'Owned data-plane probe verified %d unique packet observations, path persistence and WebSocket fanout.\n' "$((2 * count + 2))"
