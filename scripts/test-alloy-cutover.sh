#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="meshcore-alloy-test-$$"
network_name="${run_id}-network"
loki_name="${run_id}-loki"
alloy_name="${run_id}-alloy"
cursor_volume="${run_id}-cursor"
loki_volume="${run_id}-loki-data"
sentinel_service="${run_id}-sentinel"
sentinel_project="${run_id}-project"
sentinel_text="alloy-cutover-${run_id}"

loki_image="grafana/loki@sha256:5fe9fa99e9a747297cdf0239a5b25d192d8f668bd6505b09beef4dffcab5aac2"
alloy_image="grafana/alloy@sha256:491b0578c04983fd54fe99b587b6fab4404dc46d0dc16677bd6b00cc1140b308"
sentinel_image="busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"

cleanup() {
  docker rm -f "$alloy_name" "$loki_name" >/dev/null 2>&1 || true
  docker network rm "$network_name" >/dev/null 2>&1 || true
  docker volume rm "$cursor_volume" "$loki_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$network_name" >/dev/null
docker volume create "$cursor_volume" >/dev/null
docker volume create "$loki_volume" >/dev/null

docker run -d \
  --name "$loki_name" \
  --network "$network_name" \
  --network-alias loki \
  -p 127.0.0.1::3100 \
  -v "$repo_dir/logging/loki.yaml:/etc/loki/loki.yaml:ro" \
  -v "$loki_volume:/loki" \
  "$loki_image" \
  -config.file=/etc/loki/loki.yaml >/dev/null

loki_port="$(docker port "$loki_name" 3100/tcp | sed 's/.*://')"
for _ in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:${loki_port}/ready" >/dev/null; then
    break
  fi
  sleep 0.5
done
curl --fail --silent "http://127.0.0.1:${loki_port}/ready" >/dev/null

start_alloy() {
  docker run -d \
    --name "$alloy_name" \
    --network "$network_name" \
    --user 473:473 \
    --group-add "$(getent group systemd-journal | cut -d: -f3)" \
    --read-only \
    --cap-drop ALL \
    --security-opt no-new-privileges:true \
    -v "$repo_dir/logging/alloy/config.alloy:/etc/alloy/config.alloy:ro" \
    -v /var/log/journal:/var/log/journal:ro \
    -v /run/log/journal:/run/log/journal:ro \
    -v /etc/machine-id:/etc/machine-id:ro \
    -v "$cursor_volume:/var/lib/alloy" \
    "$alloy_image" \
    run \
    --server.http.listen-addr=0.0.0.0:12345 \
    --storage.path=/var/lib/alloy/data \
    /etc/alloy/config.alloy >/dev/null
  for _ in $(seq 1 40); do
    if [ "$(docker inspect "$alloy_name" --format '{{.State.Running}}' 2>/dev/null || true)" = "true" ]; then
      sleep 1
      return 0
    fi
    sleep 0.25
  done
  docker logs "$alloy_name" >&2
  return 1
}

query_count() {
  local response
  response="$(
    curl --fail --silent --get \
      --data-urlencode "query=count_over_time({project=\"${sentinel_project}\",service=\"${sentinel_service}\"} |= \"${sentinel_text}\" [10m])" \
      "http://127.0.0.1:${loki_port}/loki/api/v1/query"
  )"
  node -e '
    const payload = JSON.parse(process.argv[1]);
    const values = payload?.data?.result ?? [];
    const count = values.reduce((sum, item) => sum + Number(item?.value?.[1] ?? 0), 0);
    process.stdout.write(String(count));
  ' "$response"
}

wait_for_count() {
  local expected="$1"
  local count=0
  for _ in $(seq 1 60); do
    count="$(query_count)"
    if [ "$count" -eq "$expected" ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "expected Loki sentinel count ${expected}, got ${count}" >&2
  docker logs "$alloy_name" >&2 || true
  docker logs "$loki_name" >&2 || true
  return 1
}

emit_sentinel() {
  docker run --rm \
    --log-driver journald \
    --log-opt "labels=com.docker.compose.service,com.docker.compose.project" \
    --label "com.docker.compose.service=${sentinel_service}" \
    --label "com.docker.compose.project=${sentinel_project}" \
    "$sentinel_image" \
    echo "$sentinel_text" >/dev/null
}

start_alloy
emit_sentinel
wait_for_count 1

docker rm -f "$alloy_name" >/dev/null
start_alloy
sleep 3
wait_for_count 1

emit_sentinel
wait_for_count 2
echo "Alloy cutover restart preserved cursor and compatible Loki labels."
