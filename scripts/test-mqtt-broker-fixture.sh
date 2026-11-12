#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
fixture_dir=$(mktemp -d)
fixture_name="ukmesh-broker-limits-$$"
cleanup() {
  docker rm -f "$fixture_name" >/dev/null 2>&1 || true
  rm -rf -- "$fixture_dir"
}
trap cleanup EXIT
chmod 755 "$fixture_dir"
cp "$repo/mosquitto/capacity-limits.conf.example" "$fixture_dir/mosquitto.conf"
cat >> "$fixture_dir/mosquitto.conf" <<'CONF'
listener 1883
allow_anonymous true
queue_qos0_messages true
sys_interval 1
CONF
chmod 644 "$fixture_dir/mosquitto.conf"
docker run -d --name "$fixture_name" --memory 128m -p 127.0.0.1::1883 \
  -v "$fixture_dir/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" \
  eclipse-mosquitto@sha256:9cfdd46ad59f3e3e5f592f6baf57ab23e1ad00605509d0f5c1e9b179c5314d87 >/dev/null
ready=false
for ((attempt=0; attempt<30; attempt++)); do
  if docker exec "$fixture_name" mosquitto_pub -h 127.0.0.1 -t owned/ready -m ready; then ready=true; break; fi
  sleep 1
done
[[ $ready == true ]] || { echo 'owned broker fixture did not become ready' >&2; exit 1; }
address=$(docker port "$fixture_name" 1883/tcp)
cd "$repo/backend"
TEST_MQTT_BROKER_URL="mqtt://$address" node --import tsx --test src/mqtt/brokerLimits.integration.test.ts
