#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
port="$((18080 + ($$ % 1000)))"
receiver_pid=""

cleanup() {
  if [ -n "$receiver_pid" ]; then
    kill "$receiver_pid" >/dev/null 2>&1 || true
    wait "$receiver_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

(
  cd "$repo_dir/backend"
  ALERT_RECEIVER_PORT="$port" \
  ALERT_RECEIVER_PATH="$tmp_dir/alerts.jsonl" \
  node --import tsx src/workers/alert-receiver.ts
) >"$tmp_dir/receiver.log" 2>&1 &
receiver_pid="$!"

for _ in $(seq 1 40); do
  if curl --fail --silent "http://127.0.0.1:${port}/healthz" >/dev/null; then
    break
  fi
  sleep 0.25
done
curl --fail --silent "http://127.0.0.1:${port}/healthz" >/dev/null

curl --fail --silent \
  -H 'content-type: application/json' \
  --data '{"kind":"alert","service":"meshcore-analytics","check":"dependency_readiness","detail":"test"}' \
  "http://127.0.0.1:${port}/alerts" >/dev/null
curl --fail --silent \
  -H 'content-type: application/json' \
  --data '{"kind":"recovery","service":"meshcore-analytics","check":"dependency_readiness","detail":"test"}' \
  "http://127.0.0.1:${port}/alerts" >/dev/null

node -e '
  const fs = require("fs");
  const records = fs.readFileSync(process.argv[1], "utf8").trim().split("\n").map(JSON.parse);
  if (records.length !== 2) throw new Error(`expected two receipts, got ${records.length}`);
  if (records[0].status !== "firing" || records[1].status !== "recovery") {
    throw new Error(`unexpected receipt states: ${records.map((r) => r.status).join(",")}`);
  }
  if (JSON.stringify(records).includes("\"detail\"")) {
    throw new Error("receipt persisted unbounded alert detail");
  }
' "$tmp_dir/alerts.jsonl"

echo "Synthetic firing and recovery reached the bounded test receiver."
