#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d "${repo_dir}/.test-alert-receiver.XXXXXX")"
port="$((18080 + ($$ % 1000)))"
receiver_pid=""
receiver_token="test-receiver-token"

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
  ALERT_RECEIVER_TOKEN="$receiver_token" \
  node --import tsx src/workers/alert-receiver.ts
) >"$tmp_dir/receiver.log" 2>&1 &
receiver_pid="$!"

for _ in $(seq 1 40); do
  if curl --silent "http://127.0.0.1:${port}/healthz" >/dev/null; then
    break
  fi
  sleep 0.25
done

# Liveness stays 200 while the receiver reports degraded archive-only delivery.
health_code="$(curl --silent --output "$tmp_dir/health.json" --write-out '%{http_code}' "http://127.0.0.1:${port}/healthz")"
test "$health_code" = 200
node -e '
  const health = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
  if (health.status !== "degraded" || !health.detail.includes("archive-only")) {
    throw new Error(`unexpected archive-only liveness response: ${JSON.stringify(health)}`);
  }
' "$tmp_dir/health.json"

# Readiness fails in archive-only mode so Compose never starts Alertmanager.
ready_code="$(curl --silent --output "$tmp_dir/ready.json" --write-out '%{http_code}' "http://127.0.0.1:${port}/readyz")"
test "$ready_code" = 503
node -e '
  const ready = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
  if (ready.status !== "degraded" || !ready.detail.includes("archive-only")) {
    throw new Error(`unexpected archive-only readiness response: ${JSON.stringify(ready)}`);
  }
' "$tmp_dir/ready.json"

# Archive-only POSTs must archive the receipt but refuse to acknowledge delivery:
# a 503 with Retry-After: 5 makes the producer retry instead of dropping the alert.
post_alert() {
  local payload="$1"
  local status
  status="$(curl --silent \
    --dump-header "$tmp_dir/post.headers" \
    --output "$tmp_dir/post.json" \
    --write-out '%{http_code}' \
    -H 'content-type: application/json' \
    -H "authorization: Bearer ${receiver_token}" \
    --data "$payload" \
    "http://127.0.0.1:${port}/alerts")"
  if [ "$status" != "503" ]; then
    echo "expected 503 in archive-only mode, got $status" >&2
    exit 1
  fi
  if ! grep -qi '^retry-after: 5' "$tmp_dir/post.headers"; then
    echo "archive-only 503 is missing Retry-After: 5" >&2
    exit 1
  fi
}

post_alert '{"kind":"alert","service":"meshcore-analytics","check":"dependency_readiness","detail":"test"}'
post_alert '{"kind":"recovery","service":"meshcore-analytics","check":"dependency_readiness","detail":"test"}'

# Prometheus exposition must be scrapeable even in archive-only mode.
metrics_code="$(curl --silent --output "$tmp_dir/metrics.txt" --write-out '%{http_code}' "http://127.0.0.1:${port}/metrics")"
test "$metrics_code" = 200
for metric in \
  'meshcore_alert_receiver_pending_count 0' \
  'meshcore_alert_receiver_dead_letter_count 0' \
  'meshcore_alert_receiver_queue_oldest_age_seconds 0' \
  'meshcore_alert_receiver_last_success_timestamp_seconds 0' \
  'meshcore_alert_receiver_enqueued_total 0' \
  'meshcore_alert_receiver_forwarded_total 0' \
  'meshcore_alert_receiver_failed_total 0' \
  'meshcore_alert_receiver_dead_lettered_total 0'; do
  if ! grep -q "^${metric}$" "$tmp_dir/metrics.txt"; then
    echo "missing metrics line: ${metric}" >&2
    exit 1
  fi
done
if ! grep -q '^# TYPE meshcore_alert_receiver_forwarded_total counter$' "$tmp_dir/metrics.txt"; then
  echo "metrics exposition is missing counter TYPE metadata" >&2
  exit 1
fi

# Unauthenticated posts must be refused and must not persist a receipt.
unauth_status="$(curl --silent -o /dev/null -w '%{http_code}' \
  -H 'content-type: application/json' \
  --data '{"kind":"alert","service":"forged","check":"x","detail":"forged"}' \
  "http://127.0.0.1:${port}/alerts")"
if [ "$unauth_status" != "401" ]; then
  echo "expected 401 for unauthenticated alert post, got $unauth_status" >&2
  exit 1
fi

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

echo "Archive-only firing and recovery were archived and reported retryable 503s; metrics exposition validated."
