#!/usr/bin/env bash
# Exercise both shipped runtimes against one exclusively owned Redis instance.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
backend_image=${BACKEND_CONTRACT_IMAGE:-meshcore-analytics-backend:local}
worker_image=${RF_WORKER_TEST_IMAGE:-meshcore-viewshed-worker:test}
fixture="ukmesh-link-contract-$$"
trap 'docker rm -f "$fixture" >/dev/null 2>&1 || true' EXIT
password=owned-link-contract-password
docker run -d --name "$fixture" --memory 128m -p 127.0.0.1::6379 \
  redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065 \
  redis-server --requirepass "$password" --maxmemory-policy noeviction >/dev/null
ready=false
for ((attempt=0; attempt<30; attempt++)); do
  if docker exec "$fixture" redis-cli -a "$password" --no-auth-warning ping >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ $ready == true ]] || { echo 'owned link fixture did not become ready' >&2; exit 1; }
address=$(docker port "$fixture" 6379/tcp)
producer() {
  docker run --rm --network host --entrypoint node \
    -e TEST_REDIS_URL="redis://$address" -e TEST_REDIS_PASSWORD="$password" \
    -v "$repo/scripts/fixtures/link-queue-producer.mjs:/app/contract-probe.mjs:ro" \
    "$backend_image" /app/contract-probe.mjs "$1"
}
consumer() {
  docker run --rm --network host --entrypoint python \
    -e TEST_REDIS_URL="redis://$address" -e TEST_REDIS_PASSWORD="$password" \
    -v "$repo/scripts/fixtures/link-queue-consumer.py:/app/contract-probe.py:ro" \
    "$worker_image" /app/contract-probe.py "$1"
}
producer admit-observe
consumer ack-observe
producer verify-observe
consumer admit-physical
producer verify-physical
consumer ack-physical
producer admit-rebuild
consumer ack-rebuild
producer verify-drained
for value in '' '1.9' 'garbage' '9999999'; do
  docker run --rm --network host --entrypoint node \
    -e TEST_REDIS_URL="redis://$address" -e TEST_REDIS_PASSWORD="$password" \
    -e LINK_QUEUE_V3_MAX_JOBS="$value" \
    -v "$repo/scripts/fixtures/link-queue-producer.mjs:/app/contract-probe.mjs:ro" \
    "$backend_image" /app/contract-probe.mjs limits
done
for value in '' '1.9' 'garbage'; do
  if docker run --rm --network none --entrypoint python -e LINK_QUEUE_V3_MAX_JOBS="$value" \
    "$worker_image" -c 'import link_queue_v3' >/dev/null 2>&1; then
    echo 'Python invalid queue limit unexpectedly succeeded' >&2; exit 1
  fi
done
printf 'Cross-language link-v3 contract passed.\n'
