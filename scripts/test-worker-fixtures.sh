#!/usr/bin/env bash
# Tests own both temporary services; no project .env or retained database is read.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
worker_image=${RF_WORKER_TEST_IMAGE:-meshcore-viewshed-worker:test}
fixture="ukmesh-worker-fixture-$$"
redis_name="$fixture-redis"
pg_name="$fixture-pg"
cleanup() {
  docker rm -f "$redis_name" "$pg_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
password=owned-worker-test-password
pg_image=postgres@sha256:5db6dac2ef5dcff512f4f00a009c3c332b9f2f28f24277175bf3e4a3d02ef83a
if ! docker image inspect "$pg_image" >/dev/null 2>&1; then
  python3 "$repo/scripts/ci-retry.py" pull docker pull "$pg_image"
fi
docker run -d --name "$redis_name" --memory 128m -p 127.0.0.1::6379 \
  redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065 \
  redis-server --requirepass "$password" --maxmemory-policy noeviction >/dev/null
docker run -d --name "$pg_name" --memory 512m -p 127.0.0.1::5432 \
  -e POSTGRES_PASSWORD="$password" \
  "$pg_image" >/dev/null
ready=false
for ((attempt=0; attempt<60; attempt++)); do
  if docker exec "$redis_name" redis-cli -a "$password" --no-auth-warning ping >/dev/null 2>&1 \
    && docker exec "$pg_name" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; then
    ready=true; break
  fi
  sleep 1
done
[[ $ready == true ]] || { echo 'owned worker fixtures did not become ready' >&2; exit 1; }
redis_port=$(docker port "$redis_name" 6379/tcp)
pg_port=$(docker port "$pg_name" 5432/tcp)
docker run --rm --network host --entrypoint python \
  -e TEST_REDIS_URL="redis://$redis_port" -e TEST_REDIS_PASSWORD="$password" \
  -e TEST_LINK_DATABASE_URL="postgresql://postgres:$password@$pg_port/postgres" \
  "$worker_image" -m unittest discover -s /app/tests -v
docker run --rm --network none --entrypoint python \
  "$worker_image" /app/tests/benchmark_rf.py --rays 12 --steps 1000
docker run --rm --network none --entrypoint python \
  "$worker_image" /app/tests/benchmark_rf_radial.py --rays 72 --steps 500 --repeats 1
