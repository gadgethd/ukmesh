#!/usr/bin/env bash
# Own a disposable loopback-only TimescaleDB. Never use application Compose,
# DATABASE_URL, an existing container, or a persistent data volume.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_id=""
cleanup() {
  if [[ -n "$fixture_id" ]]; then
    docker stop --time 10 "$fixture_id" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
fixture_id="$(docker run --detach --rm \
  --shm-size=1g \
  --name "ukmesh-privacy-test-${UID}-$$" \
  --label ukmesh.test=privacy-fence \
  --publish 127.0.0.1::5432 \
  --tmpfs /var/lib/postgresql/data:rw \
  --env POSTGRES_USER=privacy_fixture \
  --env POSTGRES_PASSWORD=privacy-fence-local-test-only \
  --env POSTGRES_DB=privacy_fence_test \
  timescale/timescaledb@sha256:22e8a5ae7aef121d1537afe946dd7cc5deeeb63ab36ce19849d671bd3b663509)"
ready=false
for ((attempt = 0; attempt < 60; attempt++)); do
  if docker exec "$fixture_id" pg_isready -h 127.0.0.1 -U privacy_fixture -d privacy_fence_test >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'local privacy fixture did not become ready' >&2; exit 1; }
binding="$(docker port "$fixture_id" 5432/tcp)"
fixture_port="${binding##*:}"
[[ "$binding" == "127.0.0.1:${fixture_port}" && "$fixture_port" =~ ^[0-9]+$ ]] || exit 1
export TEST_PRIVACY_DATABASE_URL="postgresql://privacy_fixture:privacy-fence-local-test-only@127.0.0.1:${fixture_port}/privacy_fence_test"
cd "$repo_root/backend"
npm run test:privacy-integration
