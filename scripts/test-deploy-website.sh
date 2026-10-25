#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_root="$(mktemp -d "${repo_dir}/.test-deploy-website.XXXXXX")"
trap 'rm -rf -- "$tmp_root"' EXIT
project="$tmp_root/project"
fake_bin="$tmp_root/fake-bin"
mkdir -p "$project/scripts" "$fake_bin"
cp "$repo_dir/scripts/deploy-website.sh" "$project/scripts/deploy-website.sh"
chmod 0755 "$project/scripts/deploy-website.sh"
cat >"$project/scripts/check-website-drift.sh" <<'EOF'
#!/usr/bin/env bash
echo 'NO DRIFT — mocked manifest check'
exit 0
EOF
chmod 0755 "$project/scripts/check-website-drift.sh"
touch "$project/docker-compose.yml" "$project/docker-compose.live.yml"

cat >"$fake_bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_DOCKER_LOG"
case "${1:-}" in
  compose)
    shift
    while [ "${1:-}" = "-f" ]; do shift 2; done
    action="${1:-}"
    shift || true
    case "$action" in
      up)
        pin="$(sed -n 's/^WEBSITE_IMAGE=//p' .env)"
        if [ "${MOCK_FAIL_NEW_UP:-false}" = "true" ] && [ "$pin" = "$MOCK_NEW_DIGEST" ]; then
          exit 1
        fi
        ;;
      ps)
        printf 'website-container\n'
        ;;
      *)
        echo "unexpected docker compose action: $action" >&2
        exit 90
        ;;
    esac
    ;;
  inspect)
    case "$*" in
      *'{{.Id}}'*) printf '%s\n' "$MOCK_NEW_DIGEST" ;;
      *'{{.State.Running}}'*) printf 'true\n' ;;
      *'{{if .State.Health}}'*) printf 'healthy\n' ;;
      *) echo "unexpected docker inspect: $*" >&2; exit 91 ;;
    esac
    ;;
  run)
    case "$*" in
      *'grep -oE'*)
        if [ "${MOCK_EMPTY_ASSETS:-false}" = "true" ]; then exit 1; fi
        printf 'assets/app.js\n'
        ;;
      *'sha256sum /usr/share/nginx/html/assets/app.js'*)
        printf '%s  /usr/share/nginx/html/assets/app.js\n' "$MOCK_BUNDLE_HASH"
        ;;
      *) echo "unexpected docker run: $*" >&2; exit 92 ;;
    esac
    ;;
  *) echo "unexpected docker invocation: $*" >&2; exit 93 ;;
esac
EOF

cat >"$fake_bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
url="${*: -1}"
case "$url" in
  */assets/app.js)
    if [ "${MOCK_BAD_BUNDLE:-false}" = "true" ]; then printf 'wrong bundle'; else printf 'bundle'; fi
    ;;
  */) printf 'index' ;;
  *) echo "unexpected curl URL: $url" >&2; exit 94 ;;
esac
EOF

cat >"$fake_bin/sleep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod 0755 "$fake_bin/docker" "$fake_bin/curl" "$fake_bin/sleep"

old_digest='sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
new_digest='sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
bundle_hash="$(printf 'bundle' | sha256sum | awk '{print $1}')"

reset_fixture() {
  printf 'WEBSITE_IMAGE=%s\n' "${1-$old_digest}" >"$project/.env"
  : >"$tmp_root/docker.log"
}

run_deploy() {
  (
    cd "$project"
    PATH="$fake_bin:$PATH" \
      MOCK_DOCKER_LOG="$tmp_root/docker.log" \
      MOCK_NEW_DIGEST="$new_digest" \
      MOCK_BUNDLE_HASH="$bundle_hash" \
      MOCK_FAIL_NEW_UP="${MOCK_FAIL_NEW_UP:-false}" \
      MOCK_BAD_BUNDLE="${MOCK_BAD_BUNDLE:-false}" \
      MOCK_EMPTY_ASSETS="${MOCK_EMPTY_ASSETS:-false}" \
      scripts/deploy-website.sh fixture-image website-ukmesh
  )
}

reset_fixture
MOCK_FAIL_NEW_UP=false MOCK_BAD_BUNDLE=false run_deploy >/dev/null
test "$(sed -n 's/^WEBSITE_IMAGE=//p' "$project/.env")" = "$new_digest"

reset_fixture
if MOCK_FAIL_NEW_UP=false MOCK_BAD_BUNDLE=true run_deploy >/dev/null 2>&1; then
  echo 'mismatched served bundle unexpectedly reported success' >&2
  exit 1
fi
test "$(sed -n 's/^WEBSITE_IMAGE=//p' "$project/.env")" = "$old_digest"
test "$(grep -c 'compose .* up ' "$tmp_root/docker.log")" -eq 2

reset_fixture ''
if MOCK_FAIL_NEW_UP=true MOCK_BAD_BUNDLE=false run_deploy >/dev/null 2>&1; then
  echo 'failed deployment with an empty prior pin unexpectedly reported success' >&2
  exit 1
fi
test "$(sed -n 's/^WEBSITE_IMAGE=//p' "$project/.env")" = ''
test "$(grep -c 'compose .* up ' "$tmp_root/docker.log")" -eq 2

reset_fixture
if MOCK_FAIL_NEW_UP=true MOCK_BAD_BUNDLE=false run_deploy >/dev/null 2>&1; then
  echo 'failed container recreation unexpectedly reported success' >&2
  exit 1
fi
test "$(sed -n 's/^WEBSITE_IMAGE=//p' "$project/.env")" = "$old_digest"
test "$(grep -c 'compose .* up ' "$tmp_root/docker.log")" -eq 2

reset_fixture
if MOCK_FAIL_NEW_UP=false MOCK_BAD_BUNDLE=false MOCK_EMPTY_ASSETS=true run_deploy >/dev/null 2>&1; then
  echo 'empty bundle list unexpectedly reported success' >&2
  exit 1
fi
test "$(sed -n 's/^WEBSITE_IMAGE=//p' "$project/.env")" = "$old_digest"
test "$(grep -c 'compose .* up ' "$tmp_root/docker.log")" -eq 2

echo 'Website deploy success, verification rollback, and recreate-failure rollback passed.'
