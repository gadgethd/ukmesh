#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
replace_script="${script_dir}/replace-container.sh"
source_revision="0123456789abcdef0123456789abcdef01234567"
desired_image="registry.example/meshcore-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
prior_image="registry.example/meshcore-backend@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

test -x "$replace_script"
for command in jq openssl sha256sum; do
  command -v "$command" >/dev/null
done

write_mock_commands() {
  local fake_bin="$1"

cat >"${fake_bin}/cosign" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_COSIGN_LOG"
test "${1:-}" = "verify"
exit 0
EOF

  cat >"${fake_bin}/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-} ${2:-}" in
  "rev-parse HEAD")
    printf '%s\n' "$MOCK_SOURCE_REVISION"
    ;;
  "diff --quiet"|"diff --cached")
    exit 0
    ;;
  *)
    printf 'unexpected git invocation: %s\n' "$*" >&2
    exit 90
    ;;
esac
EOF

  cat >"${fake_bin}/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '{"status":"ready"}\n'
EOF

  cat >"${fake_bin}/openssl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF

  cat >"${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$MOCK_DOCKER_LOG"

command_name="${1:-}"
shift || true
case "$command_name" in
  compose)
    project_name=''
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --project-name)
          project_name="$2"
          shift 2
          ;;
        --project-directory|-f)
          shift 2
          ;;
        config)
          if [ "${2:-}" = "--services" ]; then
            printf '%s\n' backend db-migrate timescaledb app-ukmesh
            if [ "${MOCK_ADOPTION_MODE:-valid}" = long_service_list ]; then
              # More than a pipe buffer: an early grep -q exit must not turn
              # Compose's SIGPIPE into an incorrect unknown-service rejection.
              for ((index = 0; index < 10000; index++)); do printf 'fixture-service-%s\n' "$index"; done
            fi
          elif [ "${2:-}" != "-q" ]; then
            printf 'services: {}\n'
          fi
          exit 0
          ;;
        ps)
          shift
          if [ "${1:-}" != "-q" ]; then
            printf 'unexpected compose ps invocation: %s\n' "$*" >&2
            exit 92
          fi
          service="${2:-}"
          if [ "$service" = "timescaledb" ] || [ "$project_name" = 'meshcore-infra' ]; then
            printf 'infra-timescaledb\n'
            exit 0
          fi
          mode=valid
          if [ "$service" = "$MOCK_ADOPTION_SERVICE" ]; then mode="$MOCK_ADOPTION_MODE"; fi
          case "$mode" in
            no_container) ;;
            multiple_containers) printf 'current-%s\nother-%s\n' "$service" "$service" ;;
            *) printf 'current-%s\n' "$service" ;;
          esac
          exit 0
          ;;
        run)
          test "${*: -1}" = "db-migrate"
          exit 0
          ;;
        exec)
          if [[ "$*" == *"timescaledb"* ]]; then
            printf '30\n'
          elif [[ "$*" == *"/readyz"* ]]; then
            printf '{"status":"ready"}\n'
          elif [[ "$*" == *"/metrics"* ]]; then
            printf 'meshcore_process_start_time_seconds 1\n'
          else
            printf 'unexpected docker compose exec invocation: %s\n' "$*" >&2
            exit 91
          fi
          exit 0
          ;;
        up)
          if [ "${MOCK_FAIL_DESIRED_DEPLOY:-false}" = "true" ] \
            && [ "${BACKEND_IMAGE:-}" = "$MOCK_DESIRED_IMAGE" ]; then
            exit 1
          fi
          exit 0
          ;;
        *)
          printf 'unexpected docker compose invocation: %s\n' "$*" >&2
          exit 92
          ;;
      esac
    done
    ;;
  pull)
    ;;
  image)
    test "${1:-}" = "inspect"
    printf '%s\n' "$MOCK_SOURCE_REVISION"
    ;;
  inspect)
    case "$*" in
      *'.Config.Image'*)
        printf '%s\n' "$MOCK_PRIOR_IMAGE"
        ;;
      *'.Config.Labels'*)
        service="${1#current-}"
        mode=valid
        if [ "$service" = "$MOCK_ADOPTION_SERVICE" ]; then mode="$MOCK_ADOPTION_MODE"; fi
        if [ "$mode" = malformed_labels ]; then printf 'invalid JSON\n'; exit 0; fi
        jq -n --arg directory "$MOCK_PROJECT_DIR" --arg mode "$mode" --arg service "$service" '
          {
            "com.docker.compose.project": "meshcore-analytics",
            "com.docker.compose.service": $service,
            "com.docker.compose.project.working_dir": $directory,
            "com.docker.compose.project.config_files": ($directory + "/docker-compose.yml")
          }
          | if $mode == "empty" then {}
            elif $mode == "missing_config" then .["com.docker.compose.project.config_files"] = ""
            elif $mode == "wrong_directory" then .["com.docker.compose.project.working_dir"] = "/wrong-source"
            elif $mode == "wrong_project" then .["com.docker.compose.project"] = "wrong-project"
            elif $mode == "wrong_service" then .["com.docker.compose.service"] = "wrong-service"
            elif $mode == "null_labels" then null
            elif $mode == "overlay_only" then .["com.docker.compose.project.config_files"] = ($directory + "/docker-compose.live.yml")
            elif $mode == "overlay" then .["com.docker.compose.project.config_files"] += ("," + $directory + "/docker-compose.live.yml")
            else . end'
        ;;
      *'.Config.Env'*)
        printf '%s\n' \
          'DATABASE_URL=postgresql://meshcore:fixture@timescaledb:5432/meshcore' \
          'REDIS_URL=redis://redis:6379'
        ;;
      *'.NetworkSettings.Networks'*)
        printf 'meshcore-net\n'
        ;;
      *'.State.Status'*)
        printf 'running\n'
        ;;
      *'.State.Running'*)
        printf 'true\n'
        ;;
      *'.State.Health'*)
        printf 'healthy\n'
        ;;
      *)
        printf 'unexpected docker inspect invocation: %s\n' "$*" >&2
        exit 93
        ;;
    esac
    ;;
  run)
    printf 'compat-container-id\n'
    ;;
  exec)
    if [[ "$*" == *"infra-timescaledb"*"psql"* ]]; then
      printf '30\n'
      exit 0
    fi
    if [ "${MOCK_COMPAT_READY:-true}" != "true" ]; then
      exit 1
    fi
    if [[ "$*" == *"/readyz"* ]]; then
      printf '{"status":"ready"}\n'
    elif [[ "$*" == *"/api/stats"* ]]; then
      printf '{"totalNodes":1}\n'
    else
      printf 'unexpected docker exec invocation: %s\n' "$*" >&2
      exit 94
    fi
    ;;
  rm|logs)
    ;;
  *)
    printf 'unexpected docker invocation: %s\n' "$command_name $*" >&2
    exit 95
    ;;
esac
EOF

  chmod 0755 \
    "${fake_bin}/cosign" \
    "${fake_bin}/curl" \
    "${fake_bin}/docker" \
    "${fake_bin}/git" \
    "${fake_bin}/openssl"
}

run_case() {
  local case_name="$1"
  local compat_ready="$2"
  local fail_desired_deploy="$3"
  local expected_status="$4"
  local expected_up_count="$5"
  local signature_mode="$6"
  local adoption_mode="${7:-valid}"
  local service="${8:-backend}"
  local adoption_service="${9:-$service}"
  local test_root
  test_root="$(mktemp -d)"
  trap 'rm -rf -- "$test_root"' RETURN

  mkdir -p \
    "${test_root}/project/scripts/lib" \
    "${test_root}/infra" \
    "${test_root}/fake-bin" \
    "${test_root}/releases"
  cp "$replace_script" "${test_root}/project/scripts/replace-container.sh"
  if [ -f "${script_dir}/check-compose-adoption.sh" ]; then
    cp "${script_dir}/check-compose-adoption.sh" "${test_root}/project/scripts/check-compose-adoption.sh"
  fi
  cp "${script_dir}/lib/infra-compose.sh" "${test_root}/project/scripts/lib/infra-compose.sh"
  chmod 0755 "${test_root}/project/scripts/replace-container.sh"
  printf 'services: {}\n' >"${test_root}/project/docker-compose.yml"
  printf 'services: {}\n' >"${test_root}/infra/docker-compose.yml"

  local now
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -n \
    --arg now "$now" \
    '{
      format:"meshcore-restore-receipt-v1",
      receipt_id:"restore-fixture",
      status:"verified",
      backup_completed_at:$now,
      restore_verified_at:$now,
      datasets:["analytics","owner_auth","mosquitto","redis","configuration"]
    }' >"${test_root}/restore-receipt.json"
  : >"${test_root}/restore-receipt.json.sig"
  : >"${test_root}/receipt-verify.pem"
  : >"${test_root}/cosign.pub"
  : >"${test_root}/cosign.log"
  : >"${test_root}/docker.log"
  write_mock_commands "${test_root}/fake-bin"

  local cosign_public_key="${test_root}/cosign.pub"
  local cosign_identity_regexp=""
  local cosign_oidc_issuer=""
  if [ "$signature_mode" = "keyless" ]; then
    cosign_public_key=""
    cosign_identity_regexp='^https://github.com/example/meshcore/.github/workflows/release.yml@'
    cosign_oidc_issuer='https://token.actions.githubusercontent.com'
  fi

  set +e
  PATH="${test_root}/fake-bin:${PATH}" \
    MOCK_SOURCE_REVISION="$source_revision" \
    MOCK_PROJECT_DIR="${test_root}/project" \
    MOCK_ADOPTION_MODE="$adoption_mode" \
    MOCK_ADOPTION_SERVICE="$adoption_service" \
    MOCK_DESIRED_IMAGE="$desired_image" \
    MOCK_PRIOR_IMAGE="$prior_image" \
    MOCK_COMPAT_READY="$compat_ready" \
    MOCK_FAIL_DESIRED_DEPLOY="$fail_desired_deploy" \
    MOCK_DOCKER_LOG="${test_root}/docker.log" \
    MOCK_COSIGN_LOG="${test_root}/cosign.log" \
    COSIGN_PUBLIC_KEY="$cosign_public_key" \
    COSIGN_CERTIFICATE_IDENTITY_REGEXP="$cosign_identity_regexp" \
    COSIGN_CERTIFICATE_OIDC_ISSUER="$cosign_oidc_issuer" \
    RESTORE_RECEIPT_PATH="${test_root}/restore-receipt.json" \
    RESTORE_RECEIPT_VERIFY_KEY="${test_root}/receipt-verify.pem" \
    RELEASE_STATUS_DIR="${test_root}/releases" \
    MESHCORE_INFRA_PROJECT_DIR="${test_root}/infra" \
    COMPATIBILITY_TIMEOUT_SECONDS=1 \
    "${test_root}/project/scripts/replace-container.sh" \
      "$service" \
      "--image=${desired_image}" \
      "--backend-image=${desired_image}" \
      "--source-revision=${source_revision}" \
      >"${test_root}/stdout.log" 2>"${test_root}/stderr.log"
  local status=$?
  set -e

  if [ "$adoption_mode" != "valid" ] && [ "$adoption_mode" != "overlay" ] && [ "$adoption_mode" != "long_service_list" ]; then
    if [ "$status" -ne 65 ]; then
      printf '%s: expected Compose adoption rejection (65), got %s\n' "$case_name" "$status" >&2
      return 1
    fi
    grep -q 'not Compose-adoptable' "${test_root}/stderr.log"
    grep -q "service ${adoption_service} is not Compose-adoptable" "${test_root}/stderr.log"
    test ! -s "${test_root}/cosign.log"
    if grep -Eq '^(pull |compose .* (up|run) )' "${test_root}/docker.log"; then
      echo 'adoption guard allowed a mutation before rejection' >&2
      return 1
    fi
    test -z "$(find "${test_root}/releases" -type f -print -quit)"
    printf '%s passed\n' "$case_name"
    return 0
  fi
  if [ "$status" -eq 0 ]; then
    printf '%s: expected a controlled failure\n' "$case_name" >&2
    return 1
  fi

  local release_status
  release_status="$(
    find "${test_root}/releases" -maxdepth 1 -type f -name '*.json' \
      -print -quit
  )"
  if [ -z "$release_status" ]; then
    printf '%s: expected a controlled release receipt, got exit %s\n' "$case_name" "$status" >&2
    cat "${test_root}/stderr.log" >&2
    return 1
  fi
  test "$(jq -r '.status' "$release_status")" = "$expected_status"
  test "$(jq -r '.schema_version' "$release_status")" = "30"
  test "$(jq -r '.prior_image' "$release_status")" = "$prior_image"

  local up_count
  up_count="$(
    grep -Ec '^compose --project-name meshcore-analytics up ' \
      "${test_root}/docker.log" || true
  )"
  test "$up_count" -eq "$expected_up_count"
  grep -Eq -- \
    '^run .* --volumes-from current-backend:ro( |$)' \
    "${test_root}/docker.log"
  if grep -qF 'ps -q timescaledb' "${test_root}/docker.log"; then
    local resolver_line migration_line
    resolver_line="$(grep -nF 'ps -q timescaledb' "${test_root}/docker.log" | head -1 | cut -d: -f1)"
    migration_line="$(grep -nF 'compose --project-name meshcore-analytics run --rm db-migrate' "${test_root}/docker.log" | head -1 | cut -d: -f1)"
    test -n "$migration_line"
    test "$resolver_line" -lt "$migration_line"
    grep -Fq 'exec -i infra-timescaledb psql -U meshcore -d meshcore -Atc' "${test_root}/docker.log"
  fi
  if [ "$signature_mode" = "keyless" ]; then
    grep -q -- '--certificate-identity-regexp' "${test_root}/cosign.log"
    grep -q -- '--certificate-oidc-issuer' "${test_root}/cosign.log"
    if grep -q -- '--key' "${test_root}/cosign.log"; then
      echo "keyless verification unexpectedly used a public key" >&2
      return 1
    fi
  else
    grep -q -- '--key' "${test_root}/cosign.log"
  fi
  printf '%s passed\n' "$case_name"
}

run_case \
  failed_deploy_rolls_back \
  true \
  true \
  rolled_back \
  2 \
  public-key
run_case \
  incompatible_schema_stops_before_deploy \
  false \
  false \
  stopped \
  0 \
  keyless

for adoption_mode in empty missing_config wrong_directory wrong_project wrong_service no_container multiple_containers null_labels malformed_labels overlay_only; do
  run_case "adoption_${adoption_mode}_stops_before_mutation" true false stopped 0 public-key "$adoption_mode"
done

run_case overlay_labels_remain_adoptable false false stopped 0 keyless overlay
run_case long_service_list_remains_adoptable false false stopped 0 keyless long_service_list
run_case invalid_frontend_stops_before_mutation true false stopped 0 public-key empty app-ukmesh
run_case valid_frontend_with_invalid_backend_stops_before_mutation true false stopped 0 public-key empty app-ukmesh backend

printf 'replace-container rollback, compatibility and Compose adoption drills passed (16/16)\n'
