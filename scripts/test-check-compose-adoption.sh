#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
tmp_root="$(mktemp -d "${repo_dir}/.test-compose-adoption.XXXXXX")"
trap 'rm -rf -- "$tmp_root"' EXIT
project_dir="$tmp_root/project with spaces"
mkdir -p "$project_dir/scripts" "$tmp_root/fake-bin"
cp "$repo_dir/scripts/check-compose-adoption.sh" "$project_dir/scripts/"
chmod 0755 "$project_dir/scripts/check-compose-adoption.sh"

cat >"$tmp_root/fake-bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
test "${COMPOSE_PROJECT_NAME:-}" = operator.project
test "${COMPOSE_FILE:-}" = 'compose base.yml|compose live.yml'
test "${COMPOSE_PATH_SEPARATOR:-}" = '|'
jq -nc --args '$ARGS.positional' -- "$@" >>"$MOCK_DOCKER_LOG"
case "$1" in
  compose)
    test "$2" = --project-name && test "$3" = operator.project
    case "$4" in
      ps)
        test "$5" = -q && test "$6" = backend
        if [ "$MOCK_MODE" = compose_failure ]; then exit 1; fi
        if [ -f "$MOCK_REPAIRED" ]; then printf 'abc123\n'; exit 0; fi
        case "$MOCK_MODE" in
          orphan|custom_name|absent|name_failure|config_failure) ;;
          *) printf 'abc123\n' ;;
        esac
        ;;
      config)
        test "$5" = --format && test "$6" = json
        if [ "$MOCK_MODE" = config_failure ]; then exit 1; fi
        if [ "$MOCK_MODE" = custom_name ]; then
          printf '{"services":{"backend":{"container_name":"manual.backend"}}}\n'
        else
          printf '{"services":{"backend":{}}}\n'
        fi
        ;;
      up)
        test "${*:4}" = 'up -d --no-deps --no-build backend'
        touch "$MOCK_REPAIRED"
        ;;
      *) exit 90 ;;
    esac
    ;;
  ps)
    if [ "$MOCK_MODE" = name_failure ]; then exit 1; fi
    if [ "$MOCK_MODE" = custom_name ]; then
      test "${*:2}" = '--all --quiet --filter name=^/manual\.backend$'
    else
      test "${*:2}" = '--all --quiet --filter name=^/operator\.project-backend-1$'
    fi
    if [ "$MOCK_MODE" != absent ]; then printf 'deadbeef\n'; fi
    ;;
  inspect)
    test "$2" = abc123
    test "$3" = --format && test "$4" = '{{json .Config.Labels}}'
    if [ "$MOCK_MODE" = inspect_failure ]; then exit 1; fi
    if [ "$MOCK_MODE" = drift ] && [ ! -f "$MOCK_REPAIRED" ]; then
      printf '{}\n'
    else
      jq -nc --arg directory "$MOCK_PROJECT_DIR" '{
        "com.docker.compose.project":"operator.project",
        "com.docker.compose.service":"backend",
        "com.docker.compose.project.working_dir":$directory,
        "com.docker.compose.project.config_files":($directory + "/docker-compose.yml," + $directory + "/docker-compose.live.yml")
      }'
    fi
    ;;
  rm)
    test "$2" = -f && test "$3" = -- && test "$#" = 4
    case "$MOCK_MODE" in
      drift) test "$4" = abc123 ;;
      orphan|custom_name) test "$4" = deadbeef ;;
      *) exit 91 ;;
    esac
    ;;
  *) exit 92 ;;
esac
EOF
chmod 0755 "$tmp_root/fake-bin/docker"

export PATH="$tmp_root/fake-bin:$PATH"
export MOCK_PROJECT_DIR="$project_dir" MOCK_DOCKER_LOG="$tmp_root/docker.log" MOCK_REPAIRED="$tmp_root/repaired"
export COMPOSE_PROJECT_NAME=operator.project COMPOSE_FILE='compose base.yml|compose live.yml' COMPOSE_PATH_SEPARATOR='|'

cases=0
for mode in valid drift orphan custom_name absent compose_failure inspect_failure name_failure config_failure; do
  export MOCK_MODE="$mode"
  rm -f -- "$MOCK_REPAIRED"
  : >"$MOCK_DOCKER_LOG"
  status=0
  "$project_dir/scripts/check-compose-adoption.sh" backend >"$tmp_root/stdout" 2>"$tmp_root/stderr" || status=$?
  case "$mode" in
    valid) test "$status" -eq 0 ;;
    *_failure)
      test "$status" -eq 69
      grep -Fq 'cannot verify Compose adoption' "$tmp_root/stderr"
      # An unverified Docker/config error must not suggest removing a container.
      ! grep -Fq 'docker rm' "$tmp_root/stderr"
      ;;
    *)
      test "$status" -eq 65
      grep -Fq 'not Compose-adoptable' "$tmp_root/stderr"
      ;;
  esac
  # The guard itself must only inspect. The repair is tested separately below.
  jq -se 'all(.[]; .[0] == "inspect" or .[0] == "ps" or (.[0] == "compose" and (.[3] == "ps" or .[3] == "config")))' "$MOCK_DOCKER_LOG" >/dev/null
  case "$mode" in
    drift|orphan|custom_name|absent)
      repair="$(sed -n '/^  (cd /p' "$tmp_root/stderr")"
      test -n "$repair"
      if [ "$mode" = absent ]; then ! grep -Fq 'docker rm' "$tmp_root/stderr"; fi
      # Execute only against strict fake Docker. Unset Compose variables to
      # prove the pasted command restores project, overlay and separator itself.
      env -u COMPOSE_FILE -u COMPOSE_PROJECT_NAME -u COMPOSE_PATH_SEPARATOR \
        bash -c "$repair" >"$tmp_root/repair-stdout" 2>"$tmp_root/repair-stderr"
      test -f "$MOCK_REPAIRED"
      grep -Fq 'Compose adoption metadata verified for backend' "$tmp_root/repair-stdout"
      ;;
  esac
  printf 'compose adoption %s passed\n' "$mode"
  cases=$((cases + 1))
done
printf 'Compose adoption read-only checks and printed repair drills passed (%s/%s)\n' "$cases" "$cases"
