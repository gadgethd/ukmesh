#!/usr/bin/env bash
# Portable quality gate: ruff + pattern checks + pytest.
#
#   scripts/quality.sh              # lint changed files + patterns + tests
#   scripts/quality.sh --lint-only  # ruff only (used by CI on a diff)
#   scripts/quality.sh --patterns-only
#   scripts/quality.sh --test-only  # tests only
#   scripts/quality.sh --all        # lint every file (full audit; legacy debt will show)
#
# Environment:
#   BASE_REF   git ref to diff against (CI sets this: PR base, or HEAD~1 on push)
#   PYTHON     interpreter (default: first of .venv, $HOME/harness/.venv, python3 that has pytest)
#   TEST_DIR   directory to run pytest from (default: scripts/pytest-dir.txt if present, else repo root)
#   RUFF       ruff binary (default: PATH, then ~/harness/.venv/bin/ruff)
#   HCHECK     hcheck binary (default: PATH, then ~/harness/.venv/bin/hcheck)
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 2

REPO_ROOT=$(pwd)
has_pytest() { [ -n "${1:-}" ] && "$1" -c 'import pytest' >/dev/null 2>&1; }

# Resolved lazily, from the directory the tests actually run in: a repo may keep
# its virtualenv beside the package (gpu-lab-mcp: gateway/.venv) rather than at
# the root, and the dependencies live in that interpreter.
pick_python() {
  if [ -n "${PYTHON:-}" ]; then echo "$PYTHON"; return 0; fi
  local candidates=(.venv/bin/python venv/bin/python
                    "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/venv/bin/python"
                    python3 "$HOME/harness/.venv/bin/python")
  local candidate
  for candidate in "${candidates[@]}"; do
    if has_pytest "$candidate"; then echo "$candidate"; return 0; fi
  done
  return 1
}

if [ -z "${RUFF:-}" ]; then
  if command -v ruff >/dev/null 2>&1; then
    RUFF=ruff
  elif [ -x "$HOME/harness/.venv/bin/ruff" ]; then
    RUFF="$HOME/harness/.venv/bin/ruff"
  else
    RUFF=ruff
  fi
fi
if [ -z "${HCHECK:-}" ]; then
  if command -v hcheck >/dev/null 2>&1; then
    HCHECK=hcheck
  elif [ -x "$HOME/harness/.venv/bin/hcheck" ]; then
    HCHECK="$HOME/harness/.venv/bin/hcheck"
  else
    HCHECK=""
  fi
fi
BASE_REF="${BASE_REF:-}"
MODE="${1:-all}"

lint() {
  local files=""
  if [ "$MODE" = "--all" ]; then
    echo "== ruff (all files — legacy debt may appear) =="
    "$RUFF" check .
    return
  fi
  if [ -n "$BASE_REF" ] && git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
    files=$(git diff --name-only --diff-filter=ACMR "$BASE_REF"...HEAD -- '*.py' | tr '\n' ' ')
    echo "== ruff (changed vs $BASE_REF) =="
  else
    # Untracked files are part of the change: a new module that is not staged yet
    # must still be linted, or the first commit of a broken file passes the gate.
    files=$( { git diff --name-only --diff-filter=ACMR HEAD -- '*.py'; \
               git diff --name-only --diff-filter=ACMR --cached -- '*.py'; \
               git ls-files --others --exclude-standard -- '*.py'; } | sort -u | tr '\n' ' ')
    echo "== ruff (uncommitted + untracked changes) =="
  fi
  # A clean tree has no uncommitted changes, so falling back to the last commit
  # keeps this from being a gate that passes having checked nothing.
  if [ -z "${files// /}" ]; then
    if git rev-parse --verify --quiet HEAD~1 >/dev/null; then
      files=$(git diff --name-only --diff-filter=ACMR HEAD~1 HEAD -- '*.py' | tr '\n' ' ')
      if [ -n "${files// /}" ]; then
        echo "== ruff (tree clean; files changed by HEAD $(git rev-parse --short HEAD)) =="
      else
        echo "== ruff (tree clean; HEAD $(git rev-parse --short HEAD) changed no python files) =="
      fi
    else
      files=$(git ls-files '*.py' | tr '\n' ' ')
      echo "== ruff (tree clean, no parent commit; all tracked python files) =="
    fi
  fi
  if [ -z "${files// /}" ]; then
    echo "no python files in scope; nothing to lint"
    return 0
  fi
  # shellcheck disable=SC2086
  "$RUFF" check $files
}

patterns() {
  if [ -z "$HCHECK" ]; then
    echo "== hcheck (not installed; pattern rules skipped) =="
    return 0
  fi
  echo "== hcheck (patterns ruff cannot express) =="
  "$HCHECK" run .
}

tests() {
  # A repository may own its test entry point (unittest, cargo, npm …). The file
  # holds one argv line; it is executed directly, never through a shell.
  if [ -f scripts/test-cmd.txt ]; then
    local -a argv=()
    read -r -a argv < <(grep -v '^[[:space:]]*#' scripts/test-cmd.txt | head -1)
    if [ "${#argv[@]}" -eq 0 ]; then
      echo "quality: scripts/test-cmd.txt is empty" >&2
      return 2
    fi
    echo "== test command (${argv[*]}) =="
    "${argv[@]}"
    return $?
  fi
  local dir="${TEST_DIR:-}"
  if [ -z "$dir" ] && [ -f scripts/pytest-dir.txt ]; then
    dir=$(head -1 scripts/pytest-dir.txt | tr -d '[:space:]')
  fi
  if [ -n "$dir" ] && [ -d "$dir" ]; then
    # Some repos keep their package and tests in a subdirectory; running pytest
    # from the root then fails to import the package at all.
    ( cd "$dir" && tests_here )
    return $?
  fi
  tests_here
}

tests_here() {
  echo "== pytest (in $(pwd)) =="
  local extra="" cfg=()
  PYTHON=$(pick_python) || {
    echo "quality: no interpreter with pytest (set PYTHON=... or create .venv)" >&2
    return 2
  }
  # Repo-local deselects for known-broken tests live in git, not in someone's head.
  if [ -f scripts/pytest-args.txt ]; then
    extra=$(grep -v '^[[:space:]]*#' scripts/pytest-args.txt | grep -v '^[[:space:]]*$' | tr '\n' ' ')
  fi
  # Pin the config. Worktrees nested under a parent project otherwise inherit
  # that parent's pytest settings (wrong rootdir, wrong conftest resolution).
  if [ -f pytest.ini ]; then
    cfg=(-c pytest.ini)
  elif [ -f pyproject.toml ] && grep -q '^\[tool\.pytest' pyproject.toml; then
    cfg=()
  else
    cfg=(-c /dev/null --rootdir .)
  fi
  # shellcheck disable=SC2086
  "$PYTHON" -m pytest -q "${cfg[@]}" $extra
}

rc=0
case "$MODE" in
  --lint-only) lint || rc=$? ;;
  --patterns-only) patterns || rc=$? ;;
  --test-only) tests || rc=$? ;;
  --all|*)
    lint; rc=$?
    patterns; p_rc=$?
    tests; t_rc=$?
    [ "$rc" -eq 0 ] && [ "$p_rc" -ne 0 ] && rc=$p_rc
    [ "$rc" -eq 0 ] && [ "$t_rc" -ne 0 ] && rc=$t_rc
    ;;
esac
exit "$rc"
