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
  # An exported PYTHON= silently overrode resolution and made a healthy repo look
  # broken (it has to satisfy the check, not just exist). Say so and keep going.
  if [ -n "${PYTHON:-}" ]; then
    if has_pytest "$PYTHON"; then
      echo "quality: using PYTHON=$PYTHON (from the environment)" >&2
      echo "$PYTHON"; return 0
    fi
    echo "quality: warning: PYTHON=$PYTHON cannot run the tests (no pytest or not executable); ignoring it" >&2
  fi
  local candidates=(.venv/bin/python venv/bin/python
                    "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/venv/bin/python"
                    python3 "$HOME/harness/.venv/bin/python")
  local candidate
  for candidate in "${candidates[@]}"; do
    if has_pytest "$candidate"; then
      [ -n "${PYTHON:-}" ] && echo "quality: falling back to $candidate" >&2
      echo "$candidate"; return 0
    fi
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
  if [ "$MODE" = "--all" ]; then
    echo "== ruff (all files — legacy debt may appear) =="
    "$RUFF" check .
    return
  fi

  # Read paths NUL-separated into an array: a file called "my module.py" must not
  # be passed to ruff as two paths.
  local -a files=()
  if [ -n "$BASE_REF" ] && git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
    echo "== ruff (changed vs $BASE_REF) =="
    while IFS= read -r -d '' f; do files+=("$f"); done \
      < <(git diff -z --name-only --diff-filter=ACMR "$BASE_REF"...HEAD -- '*.py')
  else
    echo "== ruff (uncommitted + untracked changes) =="
    while IFS= read -r -d '' f; do files+=("$f"); done < <(
      { git diff -z --name-only --diff-filter=ACMR HEAD -- '*.py'
        git diff -z --name-only --diff-filter=ACMR --cached -- '*.py'
        git ls-files -z --others --exclude-standard -- '*.py'; } )
  fi

  # A clean tree has no uncommitted changes; falling back to the last commit keeps
  # this from being a gate that passes having checked nothing.
  if [ "${#files[@]}" -eq 0 ]; then
    if git rev-parse --verify --quiet HEAD~1 >/dev/null; then
      while IFS= read -r -d '' f; do files+=("$f"); done \
        < <(git diff -z --name-only --diff-filter=ACMR HEAD~1 HEAD -- '*.py')
      echo "== ruff (tree clean; files changed by HEAD $(git rev-parse --short HEAD)) =="
    else
      while IFS= read -r -d '' f; do files+=("$f"); done < <(git ls-files -z -- '*.py')
      echo "== ruff (tree clean, no parent commit; all tracked python files) =="
    fi
  fi

  if [ "${#files[@]}" -eq 0 ]; then
    # This gate is incremental: it lints the change under test. If the change (and
    # HEAD) touched no python, there is nothing to check and saying so is honest.
    # A full sweep is `quality.sh --lint-only --all`, which is a deliberate act.
    echo "quality: no python files in the change under test; nothing to lint" >&2
    return 0
  fi
  "$RUFF" check "${files[@]}"
}

patterns() {
  if [ -z "$HCHECK" ]; then
    echo "quality: hcheck not installed; the pattern gate cannot run" >&2
    return 2
  fi
  echo "== hcheck (patterns ruff cannot express) =="
  "$HCHECK" run .
}

tests() {
  # A repository may own its test entry point (unittest, cargo, npm …). The file
  # holds one argv line; it is executed directly, never through a shell.
  if [ -f scripts/test-cmd.txt ]; then
    # shlex semantics, not `read -a`: quoting must work, and a leading `python`
    # must become the interpreter that actually has the dependencies.
    local line py
    line=$(grep -v '^[[:space:]]*#' scripts/test-cmd.txt | grep -v '^[[:space:]]*$' | head -1)
    if [ -z "$line" ]; then
      echo "quality: scripts/test-cmd.txt is empty" >&2
      return 2
    fi
    py=$(pick_python) || true
    local -a argv=()
    while IFS= read -r -d '' tok; do argv+=("$tok"); done \
      < <(PYTHON_ARG="$py" python3 - "$line" <<'PY'
import os, shlex, sys
toks = shlex.split(sys.argv[1])
py = os.environ.get("PYTHON_ARG") or ""
if py and toks and toks[0] in ("python", "python3"):
    toks[0] = py
sys.stdout.write("".join(t + "\0" for t in toks))
PY
)
    if [ "${#argv[@]}" -eq 0 ]; then
      echo "quality: scripts/test-cmd.txt did not parse" >&2
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
