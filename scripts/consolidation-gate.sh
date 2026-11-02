#!/usr/bin/env bash
# UKMesh backend consolidation gate harness (P0-9).
# Read-only unless a gate's evidence file records a separately approved action.
# Usage: scripts/consolidation-gate.sh [g0|g1a|g1|g2a|g2|g3a|g3|g4|g5a|g5|g6|all]
set -uo pipefail

ROOT="${EVIDENCE_ROOT:-$HOME/.review-notes/ukmesh-revamp-20261101}"
GATES="$ROOT/gates"
FAIL=0

say()  { printf '%s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; FAIL=1; }
need_file() {
  if [ ! -s "$1" ]; then fail "missing or empty evidence: $1"; return 1; fi
  say "ok: $1"
}
need_text() { # file, text
  if ! grep -qF "$2" "$1" 2>/dev/null; then fail "$1 does not contain: $2"; return 1; fi
  say "ok: $1 contains '$2'"
}
dbq() {
  ssh ukmesh-vps "docker exec meshcore-infra-timescaledb-1 psql -U meshcore -d meshcore -Atc $(printf '%q' "$1")" 2>/dev/null
}
disk_free_gb() {
  ssh ukmesh-vps "df -BG / | awk 'NR==2 {gsub(/G/,\"\",\$4); print \$4}'" 2>/dev/null
}

g0() {
  say "== G0: P0 safety and baseline =="
  need_file "$ROOT/baseline/BASELINE.md"
  need_file "$ROOT/p0/P0-1-backup-coverage.md"
  need_file "$ROOT/p0/P0-3-space-plan.md"
  need_file "$ROOT/p0/P0-5-branch.txt"
  need_file "$ROOT/p0/P0-8-test-stacks.md"
  need_file "$ROOT/p0/P0-12-memory-headroom.md"
  need_file "$ROOT/p0/P0-13-708-closure.md"
  need_file "$ROOT/manifests/service-inventory.md"
  need_file "$ROOT/manifests/object-manifest.md"
  need_file "$ROOT/manifests/gate-manifest.md"
  need_file "$ROOT/runbooks/ROLLBACK.md"

  # P0-1 is either verified or explicitly waived by Ben; no silent pass.
  if grep -q "WAIVED by Ben" "$ROOT/p0/P0-1-backup-coverage.md"; then
    say "P0-1: waived by Ben (no restore/RPO proof)"
  else
    fail "P0-1: neither verified nor explicitly waived"
  fi
  # P0-4 window must be named, not a template.
  if grep -q "PENDING" "$ROOT/runbooks/WINDOW.md"; then
    fail "P0-4: production change window/owners are still PENDING"
  fi

  need_file "$ROOT/p0/P0-11-data-ownership.md"
  need_file "$ROOT/p0/P0-14-path-hash-provenance.md"

  # Production schema unchanged: no target schemas, ledger still 70/070.
  local nschema nledger last
  nschema="$(dbq "select count(*) from pg_namespace where nspname in ('core','mesh','analytics','archive','ops');")"
  nledger="$(dbq "select count(*) from schema_migrations;")"
  last="$(dbq "select max(name) from schema_migrations;")"
  [ "$nschema" = "0" ] || fail "target schemas already exist (count=$nschema); P0 must be read-only"
  [ "$nledger" = "70" ] || fail "schema_migrations count=$nledger, expected 70"
  [ "$last" = "070_ingest_ledger_archive.sql" ] || fail "ledger last=$last, expected 070_ingest_ledger_archive.sql"
  say "schema/ledger check: namespaces=$nschema ledger=$nledger last=$last"

  local free_gb
  free_gb="$(disk_free_gb)"
  if [ -n "$free_gb" ] && [ "$free_gb" -lt 20 ]; then
    fail "free disk ${free_gb}G is below the 20G floor"
  fi
  say "free disk: ${free_gb:-unknown}G"

  [ "$FAIL" -eq 0 ] && say "G0: PASS" || say "G0: BLOCKED"
  return "$FAIL"
}

g1a() {
  say "== G1a: recovery and resource admission =="
  # P0-1 restore proof is waived by Ben 2026-11-02; do not require it.
  say "G1a: restore proof waived (P0-1)"
  need_file "$GATES/G1a-memory.json"
  need_file "$GATES/G1a-roles.txt"
  local free_gb
  free_gb="$(disk_free_gb)"
  if [ -z "$free_gb" ] || [ "$free_gb" -lt 30 ]; then
    fail "free disk ${free_gb:-unknown}G is below the adaptive 30G G1a floor"
  fi
  say "free disk: ${free_gb:-unknown}G"
  [ "$FAIL" -eq 0 ] && say "G1a: PASS" || say "G1a: BLOCKED"
  return "$FAIL"
}

require_gate() { # name
  local name="$1" file="$GATES/$1.json"
  say "== $name =="
  need_file "$file" || { say "$name: BLOCKED"; return 1; }
  python3 - "$file" <<'PY'
import json, sys
p = sys.argv[1]
try:
    d = json.load(open(p))
except Exception as exc:
    print(f"invalid JSON: {exc}", file=sys.stderr)
    raise SystemExit(1)
if d.get("status") != "pass":
    print(f"status is {d.get('status')!r}, expected 'pass'", file=sys.stderr)
    raise SystemExit(1)
if not isinstance(d.get("assertions"), list) or not d["assertions"]:
    print("assertions must be a non-empty list", file=sys.stderr)
    raise SystemExit(1)
if not d.get("evidence"):
    print("evidence is required", file=sys.stderr)
    raise SystemExit(1)
if not d.get("verified_by"):
    print("verified_by is required", file=sys.stderr)
    raise SystemExit(1)
print(f"ok: {p} status=pass assertions={len(d['assertions'])}")
PY
  local rc=$?
  if [ "$rc" -ne 0 ]; then fail "$name: evidence schema/assertions failed"; fi
  [ "$FAIL" -eq 0 ] && say "$name: PASS" || say "$name: BLOCKED"
  return "$FAIL"
}

case "${1:-all}" in
  g0)  g0 ;;
  g1a) g1a ;;
  g1)  require_gate G1 ;;
  g2a) require_gate G2a ;;
  g2)  require_gate G2 ;;
  g3a) require_gate G3a ;;
  g3)  require_gate G3 ;;
  g4)  require_gate G4 ;;
  g5a) require_gate G5a ;;
  g5)  require_gate G5 ;;
  g6)  require_gate G6 ;;
  all)
    g0;  G0=$?
    g1a; G1A=$?
    say "== summary =="
    say "G0=$G0 G1a=$G1A; G1..G6 require their own phase evidence"
    exit $(( G0 || G1A ))
    ;;
  *)
    say "usage: $0 [g0|g1a|g1|g2a|g2|g3a|g3|g4|g5a|g5|g6|all]" >&2
    exit 2
    ;;
esac
