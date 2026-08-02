#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
tsx="${script_dir}/node_modules/.bin/tsx"
test -x "$tsx"

test_root="$(mktemp -d)"
trap 'rm -rf -- "$test_root"' EXIT

safe_dir="${test_root}/safe/keys"
stdout_path="${test_root}/stdout.log"
"$tsx" "${script_dir}/generate-observer-key.ts" \
  --name "Security fixture" \
  --output-dir "$safe_dir" \
  >"$stdout_path"

test "$(stat -c '%a' "$safe_dir")" = "700"
key_path="$(
  find "$safe_dir" -maxdepth 1 -type f -name 'observer-*.json' -print -quit
)"
test -n "$key_path"
test "$(stat -c '%a' "$key_path")" = "600"
test "$(jq -r '.name' "$key_path")" = "Security fixture"
public_key="$(jq -r '.publicKeyHex' "$key_path")"
private_key="$(jq -r '.privateKeyHex' "$key_path")"
test "${#public_key}" -eq 64
test "${#private_key}" -eq 64
grep -Fq "$public_key" "$stdout_path"
if grep -Fq "$private_key" "$stdout_path"; then
  echo "private key was exposed on stdout" >&2
  exit 1
fi

unsafe_dir="${test_root}/unsafe"
mkdir -m 0755 "$unsafe_dir"
set +e
"$tsx" "${script_dir}/generate-observer-key.ts" \
  --output-dir "$unsafe_dir" \
  >"${test_root}/unsafe.stdout" 2>"${test_root}/unsafe.stderr"
unsafe_status=$?
set -e
test "$unsafe_status" -ne 0
test -z "$(find "$unsafe_dir" -maxdepth 1 -type f -print -quit)"

symlink_target="${test_root}/symlink-target"
mkdir -m 0700 "$symlink_target"
ln -s "$symlink_target" "${test_root}/symlink"
set +e
"$tsx" "${script_dir}/generate-observer-key.ts" \
  --output-dir "${test_root}/symlink" \
  >"${test_root}/symlink.stdout" 2>"${test_root}/symlink.stderr"
symlink_status=$?
set -e
test "$symlink_status" -ne 0
test -z "$(find "$symlink_target" -maxdepth 1 -type f -print -quit)"

printf 'observer key permissions, symlink, and stdout privacy tests passed\n'
