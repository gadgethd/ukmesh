#!/usr/bin/env bash
set -euo pipefail
umask 077

source_dir="${BACKUP_SOURCE_DIR:-/home/ben/ukmesh/backups}"
target_dir="${BACKUP_SYNC_TARGET_DIR:?BACKUP_SYNC_TARGET_DIR must name the mounted off-host publication target}"
verify_key="${BACKUP_RECEIPT_VERIFY_KEY:?BACKUP_RECEIPT_VERIFY_KEY is required}"
allow_local="${BACKUP_SYNC_ALLOW_LOCAL_TEST:-false}"

for command in openssl jq sha256sum findmnt find sort sed awk sync; do
  command -v "$command" >/dev/null || {
    echo "required command is unavailable: $command" >&2
    exit 69
  }
done
test -d "$source_dir" || { echo "backup source is missing: $source_dir" >&2; exit 66; }
test -r "$verify_key" || { echo "receipt verify key is unreadable" >&2; exit 66; }
mkdir -p "$target_dir"
target_dir="$(cd "$target_dir" && pwd)"
test -f "$target_dir/.meshcore-offsite-target" || {
  echo "sync target is not attested (.meshcore-offsite-target missing)" >&2
  exit 65
}
if [ "$allow_local" != "true" ] \
  && [ "$(findmnt -T "$source_dir" -n -o SOURCE)" = "$(findmnt -T "$target_dir" -n -o SOURCE)" ]; then
  echo "sync target shares the backup source filesystem" >&2
  exit 65
fi

receipt_path="$(
  find "$source_dir" -maxdepth 1 -type f -name 'backup-*.receipt.json' -printf '%T@ %p\n' \
    | sort -nr \
    | sed -n '1 { s/^[^ ]* //; p; }'
)"
test -n "$receipt_path" && test -f "$receipt_path" || {
  echo "no completed backup receipt found" >&2
  exit 66
}
signature_path="${receipt_path}.sig"
test -f "$signature_path" || { echo "backup receipt signature is missing" >&2; exit 66; }
openssl dgst -sha256 -verify "$verify_key" \
  -signature "$signature_path" "$receipt_path" >/dev/null
jq -e '.format == "meshcore-backup-receipt-v1" and .status == "complete"' \
  "$receipt_path" >/dev/null
archive_name="$(jq -er '.archive' "$receipt_path")"
case "$archive_name" in
  */*|'') echo "backup receipt archive name is unsafe" >&2; exit 65 ;;
esac
archive_path="$source_dir/$archive_name"
test -f "$archive_path" || { echo "backup archive is missing" >&2; exit 66; }
expected_archive_sha="$(jq -er '.archive_sha256' "$receipt_path")"
actual_archive_sha="$(sha256sum "$archive_path" | awk '{print $1}')"
test "$actual_archive_sha" = "$expected_archive_sha" || {
  echo "backup archive checksum does not match receipt" >&2
  exit 65
}

publication_id="$(jq -er '.backup_id' "$receipt_path")"
case "$publication_id" in
  backup-[0-9A-Za-z._-]*) ;;
  *) echo "backup id is unsafe" >&2; exit 65 ;;
esac
partial_dir="$target_dir/.${publication_id}.partial"
test ! -e "$partial_dir" || { echo "partial publication already exists" >&2; exit 73; }
mkdir "$partial_dir"
cleanup() {
  if [ -d "$partial_dir" ]; then
    find "$partial_dir" -maxdepth 1 -type f -delete
    rmdir "$partial_dir"
  fi
}
trap cleanup EXIT

cp -- "$archive_path" "$partial_dir/$archive_name"
cp -- "$receipt_path" "$partial_dir/latest.receipt.json"
cp -- "$signature_path" "$partial_dir/latest.receipt.json.sig"
test "$(sha256sum "$partial_dir/$archive_name" | awk '{print $1}')" = "$expected_archive_sha"
openssl dgst -sha256 -verify "$verify_key" \
  -signature "$partial_dir/latest.receipt.json.sig" \
  "$partial_dir/latest.receipt.json" >/dev/null
(
  cd "$partial_dir"
  sha256sum "$archive_name" latest.receipt.json latest.receipt.json.sig \
    >latest.sha256
  sha256sum --check latest.sha256 >/dev/null
)
sync "$partial_dir/$archive_name" "$partial_dir/latest.receipt.json" \
  "$partial_dir/latest.receipt.json.sig" "$partial_dir/latest.sha256"

mv "$partial_dir/$archive_name" "$target_dir/$archive_name"
mv "$partial_dir/latest.receipt.json" "$target_dir/latest.receipt.json"
mv "$partial_dir/latest.receipt.json.sig" "$target_dir/latest.receipt.json.sig"
mv "$partial_dir/latest.sha256" "$target_dir/latest.sha256"
rmdir "$partial_dir"
sync "$target_dir/$archive_name" "$target_dir/latest.receipt.json" \
  "$target_dir/latest.receipt.json.sig" "$target_dir/latest.sha256"
trap - EXIT

(
  cd "$target_dir"
  sha256sum --check latest.sha256 >/dev/null
)
openssl dgst -sha256 -verify "$verify_key" \
  -signature "$target_dir/latest.receipt.json.sig" \
  "$target_dir/latest.receipt.json" >/dev/null
jq -n --arg backup_id "$publication_id" --arg target "$target_dir" \
  --arg archive "$archive_name" \
  '{backup_id: $backup_id, target: $target, archive: $archive, status: "verified"}'
