#!/usr/bin/env bash
set -euo pipefail

# Every published application image must be signed by the protected tag job.
# Match only matrix entries in the two job blocks, preserving the workflow's
# existing explicit matrix format without introducing a YAML dependency.
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
workflow="${1:-${root}/.github/workflows/release.yml}"
built="$(sed -n '/^  image:/,/^  sign:/p' "$workflow" | sed -n 's/^ *- component: //p' | sort)"
signed="$(sed -n '/^  sign:/,$p' "$workflow" | sed -n 's/^ *- component: //p' | sort)"
if [ -z "$built" ] || [ "$built" != "$signed" ]; then
  echo 'Release build and signing components differ; every image must be signed.' >&2
  diff -u <(printf '%s\n' "$built") <(printf '%s\n' "$signed") >&2 || true
  exit 65
fi
printf 'Release build and signing components match (%s images).\n' "$(wc -l <<<"$built" | tr -d ' ')"
