#!/usr/bin/env bash
# Resolve the reviewed Health Check source from the root gitlink.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
source_path=third_party/meshcore-health-check
read -r mode revision stage _ < <(git -C "$repo" ls-files --stage -- "$source_path") || {
  echo 'Health Check source pin is missing.' >&2
  exit 1
}
if [[ $mode != 160000 || $stage != 0 || ! $revision =~ ^[a-f0-9]{40}$ ]]; then
  echo 'Health Check needs a resolved submodule pin.' >&2
  exit 1
fi
if [[ ${1:-} == --verify ]]; then
  if [[ ! -e $repo/$source_path/.git || $(git -C "$repo/$source_path" rev-parse HEAD) != "$revision" ]]; then
    echo 'Check out the pinned Health Check source with scripts/dev-all.sh init.' >&2
    exit 1
  fi
  if [[ -n $(git -C "$repo/$source_path" status --porcelain) ]]; then
    echo 'Commit Health Check source changes before building the pinned image.' >&2
    exit 1
  fi
fi
printf '%s\n' "$revision"
