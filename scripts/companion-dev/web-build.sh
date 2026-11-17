#!/bin/sh
set -eu
wanted=$(sha256sum package-lock.json | cut -d ' ' -f 1)
installed=$(cat node_modules/.ukmesh-dev-lock 2>/dev/null || true)
if [ "$wanted" != "$installed" ]; then
  npm ci --cache /npm-cache
  printf '%s\n' "$wanted" > node_modules/.ukmesh-dev-lock
fi
node node_modules/typescript/bin/tsc -b
node node_modules/vite/bin/vite.js build --mode companion --outDir /out
