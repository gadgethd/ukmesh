#!/usr/bin/env bash
set -euo pipefail
mkdir -p /tmp/corescope
tar -C /src/corescope --exclude=node_modules --exclude=dist --exclude=.git -cf - . | tar -C /tmp/corescope -xf -
cd /tmp/corescope
# Use the repo's cgo build flags; both binaries share SQLite's native driver.
make build-server build-ingestor GIT_VERSION=ukmesh-dev GIT_COMMIT="${COMPANION_CORESCOPE_COMMIT:-unknown}"
for executable in corescope-server corescope-ingestor; do
  cp "dist/$executable" "/out/$executable.next"
  mv "/out/$executable.next" "/out/$executable"
done
cd /src/beacon-server
go build -trimpath -o /out/beacon.next ./cmd/beacon
mv /out/beacon.next /out/beacon
