#!/usr/bin/env bash
set -euo pipefail
mkdir -p /tmp/corescope
tar -C /src/corescope --exclude=node_modules --exclude=dist --exclude=.git -cf - . | tar -C /tmp/corescope -xf -
cd /tmp/corescope
# SQLite requires the repository's native cgo build flags.
make build-server build-ingestor GIT_VERSION=ukmesh-dev GIT_COMMIT="${COMPANION_CORESCOPE_COMMIT:-unknown}"
for executable in corescope-server corescope-ingestor; do
  cp "dist/$executable" "/out/$executable.next"
  mv "/out/$executable.next" "/out/$executable"
done
