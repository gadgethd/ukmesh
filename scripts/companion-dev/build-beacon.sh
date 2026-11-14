#!/usr/bin/env bash
set -euo pipefail
cd /src/beacon-server
go build -trimpath -o /out/beacon.next ./cmd/beacon
mv /out/beacon.next /out/beacon
