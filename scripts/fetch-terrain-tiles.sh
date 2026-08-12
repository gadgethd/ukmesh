#!/usr/bin/env bash
# fetch-terrain-tiles.sh — download UK terrain tiles (terrarium-encoded PNG)
# from the AWS Open Data "elevation-tiles-prod" bucket into ./terrain-tiles.
#
# Reproduces the DEM tile set the ukmesh map consumes via TERRAIN_DEM_SOURCE
# (frontend/src/components/Map/mapConfig.ts: raster-dem, terrarium, z5-12).
# Designed to run anywhere with python3 + curl; resumable (skips existing).
#
# Usage: ./fetch-terrain-tiles.sh [OUTDIR]
# Env:  LON_MIN LON_MAX LAT_MIN LAT_MAX Z_MIN Z_MAX JOBS (defaults below)
set -euo pipefail

OUT="${1:-terrain-tiles}"
: "${LON_MIN:=-11}"; : "${LON_MAX:=3}"; : "${LAT_MIN:=49.5}"; : "${LAT_MAX:=61}"
: "${Z_MIN:=5}";     : "${Z_MAX:=12}";  : "${JOBS:=32}"

BASE="https://s3.amazonaws.com/elevation-tiles-prod/terrarium"
mkdir -p "$OUT"

python3 - "$LON_MIN" "$LON_MAX" "$LAT_MIN" "$LAT_MAX" "$Z_MIN" "$Z_MAX" <<'PY' \
  | OUT="$OUT" BASE="$BASE" xargs -P "$JOBS" -n1 bash -c '
      IFS=/ read -r z x y <<< "$0"
      d="$OUT/$z/$x"; f="$d/$y.png"
      if [ -f "$f" ]; then exit 0; fi
      mkdir -p "$d"
      curl -fsS --retry 4 --retry-delay 2 -o "$f" "$BASE/$z/$x/$y.png"
    '
import sys, math

lon_min, lon_max, lat_min, lat_max, z_min, z_max = map(float, sys.argv[1:7])
z_min, z_max = int(z_min), int(z_max)

def xy(lon, lat, z):
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    r = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(r)) / math.pi) / 2.0 * n)
    return x, y

for z in range(z_min, z_max + 1):
    x1, _ = xy(lon_min, lat_min, z)
    x2, _ = xy(lon_max, lat_min, z)
    _, y1 = xy(lon_min, lat_max, z)  # north edge (smaller y)
    _, y2 = xy(lon_min, lat_min, z)  # south edge (larger y)
    for x in range(x1, x2 + 1):
        for y in range(y1, y2 + 1):
            print(f"{z}/{x}/{y}")
PY

echo "fetch-terrain-tiles: done -> $OUT"
