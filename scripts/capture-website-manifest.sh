#!/bin/bash
# Capture the exact deployed state of a website image into website-live-manifest.json
# Usage: scripts/capture-website-manifest.sh <image-ref> [--staging]
#   --staging: writes website-live-manifest.staging.json instead (for test-site captures)
set -euo pipefail
cd "$(dirname "$0")/.."

IMG="${1:?usage: capture-website-manifest.sh <image-ref> [--staging]}"
OUT="website-live-manifest.json"
[ "${2:-}" = "--staging" ] && OUT="website-live-manifest.staging.json"

echo "capturing $IMG -> $OUT ..."

HASHES=$(mktemp)
docker run --rm --entrypoint sh "$IMG" -c 'cd /usr/share/nginx/html && find . -type f | sort | xargs sha256sum' > "$HASHES"

python3 - "$IMG" "$OUT" "$HASHES" <<'EOF'
import json, subprocess, sys, datetime
img, out, hashes_path = sys.argv[1], sys.argv[2], sys.argv[3]
bundles = {}
for line in open(hashes_path):
    h, p = line.split('  ', 1)
    bundles[p.strip().lstrip('./')] = h
meta = subprocess.run(['docker', 'inspect', img, '--format', '{{.Id}}|{{.Created}}|{{index .Config.Labels "org.opencontainers.image.revision"}}'],
                      capture_output=True, text=True).stdout.strip().split('|')
manifest = {
    "site": "ukmesh.com",
    "captured": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    "imageId": meta[0],
    "imageCreated": meta[1] if len(meta) > 1 else "",
    "sourceRevisionLabel": meta[2] if len(meta) > 2 else "",
    "fileCount": len(bundles),
    "files": bundles,
}
json.dump(manifest, open(out, 'w'), indent=2, sort_keys=True)
print(f"wrote {out}: {len(bundles)} files")
EOF
