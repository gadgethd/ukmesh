#!/bin/bash
# Drift guard: compare a website image's content against the live manifest.
# Usage: scripts/check-website-drift.sh <image-ref> [--staging]
# Exit 0 = identical to deployed state. Exit 1 = drift (prints the differing files).
set -uo pipefail
cd "$(dirname "$0")/.."

IMG="${1:?usage: check-website-drift.sh <image-ref> [--staging]}"
MANIFEST="website-live-manifest.json"
[ "${2:-}" = "--staging" ] && MANIFEST="website-live-manifest.staging.json"
[ -f "$MANIFEST" ] || { echo "NO MANIFEST at $MANIFEST — capture one first (scripts/capture-website-manifest.sh)"; exit 2; }

HASHES=$(mktemp)
docker run --rm --entrypoint sh "$IMG" -c 'cd /usr/share/nginx/html && find . -type f | sort | xargs sha256sum' > "$HASHES" 2>/dev/null || { echo "cannot inspect $IMG"; exit 2; }

python3 - "$MANIFEST" "$HASHES" <<'EOF'
import json, sys
manifest, hashes_path = sys.argv[1], sys.argv[2]
ref = json.load(open(manifest))["files"]
cur = {}
for line in open(hashes_path):
    h, p = line.split('  ', 1)
    cur[p.strip().lstrip('./')] = h
added = sorted(set(cur) - set(ref))
removed = sorted(set(ref) - set(cur))
changed = sorted(p for p in set(cur) & set(ref) if cur[p] != ref[p])
if not (added or removed or changed):
    print(f"NO DRIFT — {len(cur)} files identical to {manifest}")
    sys.exit(0)
print(f"DRIFT vs {manifest}: +{len(added)} added, -{len(removed)} removed, {len(changed)} changed")
for p in added:   print(f"  + {p}")
for p in removed: print(f"  - {p}")
for p in changed: print(f"  ~ {p}")
sys.exit(1)
EOF
