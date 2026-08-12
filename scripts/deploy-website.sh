#!/bin/bash
# Safe website deploy: drift-check -> pin -> up -d --no-deps -> verify.
# Usage: scripts/deploy-website.sh <image-ref> <service> [--force]
#   <service> = website-ukmesh (prod) or website-dev (staging)
#   --force   = deploy even if the image drifts from the manifest (e.g. intentional release)
#
# Encodes the hard-won pitfalls: never bare `up`, never deploy the wrong image,
# never trust a build output digest, verify what is SERVED not what was built.
set -Eeuo pipefail
cd "$(dirname "$0")/.."

IMG="${1:?usage: deploy-website.sh <image-ref> <website-ukmesh|website-dev> [--force]}"
SVC="${2:?usage: deploy-website.sh <image-ref> <website-ukmesh|website-dev> [--force]}"
FORCE=0
[ "${3:-}" = "--force" ] && FORCE=1
[ "$SVC" = "website-ukmesh" ] || [ "$SVC" = "website-dev" ] || { echo "service must be website-ukmesh or website-dev"; exit 2; }
[ -f .env ] || { echo "no .env in $(pwd)"; exit 2; }

# Rollback state: set only once the live pin has been mutated. The trap fires
# on ANY exit (error, signal, or early verification failure) so a broken or
# unverified deployment always restores the previous pin.
PIN="WEBSITE_DEV_IMAGE"
[ "$SVC" = "website-ukmesh" ] && PIN="WEBSITE_IMAGE"
OLD=""
rollback() {
  if [ -n "$OLD" ] && grep -q "^$PIN=.*" .env && [ "$(grep "^$PIN=" .env | cut -d= -f2)" != "$OLD" ]; then
    echo "ROLLBACK: restoring $PIN=$OLD"
    sed -i "s|^$PIN=.*|$PIN=$OLD|" .env
    docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps "$SVC" >/dev/null 2>&1 || true
    sleep 6
    if docker ps --format '{{.Names}}' | grep -q "^$SVC$"; then
      echo "ROLLBACK: $SVC is up on old pin"
    else
      echo "ROLLBACK WARNING: $SVC is NOT running after restore — inspect manually" >&2
    fi
  fi
}
trap rollback EXIT

echo "== 1/5 drift check =="
# The drift checker exits 1 on intentional drift — capture rc without tripping set -e.
if DRIFT_OUT=$(scripts/check-website-drift.sh "$IMG" $( [ "$SVC" = "website-dev" ] && echo --staging ) 2>&1); then
  DRIFT_RC=0
else
  DRIFT_RC=$?
fi
echo "$DRIFT_OUT"
if [ "$DRIFT_RC" = "1" ] && [ "$FORCE" != "1" ]; then
  echo "ABORT: image drifts from deployed state. Re-run with --force for an intentional release."
  exit 1
fi

echo "== 2/5 resolve real digest =="
DIGEST=$(docker inspect "$IMG" --format '{{.Id}}')
echo "new digest: $DIGEST"
grep -q "^$PIN=" .env || { echo "no $PIN in .env"; exit 2; }
OLD=$(grep "^$PIN=" .env | cut -d= -f2)
echo "old pin:  $OLD"
echo "new pin:  $DIGEST"

echo "== 3/5 pin + recreate (--no-deps) =="
sed -i "s|^$PIN=.*|$PIN=$DIGEST|" .env
docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps "$SVC"

echo "== 4/5 wait for health =="
sleep 6
docker ps --format '{{.Names}} {{.Status}}' | grep "$SVC" || { echo "container not up"; exit 1; }

echo "== 5/5 verify SERVED content vs image =="
PORT=3006; [ "$SVC" = "website-ukmesh" ] && PORT=3004
PASS=1
for a in $(docker run --rm --entrypoint sh "$IMG" -c 'grep -oE "assets/[A-Za-z0-9_.-]+\\.(js|css)" /usr/share/nginx/html/index.html' | sort -u); do
  H1=$(docker run --rm --entrypoint sh "$IMG" -c "sha256sum /usr/share/nginx/html/$a" | awk '{print $1}')
  H2=$(curl -s "http://127.0.0.1:$PORT/$a" | sha256sum | awk '{print $1}')
  [ "$H1" = "$H2" ] || { echo "MISMATCH: $a"; PASS=0; }
done
if [ "$PASS" != "1" ]; then
  echo "VERIFICATION FAILED: served bundles do not match $IMG on :$PORT — rolling back"
  exit 1
fi
echo "VERIFIED: served bundles match $IMG on :$PORT"
echo "deploy complete: $SVC -> $DIGEST"

# Verification succeeded — disarm the rollback trap so the EXIT below keeps the new pin.
trap - EXIT
exit 0
