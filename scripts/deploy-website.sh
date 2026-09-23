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

# Rollback state: armed immediately before the pin mutation so even a partial
# write or any later verification failure restores the previous pin.
PIN="WEBSITE_DEV_IMAGE"
[ "$SVC" = "website-ukmesh" ] && PIN="WEBSITE_IMAGE"
OLD=""
PIN_UPDATED=0
PORT=3006
[ "$SVC" = "website-ukmesh" ] && PORT=3004

wait_for_service() {
  local container_id running health
  for _ in $(seq 1 30); do
    container_id=$(docker compose -f docker-compose.yml -f docker-compose.live.yml ps -q "$SVC" 2>/dev/null || true)
    if [ -n "$container_id" ]; then
      running=$(docker inspect "$container_id" --format '{{.State.Running}}' 2>/dev/null || true)
      health=$(docker inspect "$container_id" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>/dev/null || true)
      if [ "$running" = "true" ] \
        && { [ "$health" = "healthy" ] || [ "$health" = "none" ]; } \
        && curl --fail --silent --show-error --max-time 3 "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

rollback() {
  local original_status=$?
  trap - EXIT
  if [ "$PIN_UPDATED" = "1" ] \
    && grep -q "^$PIN=.*" .env \
    && [ "$(grep "^$PIN=" .env | cut -d= -f2)" != "$OLD" ]; then
    echo "ROLLBACK: restoring $PIN=$OLD"
    if ! sed -i "s|^$PIN=.*|$PIN=$OLD|" .env; then
      echo "ROLLBACK FAILURE: could not restore $PIN in .env" >&2
      original_status=1
    elif ! docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps "$SVC" >/dev/null; then
      echo "ROLLBACK FAILURE: could not recreate $SVC on old pin" >&2
      original_status=1
    elif wait_for_service; then
      echo "ROLLBACK: $SVC is healthy on old pin"
    else
      echo "ROLLBACK FAILURE: $SVC is not healthy after restoring the old pin" >&2
      original_status=1
    fi
  fi
  exit "$original_status"
}

echo "== 1/5 drift check =="
# The drift checker exits 1 on intentional drift — capture rc without tripping set -e.
DRIFT_ARGS=()
if [ "$SVC" = "website-dev" ]; then DRIFT_ARGS=(--staging); fi
if DRIFT_OUT=$(scripts/check-website-drift.sh "$IMG" "${DRIFT_ARGS[@]}" 2>&1); then
  DRIFT_RC=0
else
  DRIFT_RC=$?
fi
echo "$DRIFT_OUT"
if [ "$DRIFT_RC" -eq 1 ]; then
  if [ "$FORCE" != "1" ]; then
    echo "ABORT: image drifts from deployed state. Re-run with --force for an intentional release."
    exit 1
  fi
elif [ "$DRIFT_RC" -ne 0 ]; then
  echo "ABORT: drift check could not verify the image (exit $DRIFT_RC)." >&2
  exit "$DRIFT_RC"
fi

echo "== 2/5 resolve real digest =="
DIGEST=$(docker inspect "$IMG" --format '{{.Id}}')
echo "new digest: $DIGEST"
grep -q "^$PIN=" .env || { echo "no $PIN in .env"; exit 2; }
OLD=$(grep "^$PIN=" .env | cut -d= -f2)
echo "old pin:  $OLD"
echo "new pin:  $DIGEST"

echo "== 3/5 pin + recreate (--no-deps) =="
PIN_UPDATED=1
trap rollback EXIT
sed -i "s|^$PIN=.*|$PIN=$DIGEST|" .env
docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps "$SVC"

echo "== 4/5 wait for health =="
wait_for_service || { echo "container is not healthy"; exit 1; }

echo "== 5/5 verify SERVED content vs image =="
PASS=1
if ! ASSETS=$(docker run --rm --entrypoint sh "$IMG" -c 'grep -oE "assets/[A-Za-z0-9_.-]+\\.(js|css)" /usr/share/nginx/html/index.html' | sort -u); then
  echo "VERIFICATION FAILED: unable to read the bundle list from $IMG" >&2
  exit 1
fi
if [ -z "$ASSETS" ]; then
  echo "VERIFICATION FAILED: $IMG index.html contains no JS or CSS bundles" >&2
  exit 1
fi
while IFS= read -r a; do
  H1=$(docker run --rm --entrypoint sh "$IMG" -c "sha256sum /usr/share/nginx/html/$a" | awk '{print $1}')
  H2=$(curl --fail --silent --show-error --location --max-time 10 "http://127.0.0.1:$PORT/$a" | sha256sum | awk '{print $1}')
  [ "$H1" = "$H2" ] || { echo "MISMATCH: $a"; PASS=0; }
done <<< "$ASSETS"
if [ "$PASS" != "1" ]; then
  echo "VERIFICATION FAILED: served bundles do not match $IMG on :$PORT — rolling back"
  exit 1
fi
echo "VERIFIED: served bundles match $IMG on :$PORT"
echo "deploy complete: $SVC -> $DIGEST"

# Verification succeeded — disarm the rollback trap so the EXIT below keeps the new pin.
trap - EXIT
exit 0
