# Release and rollback runbook

Production application containers are released by immutable registry digest.
Do not deploy a mutable tag and do not substitute `docker compose build` for the
signed release path.

## Preconditions

- The release workflow has built the exact Git revision, scanned it for
  critical vulnerabilities, emitted an SPDX SBOM and provenance, and signed
  every image digest.
- Keyless Cosign trust identifies the exact GitHub release workflow and GitHub
  Actions OIDC issuer. A separately managed `COSIGN_PUBLIC_KEY` is also
  supported when releases intentionally use key-pair signing.
- A complete encrypted backup has a signed receipt.
- `scripts/restore-drill.sh` has produced a signed, verified restore receipt
  less than seven days old.
- The tracked checkout is clean and is exactly `SOURCE_REVISION`.
- Current service health is green and the prior signed digest is recorded.

Verify the evidence before changing a service:

```bash
git rev-parse HEAD
git status --short
openssl dgst -sha256 -verify "$RESTORE_RECEIPT_VERIFY_KEY" \
  -signature "${RESTORE_RECEIPT_PATH}.sig" "$RESTORE_RECEIPT_PATH"
jq '{status,source_revision,restore_verified_at,checks}' "$RESTORE_RECEIPT_PATH"
cosign verify "$BACKEND_IMAGE" \
  --certificate-identity-regexp "$COSIGN_CERTIFICATE_IDENTITY_REGEXP" \
  --certificate-oidc-issuer "$COSIGN_CERTIFICATE_OIDC_ISSUER"
cosign verify "$TARGET_IMAGE" \
  --certificate-identity-regexp "$COSIGN_CERTIFICATE_IDENTITY_REGEXP" \
  --certificate-oidc-issuer "$COSIGN_CERTIFICATE_OIDC_ISSUER"
docker compose config -q
```

## Release one service

Set immutable values and invoke the controlled replacement:

```bash
export COSIGN_CERTIFICATE_IDENTITY_REGEXP='^https://github.com/OWNER/REPOSITORY/.github/workflows/release.yml@'
export COSIGN_CERTIFICATE_OIDC_ISSUER='https://token.actions.githubusercontent.com'
export RESTORE_RECEIPT_PATH=/secure/receipts/latest.json
# The verify key lives outside the receipts directory and is pinned by sha256.
export RESTORE_RECEIPT_VERIFY_KEY=/secure/restore-receipt-trust/verify.pem
export RESTORE_RECEIPT_VERIFY_KEY_SHA256="$(openssl pkey -pubin -in "$RESTORE_RECEIPT_VERIFY_KEY" -outform DER | sha256sum | awk '{print $1}')"
export RELEASE_RECEIPT_SIGNING_KEY=/secure/release/receipt-signing.pem
export MESHCORE_INFRA_DIR=/home/ben/ukmesh/meshcore-infra
export MESHCORE_INFRA_PROJECT_NAME=meshcore-infra

scripts/replace-container.sh backend \
  --image="$BACKEND_IMAGE" \
  --backend-image="$BACKEND_IMAGE" \
  --source-revision="$SOURCE_REVISION"
```

If the infrastructure Compose file has another path, set
`MESHCORE_INFRA_COMPOSE_FILE` and, when needed,
`MESHCORE_INFRA_PROJECT_DIR`. The release script validates that external
project and resolves its single running `timescaledb` container before it
starts the migration job; the post-migration schema check uses that exact
container ID.


Repeat for each application service, including tagger, RF and HopReach workers,
passing its digest as `--image` and the same signed backend digest as
`--backend-image`. All application image revisions identify the UKMesh release
commit; the vendored HopReach upstream revision remains in `UPSTREAM_COMMIT`.
The release workflow builds and signs the development-site variant separately.
`scripts/check-release-components.sh` rejects an incomplete signing matrix.
The replacement script verifies signatures and revision labels, runs migrations,
starts the prior backend against the post-migration schema, replaces only the selected service with `--no-deps`,
checks health, readiness and metrics, and writes a release status receipt.

An existing mutable local deployment may enter this regime once by supplying
the exact prompt printed by the script:

```text
--bootstrap-immutable=bootstrap-SERVICE-SOURCE_REVISION
```

This is not a general signature bypass. Record the approval and do not use it
again after the first signed digest is live.

## Compose adoption preflight

`scripts/replace-container.sh` checks the selected service and backend before
pulling images, running migrations or replacing containers. You can run the
same read-only check separately from the application checkout:

```bash
scripts/check-compose-adoption.sh backend
```

Exactly one running container must carry this checkout's project, service,
working-directory and base Compose-file labels. A missing or mismatched label
stops replacement with exit 65. If Compose selects no container, the check also
looks for an unlabelled or stopped container occupying the configured name.
Docker or configuration lookup failures stop with exit 69.

The error prints an exact, shell-quoted reconciliation command using the detected
container IDs, project and any explicit `COMPOSE_FILE`/`COMPOSE_PATH_SEPARATOR`.
For an existing conflicting container it uses `docker rm -f -- <detected-id>`
followed by `docker compose --project-name <project> up -d --no-deps --no-build
<service>` and reruns the check; an absent container needs only the latter steps.
The check never executes these commands. Before an operator runs the printed
command in an approved maintenance window, verify that the authoritative Compose
files and image pins reproduce the current environment and mounts. On the live
host retain the external-network overlay (`docker-compose.live.yml`). The repair
causes service downtime but does not remove volumes or recreate sibling services.
Do not retry replacement or use `--force-recreate` to bypass this failure. Once
the preflight passes, resume the signed release procedure above.

## Stop and rollback conditions

Stop immediately for a privacy regression, cross-network response, migration
compatibility failure, data loss, failed readiness, missing metrics, or a
service health failure. `replace-container.sh` automatically rolls back only
when the prior image is itself a verified signed digest.

For a manual rollback, use the prior digests from the release receipt:

```bash
jq '{service,prior_image,prior_backend_image,schema_version,status}' \
  /home/ben/meshcore-releases/RELEASE_RECEIPT.json

export BACKEND_IMAGE='REGISTRY/backend@sha256:PRIOR_DIGEST'
docker compose pull backend
docker compose up -d --no-build --no-deps backend
curl --fail http://127.0.0.1:3000/readyz
```

Additive migrations are deliberately compatible with the prior backend. Never
attempt a destructive down-migration. If database contents must roll back,
stop MQTT ingest and all writers, restore the signed pre-release backup in an
isolated environment first, then perform a separately approved disaster
recovery cutover.

## Post-release verification

```bash
docker compose ps
curl --fail http://127.0.0.1:3000/readyz | jq .
curl --fail 'http://127.0.0.1:3000/api/stats?network=ukmesh' | jq .
docker compose exec -T backend wget -qO- http://127.0.0.1:9091/metrics \
  | grep '^meshcore_process_'
docker compose logs --since=10m backend
```

Confirm the public app, a WebSocket initial state, the owner login page, the
local operator page, Prometheus targets, and Alertmanager are healthy. Keep the
prior digest until the observation window ends.
