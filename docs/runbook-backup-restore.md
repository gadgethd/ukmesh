# Backup and restore runbook

The backup set contains the analytics and owner-auth databases, Redis durable
state, Mosquitto persistence and credentials, and the configuration needed to
reconstruct the release. The configuration payload includes the protected
`.env` recovery secrets, Compose/Docker build definitions, edge policies,
monitoring rules, database schema/migrations, and the recovery/deployment tools
themselves. It contains both app Compose overlays and the separately managed
`meshcore-infra` Compose file/configuration; data-plane reads are addressed only
through the explicit `meshcore-infra` project. `.env` files must be regular
files with no group/world permissions. The
set is encrypted before leaving its access-controlled temporary storage and has
a detached signed receipt. The initial objective is RPO 24 hours and RTO four
hours.

The single archive file uses `CMS-AES-256-CBC-CHUNKED-v1`: a signed,
length-framed sequence of independently encrypted 128 MiB CMS records. This
keeps both encryption and restore memory bounded below OpenSSL's 2 GiB attached
CMS parsing ceiling. The detached receipt authenticates the exact complete
archive before any chunk is decrypted.

## One-time keys and target

Generate separate encryption and receipt-signing keys on a protected operator
host. Keep private keys out of the repository and away from the backup target.

```bash
install -d -m 0700 /secure/meshcore-backup-keys
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 \
  -subj '/CN=MeshCore backup encryption/' \
  -keyout /secure/meshcore-backup-keys/decrypt.pem \
  -out /secure/meshcore-backup-keys/encrypt.pem
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
  -out /secure/meshcore-backup-keys/receipt-signing.pem
openssl pkey -in /secure/meshcore-backup-keys/receipt-signing.pem \
  -pubout -out /secure/meshcore-backup-keys/receipt-verify.pem
chmod 0600 /secure/meshcore-backup-keys/*
```

Mount an off-host backup target and attest it once:

```bash
install -d -m 0700 /mnt/offsite/meshcore
touch /mnt/offsite/meshcore/.meshcore-offsite-target
chmod 0600 /mnt/offsite/meshcore/.meshcore-offsite-target
```

The script rejects a target on the application filesystem unless
`BACKUP_ALLOW_LOCAL_STAGING=true` is explicitly set for a drill.

## Create and verify a backup

```bash
export BACKUP_OUTPUT_DIR=/mnt/offsite/meshcore
export BACKUP_ENCRYPTION_CERT=/secure/meshcore-backup-keys/encrypt.pem
export BACKUP_RECEIPT_SIGNING_KEY=/secure/meshcore-backup-keys/receipt-signing.pem
export MESHCORE_INFRA_DIR=/home/ben/ukmesh/meshcore-infra
export MESHCORE_INFRA_PROJECT_NAME=meshcore-infra
scripts/backup.sh
```

For the returned receipt and archive:

```bash
openssl dgst -sha256 \
  -verify /secure/meshcore-backup-keys/receipt-verify.pem \
  -signature "$RECEIPT.sig" "$RECEIPT"
test "$(jq -r .status "$RECEIPT")" = complete
test "$(sha256sum "$ARCHIVE" | cut -d' ' -f1)" = \
  "$(jq -r .archive_sha256 "$RECEIPT")"
```

Copy the receipt, signature, and public verification key alongside the
encrypted archive. Alert if any required dataset lacks a verified receipt or
the newest receipt is older than 24 hours.

If backup creation stages under `/home/ben/ukmesh/backups`, publish the newest
complete set to a separately mounted and attested filesystem with atomic names
and an independently checked checksum manifest:

```bash
export BACKUP_SOURCE_DIR=/home/ben/ukmesh/backups
export BACKUP_SYNC_TARGET_DIR=/mnt/offsite/meshcore
export BACKUP_RECEIPT_VERIFY_KEY=/secure/meshcore-backup-keys/receipt-verify.pem
scripts/sync-latest.sh
(cd /mnt/offsite/meshcore && sha256sum --check latest.sha256)
```

`BACKUP_SYNC_ALLOW_LOCAL_TEST=true` is drill-only; it does not satisfy the
off-host pre-reset gate.

## Owner authorization reset gate

Before the backup, export every active grant (including database-only and
legacy/null-method rows) without changing operator configuration:

```bash
install -d -m 0700 /home/ben/ukmesh/backups
cd backend && npm run build
node --env-file=../.env dist/tools/inventoryOwnerAuthorization.js \
  --export=/home/ben/ukmesh/backups/owner-grants-YYYY-MM-DDTHHMMSSZ.json
```

The export is secret, mode 0600, checksum-bearing, and must never be committed.
The legacy disposition is explicit preservation pending Ben's promote/revoke
decision. Restore `owner-auth.dump`, mount the export read-only, and set:

```dotenv
OWNER_AUTH_INVENTORY_HOST_DIR=/home/ben/ukmesh/backups
OWNER_AUTH_INVENTORY_BASELINE_PATH=/owner-auth-inventory/owner-grants-YYYY-MM-DDTHHMMSSZ.json
```

Before public admission, run `owner-auth:inventory -- --baseline=<host-path>
--require-complete`. `/readyz` also fails closed until active account/grant and
per-method counts, configured/desired/rendered/applied generations, last error,
and ACL readback exactly match the pre-reset baseline. Do not rewrite
`OWNER_MQTT_USERNAME_MAP` as part of this preservation step.

## Isolated restore drill

The drill creates a private Docker network and disposable PostgreSQL, Redis,
and Mosquitto volumes. It restores every dataset, migrates the restored schema,
boots the current backend read-only, checks readiness and stats, samples data
integrity and owner lookups, signs a restore receipt, then removes the isolated
containers and volumes.

```bash
export BACKUP_RECEIPT_VERIFY_KEY=/secure/meshcore-backup-keys/receipt-verify.pem
export BACKUP_DECRYPTION_CERT=/secure/meshcore-backup-keys/encrypt.pem
export BACKUP_DECRYPTION_KEY=/secure/meshcore-backup-keys/decrypt.pem
export RESTORE_RECEIPT_SIGNING_KEY=/secure/meshcore-backup-keys/receipt-signing.pem
export RESTORE_RECEIPT_VERIFY_KEY=/secure/meshcore-backup-keys/receipt-verify.pem
export RESTORE_RECEIPT_DIR=/secure/meshcore-restore-receipts

scripts/restore-drill.sh "$ARCHIVE" "$RECEIPT"
```

Verify `latest.json`, `latest.json.sig`, and `verify.pem`, then set
`BACKUP_RECEIPT_HOST_DIR` to that receipt directory so backend health and
destructive lifecycle gates see the same signed evidence.

## Disaster recovery

Do not restore directly over a running production volume.

1. Declare the incident and stop external ingest and every writer.
2. Select the newest receipt whose signature and encrypted checksum verify.
3. Run the isolated drill and record its demonstrated RPO/RTO.
4. Create fresh production volumes and restore using the same ordered dataset
   procedure as the drill.
5. Run migrations from the signed target backend image.
6. Restore `owner-auth.dump` and the owner inventory baseline before starting
   the reconciler. Boot with MQTT ingest disabled; require the CLI baseline
   validation and `/readyz`, then verify privacy-scoped stats, owner lookup,
   queue invariants, and Mosquitto authentication.
7. Reopen public reads, then workers, then MQTT ingest.
8. Preserve the failed volumes read-only until incident review completes.

If no verified backup satisfies the objective, report the actual recovery
point; never silently substitute an unsigned or untested archive.
