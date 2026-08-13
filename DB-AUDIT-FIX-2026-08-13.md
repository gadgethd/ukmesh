# ukmesh database reset-readiness fix report — 2026-08-13

## Verdict

**Fresh-DB acceptance: PASS.** A database created from empty storage on the
CI/restore pinned TimescaleDB 2.25.1 digest applied the complete 49-file chain
(the 48 audited entries plus `051_reset_readiness.sql`), passed the full object
assertions, and treated a second migration run as a no-op.

**Production reset authorization: BLOCKED only on independent off-host
publication.** The current encrypted local backup and isolated restore are
verified. The VPS has no distinct mounted off-host target, so the reset must not
start until Ben mounts and attests one and `sync-latest.sh` reports `verified`.
The local sync drill is useful evidence for the script, but is not represented
as off-host evidence.

No production migration, SQL write, service restart, or production `compose up`
was performed. The only live-stack operations were read-only database dumps,
supported Redis/Mosquitto persistence checkpoints, and the read-only owner
inventory export. `.env` grant values were not changed.

## Commits

- `e7d16d4` — `fix(db): make reset schema and lifecycle reproducible`
- `c79f71e` — `fix(owner): preserve authorization inventory across reset`
- `e3a8ba3` — `fix(backup): restore split-stack recovery workflow`

These commits are local only and were not pushed.

## Finding status

### F1 / G1 — identity materialized views: FIXED (`e7d16d4`)

Migration 051 safely replaces fresh ordinary views with populated materialized
views, while treating already-materialized live objects as no-ops. It creates
the unique sighting `(node_id, network)` and link `(node_a_id, node_b_id)`
indexes, the reversed sighting index, and both link endpoint indexes. Fresh-DB
tests assert `pg_matviews`, all five definitions, and successful concurrent
refreshes of both views.

### F2 / G5 — split-stack backup and recovery: REVISED (`e3a8ba3`)

`backup.sh` now resolves TimescaleDB, Redis, and Mosquitto through the explicit
`meshcore-infra` project/directory. The encrypted recovery archive contains the
base/live/CI/infra-client/phase-4 app Compose files and a separate protected
infra configuration archive. Broker config is captured through its container,
and restore streams verified state into isolated named volumes, avoiding host
permission/bind differences.

`sync-latest.sh` performs signed-receipt selection, full archive checksum
verification, partial-directory staging, atomic publication, a checksum
manifest, and post-publication verification. A same-filesystem functional drill
returned `verified` and its exact test directory was deleted. This finding is
REVISED rather than FIXED because no genuinely off-host mount exists yet.

Canonical local backup evidence (secret recovery material, not committed):

- ID: `backup-20260813T031400Z-2c1dd39b7173`
- completed: `2026-08-13T03:18:09Z`
- live schema captured: 50
- encrypted bytes: 1,196,412,781
- SHA-256: `6466077779be65d3f843843b3fbf57cd33abad671391c5744e93d3a0fe1911a7`
- datasets: analytics, owner auth, Redis, Mosquitto, app configuration, infra configuration
- exact-artifact isolated restore: PASS on the pinned 2.25.1 digest
- restored/current schema: 51; RTO: 414s; RPO at verification: 527s
- signed receipt: `backup-receipts/latest.json` (signature independently verified)

The receipt's `source_revision` is the mandated pre-fix HEAD `2c1dd39` because
all gates were run before the local commits. The bundled scripts and migrations
are the exact working-tree content that passed the drill.

### F3 / G3 — owner grant preservation: REVISED (`c79f71e`, `e3a8ba3`)

The protected, gitignored baseline is
`/home/ben/ukmesh/backups/owner-grants-2026-08-13T021734Z.json` (mode 0600,
content hash `2d795007c523316cb4967262f89020813c213c727653fdabdfcd130fa0463fd6`).
It contains every reviewed active grant: 27 accounts, 44 grants, 5
`operator-config`, 36 `operator-database`, and 3 legacy/null-method. Desired,
rendered, and applied ACL generations match and `lastError` is null.

`owner-auth:inventory` exports/loads a checksum-bearing baseline and
`--require-complete` rejects count, method, config generation, ACL generation,
ACL readback, or last-error drift. `/readyz` applies the same baseline gate.
The recovery bundle includes `owner-auth.dump`; the runbook restores that dump,
mounts the baseline read-only, validates it before public admission, and leaves
`OWNER_MQTT_USERNAME_MAP` unchanged.

The three legacy/null-method grants are deliberately preserved, not silently
dropped or promoted. Ben must choose promote-to-`operator-database` or revoke;
until then the recorded disposition is
`preserve-unmodified-pending-owner-review`. This outstanding policy choice does
not risk grant loss during a restore.

### F4 / G4 — packet-path privacy transitions: FIXED (`e7d16d4`)

Read/derivation SQL now evaluates path privacy from current privacy tables.
Because compatibility flags remain materialized, migration 051 also adds
ingest and node-transition rematerialization triggers plus receiver, source,
and path-prefix transition indexes. Integration tests cover public→private and
private→public both before and after source-packet deletion.

### F5 / G2 — reproducible compression/capacity state: FIXED (`e7d16d4`)

Migration 051 enables and schedules compression for all four hypertables:

- `packets`: compress after 14 days; hard row retention at 30 days
- `packet_paths`: compress after 14 days; **no retention policy**
- `node_status_samples`: compress after 14 days
- `node_neighbor_samples`: compress after 1 day (7-day retention)

The source registry uses the same values and keeps compression-only targets
separate from deletable targets. `packet_paths` is segmented by network and
ordered by descending time. Health metrics and alerts cover 30-day path row
rate, bytes-per-row/total size, and overdue uncompressed chunks. Fresh tests
assert exact Timescale settings/jobs and the packet-path no-retention invariant.

### F6 — shared observation identity and post-retention reader: FIXED (`e7d16d4`)

The classified source CTE generates one UUID used by both packet inserts.
Migration 051 repairs recent natural-key mismatches. Multibyte reconstruction
and observation-ID backfill now source durable rows from `packet_paths`.
Integration proves shared IDs, idempotency, and successful fact reconstruction
after the source packet is deleted.

### F7 — reachable row-table retention: FIXED (`e7d16d4`, `c79f71e`)

The closed worker allowlist, lifecycle declarations, Compose/default env wiring,
and tests now include `observer_registration_requests` at 365 days with an
explicit terminal-only predicate (`rejected`, `expired`, `provisioned`) and
`operator_audit_events` at 730 days. Migration 051 adds bounded-delete indexes.

### F8 — indexed deterministic decryption retention: FIXED (`e7d16d4`)

Migration 051 adds `packet_decryptions_created_at_idx`. The bounded worker
orders by timestamp, `tableoid`, and `ctid`, and deletes the exact selected
physical rows. Integration covers expired/retained rows and verifies the index
appears in the execution plan.

### F9 — full fresh-DB/CI assertions: FIXED (`e7d16d4`)

Fresh integration now checks all hypertables and chunk intervals, compression
and retention jobs, packet-path constraints/indexes/no-retention, materialized
views/indexes/concurrent refresh, every checksummed ledger row, rollback on
ledger failure, and second-run idempotency. Ingest coverage uses a non-null
two-byte path and asserts observation identity, nullable observer idempotency,
privacy transitions, packet-retention survival, and path-fact rebuilding. The
Compose MQTT fixture/readback now includes `packet_paths`.

The audited request said 48 ledger entries; the correct current assertion is
49 because migration 051 was added and all previous 48 remain asserted.

### F10 — migration transaction ownership: FIXED (`e7d16d4`)

Migration 050 no longer contains file-level `BEGIN`/`COMMIT`. The runner owns
the transaction. An injected failure at the migration-050 ledger insert proves
its schema and policy changes roll back before a normal resume.

The approved original migration-050 checksum is handled as one explicit shipped
content revision; arbitrary drift still fails closed.

### F11 — hard 30-day packet bound: FIXED (`e7d16d4`)

Packet chunks are one day and the closed health-worker target performs bounded
exact-row cleanup at 30 days. The integration test asserts the oldest retained
packet lies inside the boundary. Privacy/lifecycle documentation now describes
the hard bound rather than a 30–37-day chunk-bounded approximation.

### F12 — telemetry-off coverage: FIXED (`e7d16d4`)

Tests cover no `window`, `?telemetry=off`, non-off values, suppressed `fetch`,
and suppressed error/rejection/console handler installation. No production
frontend source was changed.

## Required pre-reset gates

1. Identity materialized-view migration: **PASS**.
2. Current post-split backup and independent restore: **PASS locally**;
   independent off-host publication: **BLOCKED — mount required**.
3. Preserve every owner grant: **PASS**; all 44 are in both `owner-auth.dump`
   and the protected baseline. Legacy disposition awaits Ben's policy decision.
4. Packet-path privacy rematerialization/current read derivation: **PASS**.
5. Migration-defined compression/capacity state plus empty-volume assertions on
   TimescaleDB 2.25.1: **PASS**.

Do not reset until gate 2's off-host sub-gate passes.

## Acceptance evidence

- Backend: **314/314 PASS**, TypeScript typecheck PASS, OpenAPI contract PASS
  (62 API and 11 operator routes).
- Frontend: **96/96 PASS**, production build PASS; telemetry cases included.
- Fresh pinned TimescaleDB integration: **2/2 PASS**, no skips.
- App live-overlay Compose config: PASS.
- `meshcore-infra` Compose config: PASS.
- Prometheus rules: **24 rules PASS** under `promtool`.
- Backup receipt signature and 1.196 GB archive SHA-256: PASS.
- Exact canonical artifact restore: PASS; migration/readiness/integrity/owner
  lookup checks all `passed`.
- Second migration run: no-op; ledger count 49 with no null checksums.
- Throwaway containers, networks, volumes, images, decrypted data, and local
  sync test copy: removed.
- `.audit-input`: deleted.

## Ben decisions and remaining operation

1. Mount the true off-host target at `/mnt/offsite/meshcore`, create its reviewed
   `.meshcore-offsite-target` attestation, copy the public receipt verification
   key there or into the reviewed key location, then run `sync-latest.sh` without
   `BACKUP_SYNC_ALLOW_LOCAL_TEST`. Verify `latest.sha256` independently.
2. Move/recoverably escrow the locally generated drill keys currently protected
   under `/home/ben/ukmesh/backup-keys`; do not rely on the same VPS as the sole
   key store.
3. Decide whether the three preserved legacy/null-method grants should be
   promoted to `operator-database` or revoked. The reset path preserves them in
   either case until an explicit decision.
4. Compression is resolved at 14 days for packets, packet paths, and status
   samples, and 1 day for neighbor samples. Packet retention is an exact hard
   30-day bound. Change these only through a reviewed migration and matching
   lifecycle registry update.

No crontab was installed. If Ben approves `/mnt/offsite/meshcore` as the actual
mount and the current key locations, these are the exact proposed lines:

```cron
17 2 * * * /usr/bin/flock -n /home/ben/ukmesh/backups/.backup.lock /usr/bin/env BACKUP_OUTPUT_DIR=/home/ben/ukmesh/backups BACKUP_ALLOW_LOCAL_STAGING=true BACKUP_ENCRYPTION_CERT=/home/ben/ukmesh/backup-keys/encrypt.pem BACKUP_RECEIPT_SIGNING_KEY=/home/ben/ukmesh/backup-keys/receipt-signing.pem MESHCORE_INFRA_DIR=/home/ben/ukmesh/meshcore-infra MESHCORE_INFRA_PROJECT_NAME=meshcore-infra /home/ben/ukmesh/meshcore-analytics/scripts/backup.sh >>/home/ben/ukmesh/backups/backup.log 2>&1
47 3 * * * /usr/bin/flock -n /home/ben/ukmesh/backups/.sync.lock /usr/bin/env BACKUP_SOURCE_DIR=/home/ben/ukmesh/backups BACKUP_SYNC_TARGET_DIR=/mnt/offsite/meshcore BACKUP_RECEIPT_VERIFY_KEY=/home/ben/ukmesh/backup-keys/receipt-verify.pem /home/ben/ukmesh/meshcore-analytics/scripts/sync-latest.sh >>/home/ben/ukmesh/backups/sync.log 2>&1
```

Before installing them, Ben should also approve backup-generation pruning;
neither script deletes historical recovery points by design.
