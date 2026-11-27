# Move from main to UKMesh 2.0 in DEV

Use this guide to start the 2.0 DEV stack or rehearse an upgrade with retained
`main` data. The clean DEV stack can run today. An upgrade with retained data
needs branch reconciliation and a restore rehearsal first. F2, the rehearsal
against the actual deployment, and VPS cutover are still deferred.

## What changes

This comparison uses `main` at `1dac8d20` and `ukmesh-dev` at `ea33da99`, the
merge of PR #264. The branches diverged at `d17f2577`. At those revisions,
28 commits were unique to `main` and 342 to `ukmesh-dev`. Recheck both tips
before planning an upgrade.

| Area | `main` | `ukmesh-dev` |
| --- | --- | --- |
| Versions and setup | Packages identify as 1.0.0. Host Node 20. Local Compose uses a configured `.env`. | Packages identify as 2.0.0. Host Node 26. `scripts/dev-all.sh` supplies local configuration and starts the full lean stack with built frontends. |
| Browser entry points | Local Compose serves the main website on port 3004, the app on 3003, and the test site on 3006. | The DEV launcher uses 5173, 3003, and 3005. The optional DEV gateway's website on 8083 integrates CoreScope Live, Beacon channels, and the guided Health Check. |
| Companion sources and storage | Health Check uses a separately configured source ref. | UKMesh pins CoreScope, Beacon Server, Beacon Web, and Health Check as Git submodules. Beacon 2 has its own PostgreSQL database. CoreScope owns a separate SQLite store and ingestor. |
| RF coverage | HopReach computes Standard and Precision on the stack host. | The VM runs the coordinator and publishes tiles. An external worker computes Standard and Precision. DEV can qualify the workflow with a mock worker. |
| Database | The branch carries `136_node_status_radio.sql` after the shared migration history. | The branch carries `136_public_snapshot_visibility.sql` and additional migrations through 156 at this revision. Region and spam publication require current privacy-generation evidence. |

Fetch both remote refs explicitly if the original checkout was cloned with
`--single-branch`.

```bash
git fetch --no-tags origin \
  refs/heads/main:refs/remotes/origin/main \
  refs/heads/ukmesh-dev:refs/remotes/origin/ukmesh-dev
git log --left-right --cherry-pick --oneline origin/main...origin/ukmesh-dev
git diff --name-status origin/main origin/ukmesh-dev -- backend/src/db/migrations
```

## Start a clean DEV stack

Use an isolated DEV host for side-by-side testing or arrange a maintenance
window for its old local stack. Checkouts share the host's Docker storage and
ports, and the launcher uses fixed project names and loopback ports. Free
those ports before starting. Install Docker Engine with Compose, Git, the
Node version in `.nvmrc` with npm, Python 3, `curl`, `flock`, and `rg`.
The LAN gateway also needs `ip`. Use normal Docker group access. Keep at
least 10 GiB of disk free for qualification and allow extra space for images
and restore copies.

```bash
git clone --branch ukmesh-dev --single-branch https://github.com/gadgethd/ukmesh.git ukmesh-dev
cd ukmesh-dev
git rev-parse HEAD
bash scripts/dev-all.sh init
git submodule status
bash scripts/healthcheck-source.sh --verify
bash scripts/dev-all.sh build
bash scripts/dev-all.sh start
bash scripts/dev-all.sh status
```

For an existing clean `ukmesh-dev` checkout, fetch and fast-forward from
`origin/ukmesh-dev`. Check the root and submodules for local edits before
updating. Run `git submodule sync --recursive` and
`git submodule update --init --recursive` to check out the recorded pins.
Run the build and start commands above after verifying the pins.

The launcher prepares or resumes its owned DEV stores. It runs the migration
job and checks its exit before starting runtime services. It supplies fixture
credentials, the local MQTT broker, and local tagging and alert endpoints.
To feed the broker from the private archive on the VM that holds it, follow the optional
[DEV replay instructions](companion-development.md#replay-the-local-archive).
Stop replay before selecting a qualification fixture. Keep the archive and its
processed output in local ignored storage.

Open the loopback website at <http://127.0.0.1:5173>. To exercise the
integrated tools, start the DEV gateway with the original DEV stores selected.
It binds to loopback when started without an address. For another device on a
trusted LAN, supply the private IPv4 address assigned to the DEV host.

```bash
bash scripts/dev-all.sh lan start
bash scripts/dev-all.sh lan status
```

Open <http://127.0.0.1:8083> for the integrated website and
<http://127.0.0.1:8080> for the DEV hub. On the integrated website, verify
`/live-map` opens CoreScope in Live mode, `/feed` shows Beacon channels,
`/feed/packets` opens the UKMesh packet feed, and `/health-check` opens
the guided Health Check inside the website. The original UKMesh dashboard is
available on port 3003 or through the gateway on port 8081. Keep the gateway
private and the public tunnel off. The [DEV UI release guide](dev-ui-release.md)
covers themed assets and rollback.

## Rehearse an upgrade with retained main data

1. Record the actual running `main` revision, image IDs or digests, Compose
   project and overrides, broker settings with secrets redacted, database
   targets, migration ledger and owner authorization inventory. Keep the old
   image set, credentials in private storage, and existing hashed browser
   assets. Create and verify a backup. Use the
   [backup and restore runbook](runbook-backup-restore.md) for a configured
   `main` stack or `bash scripts/dev-all.sh qualify backup` for the original
   stores of an already managed DEV stack.
2. Reconcile the current `main` changes on a branch based on `ukmesh-dev`.
   Review radio telemetry, owner session revocation, identity and stats
   backfills, alerting, tagging, analytics, and dependency and CI changes.
   Check the actual code and feature behavior on both sides. Qualify the
   reconciled revision on a fresh fixture and on a restored copy.
3. Resolve the migration prefix conflict before using retained data. The two
   branches each use prefix `136` for a different file. Keeping both files
   unchanged in one migration directory makes the runner reject the pair.
   On each retained database, inspect the migration ledger with a read-only
   session:

   ```sql
   SELECT name, checksum_sha256
   FROM public.schema_migrations
   WHERE name LIKE '136!_%' ESCAPE '!'
   ORDER BY name;
   ```

   Preserve applied filenames and checksums. Review the compatibility or
   forward-migration design against both ledgers. Prove that the result preserves
   radio telemetry and certifies region and spam data against the current
   privacy generation. See the
   [migration contract](../backend/src/db/migrations/README.md).
4. Restore the backup into isolated DEV stores with separate credentials.
   Keep UKMesh and owner auth on their intended separate databases. Bootstrap
   roles, apply the explicit pending migrations, and provision the owner schema
   before starting candidate writers. The migration job uses the built
   backend's `db:migrate` script with the restored database's
   `MESHCORE_MIGRATE_DATABASE_URL`. Use the bounded identity and read-model
   backfills, then verify privacy convergence and region publication. Use a
   fresh database for Beacon 2 if the previous deployment used Beacon 1.
   Follow the [2.0 rehearsal procedure](upgrade-2.0.md#rehearsal-order) and
   [owner authorization instructions](upgrade-2.0.md#owner-authorization-and-configuration).
5. Validate the restored candidate using the checks below. Rehearse starting
   the exact prior images against the migrated copy, with candidate writers
   stopped. Separately restore the verified backup into new isolated stores
   and verify that the prior images can use them. Keep both results with the
   source SHAs and image IDs.

## Verify before switching DEV

```bash
curl --fail --silent --show-error http://127.0.0.1:3100/readyz
curl --fail --silent --show-error http://127.0.0.1:3102/api/healthz
curl --fail --silent --show-error http://127.0.0.1:3103/api/v1/info
bash scripts/dev-all.sh qualify inventory
bash scripts/dev-all.sh qualify smoke
```

- Confirm the migration and other one-shot jobs exited successfully. Check that
  resident services are ready and doing their expected work. Retain the
  inventory and smoke reports under ignored `plans/lean-runtime/qualification/`.
- Send eligible DEV traffic and check accepted MQTT packets, committed
  observations and live updates in UKMesh and CoreScope. Check Beacon displays
  approved chat and that Packet Feed can resolve paths with sufficient evidence.
  Verify search aliases, region scopes and privacy changes on restored data.
- Use a DEV owner account to test login, a confirmed save, and revocation.
  Run a guided Health Check with a fresh DEV test packet. Check metrics, logs
  and local alert delivery.
- Verify desktop and mobile navigation, cached assets in an already open tab,
  and restoration of the previous UI. The
  [browser rollback procedure](upgrade-2.0.md#browser-and-lifecycle-rollback-limits)
  covers the retained service-worker caches.
- Verify Standard and Precision publication through the mock worker on this VM.
  Keep real GPU pairing and measurement as separate evidence. Repeat the
  matched reads and 40-minute MQTT soak on the exact candidate before marking
  full qualification complete. Use the
  [qualification handoff](lean-runtime-handoff.md) for workload and resource gates.

## Roll back

- For a UI-only change, use `bash scripts/dev-all.sh ui rollback` after
  confirming the prior sealed release with `ui status`. That command switches
  images and frontend mounts. Database recovery has its own procedure.
- To stop the managed candidate and keep its data, run
  `bash scripts/dev-all.sh lan stop` if the gateway is enabled, then
  `bash scripts/dev-all.sh stop`. Restart the prior DEV stack with its recorded
  configuration and original stores.
- If the prior images cannot use the migrated schema, restore the pre-upgrade
  backup into fresh stores and start the prior images against those targets.
  Keep the migrated copy and record its last accepted traffic. Use the
  [backup and restore runbook](runbook-backup-restore.md) for the recovery sequence.
