# Installation and local development

For a synthetic local environment, install Node (see `.nvmrc`), npm, Docker
Engine and Docker Compose, then run `scripts/dev-stack.sh up` from the repository
root. Open `http://127.0.0.1:5173`. Setup, readiness, fixture profiles and explicit
cleanup are described in the [local stack runbook](scripts/local-sample/README.md).
Production configuration is documented in [README.md](README.md) and
[.env.example](.env.example); the local helper does not import production settings.

## Deployment and upgrades

Use the [2.0 upgrade guide](docs/upgrade-2.0.md) for the actual starting revision,
role/schema migration order, owner sessions, bounded backfills and rollback
limits. The [release runbook](docs/runbook-release-rollback.md) governs approved
image deployment. Local setup above uses invented state and does not import
production credentials.

The following timer runbook applies to an existing configured VPS deployment,
after its normal installation and migration prerequisites have been satisfied.

## Owner baseline auto-rebaseline runbook

The timer checks the local backend readiness endpoint every ten minutes. It
only considers a settled `aclDesiredGeneration` mismatch in `aclMode=apply`.
The associated rendered/applied generation and ACL readback differences can
accompany that generation change; unrelated inventory mismatches, malformed
readiness data, and unsettled ACL state stop the run and are logged to the user
journal.

When a generation mismatch is eligible, the wrapper first runs
`scripts/refresh-owner-baseline.sh` without `--apply` in a private temporary
export directory. It requires a successful exit and the script's strict
`baselineOk: true` summary before running the script with `--apply --recreate`.
The refresh script validates again before changing `.env` or recreating the
backend. The `--check` mode only reads `/readyz`; it never calls the refresh
script, starts a container, writes a baseline, or applies a change.

The repository has no existing systemd unit convention. Units are kept under
`deploy/systemd/user/` and installed as user units on the VPS. Ben should
perform these steps after the refresh script dependency is present and this
branch has been relayed/merged into the VPS checkout.

## Install and enable on the VPS

```sh
cd /home/ben/ukmesh/meshcore-analytics
test -x scripts/refresh-owner-baseline.sh
npm --prefix backend run build
node backend/dist/tools/autoRebaseline.js --check
command -v flock
install -d -m 700 "$HOME/.config/systemd/user"
install -m 600 deploy/systemd/user/owner-baseline-autorebaseline.service "$HOME/.config/systemd/user/"
install -m 600 deploy/systemd/user/owner-baseline-autorebaseline.timer "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user enable --now owner-baseline-autorebaseline.timer
```

The service uses `/usr/bin/flock` with a lock in the user's runtime directory;
overlapping timer invocations exit with status 75 and are treated as a no-op.
Logs go to journald under `owner-baseline-autorebaseline.service`.

## Verify and operate

```sh
systemctl --user status owner-baseline-autorebaseline.timer
systemctl --user list-timers owner-baseline-autorebaseline.timer
journalctl --user -u owner-baseline-autorebaseline.service --since today
```

To inspect the next decision without running the refresh script:

```sh
cd /home/ben/ukmesh/meshcore-analytics
node backend/dist/tools/autoRebaseline.js --check
```

To disable future checks, Ben can run:

```sh
systemctl --user disable --now owner-baseline-autorebaseline.timer
```

The timer is not installed or enabled by this change. A validated automatic
refresh invokes the existing script's `--apply --recreate` path, which updates
the baseline path in `.env` and recreates the backend. Inspect the journal and
`/readyz` after that run using Ben's normal operations procedure.
