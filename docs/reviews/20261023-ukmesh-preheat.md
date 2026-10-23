# UKMesh preheat — 2026-10-23

## Scope and basis

Completed the local implementation for HermesMind #438 and #424 from
`/home/ben/.review-notes/codex-burn-20261023/BRIEF-ukmesh-preheat.md`.
Branch: `burn/ukmesh-preheat-20261023`. A fresh `git fetch origin main`
confirmed the starting HEAD and origin/main were both
`73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.

No deployment, VPS access, service restart, live-data migration, secret access,
Hermes configuration change, other-worktree change or agent delegation occurred.
Docker deployment and reconciliation commands were exercised only through strict
test doubles. The separate Compose validation used synthetic values, an empty
Docker configuration directory and a deliberately nonexistent daemon socket.

## #438 — observable, optionally parallel prewarm

- Added structured start, 30-second progress and terminal log records. Each
  reports total/completed owners, started/completed/total nodes,
  total/completed/remaining node-window refreshes, refreshed/skipped/failed
  outcomes, elapsed milliseconds, completed refreshes per second and ETA in
  seconds. ETA is null until throughput is measurable and zero for no remaining
  refreshes. The timer continues reporting during an in-flight slow refresh and
  is cleared when the pass finishes or fails. Interrupted passes say `pass stopped`.
- Added `OWNER_LAST_HOP_PREWARM_CONCURRENCY`, default **1**, hard cap **2**.
  Invalid/nonfinite/nonpositive values select one worker; finite positive values
  are rounded down before applying the cap. Workers process nodes, retaining
  sequential 7d/30d windows and the delay after each node. The environment option
  is wired through backend startup, Compose and `.env.example`; Compose also
  forwards the already documented interval and node-delay settings.
- Default database work retains account/node/window order, active-owner
  filtering, ownership arguments, 20-second initial timer, fixed pass interval,
  overlap suppression, min-age threshold, per-window failure isolation and
  per-node delay, including the final node's delay. Stop prevents new node
  admission while already started nodes finish their windows, as before.

The new 13-test prewarmer suite uses mocked timers and controllable refreshes.
It checks default ordering/delay and both windows, distinct ownership arguments
for a shared node, outcome accounting, timed rate/ETA while work is pending,
zero-throughput/empty passes, concurrency cap and out-of-order owner completion,
configuration normalization, overlap suppression, enumeration recovery and
shutdown admission/timer cleanup.

## #424 — actionable Compose adoption failures

The base already had `check-compose-adoption.sh` wired into
`replace-container.sh` for the selected service and backend, before signatures,
pulls, migrations and replacement. Preserved that ordering and its requirements:
exactly one current container, matching project/service/physical-working-directory
labels and a config-file list containing this checkout's base Compose file.

Enhanced rejection messages with an exact Bash-quoted reconciliation command:
change to the authoritative checkout, export the project and any explicit
Compose-file selection/separator, remove the identified conflicting container(s),
run `up -d --no-deps --no-build` for that service and rerun the preflight. If the
container is genuinely absent, no remove command is emitted. When Compose cannot
select a current container, a read-only exact-name lookup finds an unlabelled or
stopped conflict, including custom `container_name` values. Regex punctuation in
names is escaped. Lookup/inspection failures stop with exit 69 and do not propose
removing an unverified container; adoption drift remains exit 65.

The guard never executes the repair. The runbook explains checking image pins,
environment, mounts and Compose files before an operator runs the command in an
approved maintenance window, and retaining the live external-network overlay.
No automatic adoption, network replacement or retry loop was added.

Existing deployment drills now assert actionable errors and still verify no
signature check, pull, migration, replacement or release receipt occurs on an
adoption rejection. Nine additional drills verify read-only behavior, Docker
and configuration errors, named orphans, custom names/projects, absent containers,
and execution of the printed repair against fake Docker. The latter unset the
Compose environment first to prove the printed command restores the project,
file selection and custom separator; paths contain spaces. These drills run
from the existing CI entry point, `scripts/test-replace-container.sh`.

## Verification

All final commands below exited **0**. Commands start at the worktree root
unless their working directory is explicitly shown.

| Exact command | Result |
| --- | --- |
| `cd backend && node --import tsx --test src/owner/ownerLastHopPrewarm.test.ts src/owner/ownerService.test.ts src/owner/ownerDashboard.test.ts` | **20/20 pass**, no skipped/cancelled tests. |
| `cd backend && npm run typecheck && npm run build` | Both TypeScript checks/build pass. |
| `bash -n scripts/check-compose-adoption.sh scripts/replace-container.sh scripts/test-check-compose-adoption.sh scripts/test-replace-container.sh` | Shell syntax passes. |
| `mkdir -p .ukmesh-tools/tmp && TMPDIR="$PWD/.ukmesh-tools/tmp" bash scripts/test-replace-container.sh` | **16/16** release/adoption drills plus **9/9** guard/printed-repair drills pass. |
| `bash scripts/test-check-compose-adoption.sh` | Standalone **9/9** pass. |
| `git diff --check` | No whitespace errors. |

Daemon-free Compose validation, using only fixture values:

```bash
mkdir -p .ukmesh-tools/tmp/docker-cli
env -i PATH="$PATH" DOCKER_CONFIG="$PWD/.ukmesh-tools/tmp/docker-cli" \
  DOCKER_HOST=unix:///no-preheat-daemon.sock \
  POSTGRES_PASSWORD=fixture-only POSTGRES_USER=meshcore \
  REDIS_PASSWORD=fixture-only JWT_SECRET=fixture-only \
  OPERATOR_SITE_TOKEN=fixture-only MQTT_PASSWORD=fixture-only \
  GRAFANA_ADMIN_PASSWORD=fixture-only CLOUDFLARE_TUNNEL_TOKEN=fixture-only \
  HEALTHCHECK_SOURCE_REF=0123456789abcdef0123456789abcdef01234567 \
  ANUBIS_ED25519_PRIVATE_KEY_HEX=0000000000000000000000000000000000000000000000000000000000000001 \
  docker compose --env-file /dev/null -f docker-compose.yml \
    -f docker-compose.live.yml config --format json \
    > .ukmesh-tools/tmp/preheat-compose.json
python3 -c 'import json; c=json.load(open(".ukmesh-tools/tmp/preheat-compose.json")); e=c["services"]["backend"]["environment"]; assert e["OWNER_LAST_HOP_PREWARM_CONCURRENCY"] == "1"; assert e["OWNER_LAST_HOP_PREWARM_INTERVAL_MS"] == "900000"; assert e["OWNER_LAST_HOP_PREWARM_NODE_DELAY_MS"] == "150"; assert c["networks"]["default"]["external"] is True; print("Compose defaults and live overlay validated without daemon access")'
```

The first fixture-only Compose attempt omitted `HEALTHCHECK_SOURCE_REF` and
failed interpolation. Supplying that required fixture value produced the passing
result above; no source workaround or secret was needed. Node emits its normal
experimental MockTimers warning; no test failed.

## Deliberately excluded, open questions and remaining work

- No production load, cold-pass duration or DB throughput claim is made. Leave
  concurrency at one until an operator evaluates two workers under actual load.
  No refresh timeout, cancellation, cache rewrite or scheduling change was added.
- Existing `ownerService.ts` cache keys contain window and selected node but not
  the full owned-node set; `ownerRepository.fetchLastHopStrength` uses that set
  to exclude owned nodes. Shared nodes with differing ownership sets therefore
  warrant a separate cache-scoping/in-flight-work review. This predates the
  change; the warmer continues passing each account's original ownership set.
- Live Compose adoption and repair remain unverified and require an independently
  authorized maintenance window. No such operator action is needed to review
  or merge this local patch. No push, PR or deployment was performed.
- Stretch #454: repository-only search finds the CI database configuration and
  query-local parallelism settings, but no live PostgreSQL `/dev/shm` allocation
  declaration. The external infrastructure project/VPS was not inspected; an
  actual live shared-memory audit remains outside this worktree's evidence.
- TokenSave returned no matching symbols from its stale index and no local
  `.tokensave/tokensave.db` exists here. Source files and current command output,
  rather than historical review claims, were used as authoritative evidence.

The implementation, tests, runbook and this handoff are committed together.
