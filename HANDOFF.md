# Mission C5a handoff

Status: done  
Branch: `fix/ukmesh-highs-a`  
Push state: not pushed; coordinator relays this branch.

## Commits

- `865b5dd` — `fix(api): allowlist public node status fields`
- `794ee7d` — `fix(ops): resolve split infrastructure containers exactly`

## Changes

- #42: `/node-status/latest` and `/mqtt-nodes` return explicit public fields. Raw status JSON, hardware/firmware details, and unknown fields are omitted. Existing historical rows remain unchanged; no database sanitisation is needed because these public responses now use field-by-field projections. Owner-authenticated status handling is unchanged.
- #46: Added a shared resolver for exact `timescaledb`, `redis`, and `mosquitto` services in an external Compose project/file. Backup, release migration verification, provisioning, network relabeling, and chunk maintenance use it. Backup archives the Compose file it actually selected. The isolated restore drill continues to target its generated disposable containers on a private network.
- Updated the privacy and operations runbooks and added a non-destructive resolver dry-run to CI.

## Tests and checks

- `cd backend && TMPDIR="$OLDPWD/.tmp-c5a" node --import tsx --test src/api/routes/misc.test.ts src/api/routes/nodeStatus.test.ts` — passed, 3/3.
- `cd backend && npm run typecheck` — passed.
- `TMPDIR="$PWD/.tmp-c5a" npm --prefix scripts test --cache="$PWD/.tmp-c5a/npm-cache"` — passed (observer-key, resolver dry-run, and newuser tests).
- `TMPDIR="$PWD/.tmp-c5a" scripts/test-replace-container.sh` — passed.
- `bash -n` on modified shell scripts and `git diff --check` — passed.
- `scripts/test-vacuum-maintenance.sh` was not run because it starts a Docker test container, prohibited by the VPS constraints. CI retains this existing integration test.

Backend and scripts dependencies were installed from their lockfiles with npm caches inside this worktree.

## Gated items for Ben

- Run the real encrypted backup and isolated restore drill on an approved operations host before relying on the updated backup chain. Follow [the backup/restore runbook](docs/runbook-backup-restore.md).
- Deployment and any production migration remain for the coordinator/Ben. No live database writes or service/container operations were performed here.

## Workspace note

The temporary npm cache directory `.tmp-c5a/` remains untracked in this worktree. The cleanup command was rejected by the execution guard with: “rm -f style commands are not permitted. Use a safer approach”.

OpenViking and Tokensave tools were not exposed in this session, so shared-memory and semantic-index actions were unavailable.
