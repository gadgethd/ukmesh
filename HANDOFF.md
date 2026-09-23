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

---

# Mission C5c handoff

Status: complete locally
Branch: fix/ukmesh-auth-users
Push state: not pushed; coordinator relays these commits.

## Commits

- 9ce8633 — security: classify channel keys and baseline reviewed findings
- b50ecfa — security: revalidate MQTT auth and revoke owner sessions
- c1bc7c8 — test(ops): cover concurrent newuser grant updates
- a6b7a49 — fix(ops): preserve executable newuser test
- 771fc6f — fix(ops): cover partial newuser rollback failures
- 2be3357 — fix(owner): preserve password whitespace during login

## Changes

- #50: Classified the 41 committed recovered/community-known channels as intentionally public; environment-supplied keys are classified confidential. Replaced broad Gitleaks regex exceptions with 47 exact reviewed fingerprints and added full-history scans on push/PR plus a scheduled scan. A synthetic unclassified key was still rejected by Gitleaks.
- #51: Removed positive MQTT credential caching. Existing credential-generation checks now fail closed on Redis read errors. Added owner-auth:revoke and dist/tools/revokeOwnerSessions.js for immediate session revocation after external MQTT password resets; the seven-day session lifetime was already in place.
- #52: Added a two-process regression test that adds a grant during discovery and verifies the first process preserves it after reacquiring the lock. The reread/merge logic was already present.
- #53: Marked credential, environment, and database mutations before issuing them so an interrupted/failed command reaches rollback. Added failures after credential, environment, database, and ACL changes.
- #58: Kept username normalization and preserved password whitespace through browser and backend login validation. Added frontend and API tests.

## Main overlap and audit note

Fetched origin/main before editing; it had no changes to these target files. This worktree remains based on the open #99 resolver branch. Existing session generations, seven-day expiry, newuser map reread/EXIT trap, and backend password preservation were already present in the base; this mission closed the remaining gaps and added regression tests.

The brief cites a historical broker credential. Gitleaks 8.30.0 scanned all 476 commits reachable from the fetched refs and found 47 historical hits, all reviewed as public channel values, test fixtures, or generated revision identifiers. It found no separate broker credential in those refs. The exact baseline does not allowlist an unidentified credential; Ben should still rotate the credential identified by the audit and confirm its source/history.

## Tests and checks

- cd backend && node --import tsx --test src/mqtt/channelRegistry.test.ts src/owner/mqttCredentialVerifier.test.ts src/owner/ownerSession.test.ts src/api/routes/owner.test.ts — passed, 7/7.
- cd backend && ./node_modules/.bin/tsc --noEmit — passed.
- cd frontend && ./node_modules/.bin/tsx --test src/pages/owner/ownerPortalModel.test.ts — passed, 5/5.
- cd frontend && ./node_modules/.bin/tsc --noEmit — passed.
- cd scripts && NPM_CONFIG_CACHE="$PWD/.tmp/npm-cache" TMPDIR="$PWD/.tmp" npm test — passed (observer-key, infrastructure resolver, concurrent newuser, and rollback tests).
- gitleaks 8.30.0 git --config .gitleaks.toml --log-opts=--all --redact --no-banner — passed; 476 commits scanned, no unclassified findings.
- git diff --check — passed.

## Gated items for Ben

- Rotate the MQTT broker credential identified in the audit. No broker/service/database operations were performed here.
- Confirm all 41 committed channel values are intentionally public. Move any confidential value to managed environment configuration before deployment.
- After deploying the backend tool, run node dist/tools/revokeOwnerSessions.js <mqtt-username> immediately after each MQTT password reset. Do not consider the reset complete unless the command succeeds.
- Coordinate merge/push and deployment. Nothing was pushed or merged from this host.

## Workspace note

Backend/frontend dependencies were installed from lockfiles into ignored node_modules. I initially ran those two npm ci commands without setting NPM_CONFIG_CACHE, so npm used its default host cache outside the worktree; subsequent script dependency installation used a worktree-local cache. No live checkout, .env, service/container, or database was changed, and no push was made.
