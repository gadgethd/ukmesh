# Mission C5b — durability and retention handoff

## Status

Done: repository changes are committed locally. No services or containers were started, stopped, or restarted; no live database writes or deployment configuration edits were made. Nothing was pushed.

## Commits

- #43 durable MQTT QoS 1 ingestion: `3eb132dfd9c9a009e1ca66e090a3e1f4c6bde9c4` — `fix(mqtt): ack QoS1 after durable persistence`
- #45 bounded telemetry lifecycle: `51e595e1f220bffe665e1f764cb4cff639986f1c` — `fix(retention): bound core telemetry lifecycle`

## Files

- #43: `backend/src/mqtt/client.ts`, `backend/src/mqtt/durableHandler.ts`, `backend/src/mqtt/durableHandler.test.ts`, MQTT metric/alert cleanup, and `docs/operations.md`.
- #45: lifecycle target gates and tests, removal of packet-policy deletion from `backend/src/db/schema/base.sql`, telemetry capacity metrics and alerts, and updates to `README.md`, `docs/db-lifecycle.md`, and `docs/operations.md`.

## Tests and checks

- From `backend`: `npm run typecheck` — passed.
- From `backend`: `npm test` — passed, 328 tests, 0 failures.
- `python3 -c 'import yaml; list(yaml.safe_load_all(open("logging/rules/meshcore.yml"))); yaml.safe_load(open("logging/rules/meshcore.test.yml")); print("Prometheus rule and test YAML parsed")'` — passed.
- `git diff --check` — passed.
- Prometheus semantic rule tests were not run because `promtool` is not installed on this host.

## Push state

Not pushed. Hermes/coordinator relays the branch and opens the PR.

## Gated items for Ben

1. Before any destructive lifecycle action, confirm a fresh named backup and a successful isolated restore with a valid signed receipt. Check current Timescale policies and chunk state read-only; no policy or live data was changed here.
2. The active lifecycle configuration must include retention targets `packets,node_status_samples,node_neighbor_samples` and compression targets `packets,packet_paths,node_status_samples,node_neighbor_samples` before the corresponding flags are enabled. The checked-in health-worker fallback target list currently omits status and neighbour samples; it was left untouched under the no-config-edits constraint. With retention enabled and required targets missing, the new guard fails closed at startup. Stage the target update and rollout deliberately.
3. Apply or repair retention/compression one table at a time only after the backup/restore gate, using the commands in `docs/db-lifecycle.md`. Validate the Prometheus rules with `promtool test rules logging/rules/meshcore.test.yml` when available.
4. After deployment, verify QoS 1 redelivery using a controlled database-failure drill; no service or broker integration test was run on this host.

## Open questions

The brief describes 180-day packet retention, while the current lifecycle source and migration 050 specify 30 days for packets (180 days for status, seven days for neighbours). The implementation and runbook follow the existing repository policy. Ben should confirm the packet window before production rollout.
