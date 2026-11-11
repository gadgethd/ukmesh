# PostgreSQL shared-memory audit — HermesMind #454

Date: 2026-10-23. Branch: `burn/ukmesh-shm-20261023`, based on
`origin/main` at `73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.

## Result and scope

The repository contained two runnable Compose PostgreSQL definitions: the CI
TimescaleDB overlay and an embedded maintenance-test fixture. Both omitted
`shm_size`. The GitHub Actions backend database service and two disposable
database launcher scripts also omitted it. These definitions now specify **1 GiB**.
No running container, database, deployment, external worktree, or live config was
inspected or changed. The real `meshcore-infra` database remains **unknown**;
this report establishes repository configuration, not deployed capacity.

The guard inventories every Git-visible YAML file with top-level `services`,
including nested files, profiles and `.yml.example` templates, plus Actions
service containers. It reports **9 Compose files, 73 service entries, 4 PostgreSQL
entries**: three concrete definitions and one overlay-only reference. Entries
are source declarations, not 73 distinct deployed containers. The vacuum
fixture was moved into a YAML file so future edits cannot evade the check inside
a shell heredoc.

## Complete Compose-file coverage

| File | Service entries | PostgreSQL / compute finding |
| --- | ---: | --- |
| `docker-compose.yml` | 29 | No database server; `link-worker` and `hopreach` reviewed below |
| `docker-compose.ci.yml` | 6 | Concrete `timescaledb`; Redis/broker are not PostgreSQL |
| `docker-compose.infra-client.yml` | 13 | `timescaledb` profile overlay only; no image or sizing |
| `docker-compose.live.yml` | 10 | App/network overlays; database is external |
| `compose.phase4.yml` | 9 | App client URLs/commands only; no database definition |
| `scripts/fixtures/vacuum-compose.yml` | 1 | Concrete disposable `timescaledb` |
| `third_party/hopreach/docker-compose.yml` | 1 | Standalone HopReach compute |
| `third_party/hopreach/docker-compose.gpuworker.yml` | 1 | Optional remote GPU compute |
| `third_party/hopreach/docker-compose.override.yml.example` | 1 | GPU device overlay for standalone HopReach |

Actions contributes two more entries (`backend/postgres`, `backend/redis`).
No other YAML database service was found. Shell launchers were also searched for
TimescaleDB/Postgres images and Docker invocations; their findings follow.

## PostgreSQL classification and recommendations

“Safe” below means the checked-in configuration meets the policy for avoiding an
implicit 64 MiB ceiling; it is not a measured upper bound for arbitrary workloads.
“At risk” describes the original definition, even where today's test data is small.
No source definition mounts `/dev/shm` or shares a host/service IPC namespace.

| Service / source | Original explicit size / class | Declared volume mounts | Recommendation / resulting class and reasoning |
| --- | --- | --- | --- |
| `timescaledb`, `docker-compose.ci.yml` | Unset → Docker default 64 MiB; **at risk** | `./backend/src/db/schema/base.sql:/docker-entrypoint-initdb.d/001_schema.sql:ro`; no explicit data mount | **`shm_size: "1g"`**, now **safe** by policy. The 4 GiB memory cap, 300 connections, worker pool and maintenance jobs warrant headroom. `max_parallel_workers_per_gather=0` disables parallel queries, not parallel VACUUM/index maintenance. |
| `timescaledb`, vacuum maintenance fixture (formerly heredoc in `scripts/test-vacuum-maintenance.sh`) | Unset → 64 MiB; **at risk**, low immediate demand with 100 fixture rows | None declared | **`shm_size: "1g"`**, now **safe** by policy. Runs real compression and VACUUM; keep maintenance fixtures representative as data/indexes grow. |
| `backend/postgres`, `.github/workflows/ci.yml` | Unset → 64 MiB; **at risk** | None declared | **`--shm-size=1g`**, now **safe** by policy. Shared fixture for analysis, vacuum, ingestion and privacy integration tests; no guarantee that small current tests remain small. |
| `timescaledb`, `docker-compose.infra-client.yml` | Unset in profile-only overlay; **unknown** | None on the service. Top-level `timescaledb_data` declares an external volume but does not attach it here. | **1 GiB minimum in the owning database definition**. When combined with the checked CI source it inherits 1 GiB; this overlay does not establish the separately managed live database's configuration. |
| Disposable Postgres, `scripts/test-privacy-fence.sh` | Unset → 64 MiB; **at risk** | tmpfs at `/var/lib/postgresql/data:rw`, distinct from `/dev/shm` | **`--shm-size=1g`**, now **safe** by policy. Privacy backfill/index tests can invoke maintenance; a data-directory tmpfs does not enlarge shared memory. |
| Disposable Postgres, `scripts/restore-drill.sh` | Unset → 64 MiB; **at risk** | `$postgres_volume:/var/lib/postgresql/data` | **`--shm-size=1g`**, now **safe** by policy. Restored data and index rebuilds are variable-sized. `autovacuum=off` does not exempt restore/index/query activity. |

The image may declare implicit volumes; the table records only mounts declared
by this repository. The `timescaledb_data` read-only mounts on `backend` and
`health-worker` are disk-health views, not additional Postgres instances.

## Compute review and exemptions

All entries below have **unset `shm_size`**. Ordinary container IPC would therefore
use 64 MiB; no larger application POSIX shared-memory allocation was identified.
The reviewed mount sets do not target `/dev/shm`.

| Service / source | Declared mounts relevant to compute | Class / recommended value and evidence |
| --- | --- | --- |
| `link-worker`, base Compose; inherited by phase4/infra overlays | `srtm_data:/data/srtm`; separate 64 MiB `/tmp` tmpfs | **Safe / exempt for this failure class; retain default**. `NUM_WORKERS=1` enters `worker_loop()` directly. GDAL/NumPy arrays are process memory; reviewed code has no multiprocessing shared arrays/queues or POSIX shm payloads. Multiworker fallback creates processes, not a shared raster segment. Evidence: `viewshed-worker/worker.py` `main()`, `worker_loop()` and `ReadAsArray` use. |
| `hopreach`, base Compose | `rf_coverage_data:/data`, read-only `rf-coverage/config.ukmesh.yaml` and `rf-coverage/boundaries/uk-operational-v1.geojson` binds | **Safe / exempt for the configured CPU path; retain default**. DEM grids use disk-backed scratch files under the cache via `mmapFloat32` in `third_party/hopreach/internal/demgrid/load.go`; this is file-backed `MAP_SHARED`, not `/dev/shm`. The 8 GiB RAM cap is a different resource. No GPU device is passed through here. |
| Standalone `hopreach`, vendored Compose | `hopreach-data:/usr/share/nginx/html/data`, `hopreach-analytics:/usr/share/nginx/html/analytics`, `hopreach-dem-cache:/data/dem-cache`, `hopreach-shared-plans:/data/shared-plans` | **Safe / exempt on CPU; retain default** for the same file-backed/heap design. Preserve disk-backed DEM cache storage. |
| `gpuworker`, vendored GPU Compose | `hopreach-gpuworker-dem-cache:/data/dem-cache`; device `/dev/dri:/dev/dri` | **Unknown** for driver-internal allocations. Application code uses WebGPU/Vulkan buffers (`internal/gpucompute/gpucompute.go`), not explicit POSIX shm, but the real driver was not profiled. Recommend **`shm_size: "1g"` when enabling GPU**, then validate representative workloads; no GPU-source change made on unmeasured evidence. |
| `hopreach`, vendored GPU override example | No added volumes; inherits standalone mounts; adds `/dev/dri:/dev/dri` | **Unknown** when GPU enabled; same **1 GiB recommendation** and driver qualification as the remote worker. The example itself starts no additional service. |

Remaining base services are **exempt from the PostgreSQL guard**, with no evidence
of the targeted maintenance/shm workload: Node app/client jobs (`db-migrate`,
`backend`, `path-learning-worker`, `health-worker`, `tagger-worker`,
`privacy-remat-worker`, `synthetic-monitor`, `link-backfill-worker`,
`mesh-health-check-data-init`, `mesh-health-check`, `alert-receiver`); nginx
(`app-ukmesh`, `website-ukmesh`, `website-dev`); `anubis-website-ukmesh`,
`anubis-app-ukmesh`, `anubis-health-check`, `cloudflared`; monitoring (`loki`,
`alloy`, `prometheus`, `grafana`, `alertmanager`, `postgres-exporter`,
`redis-exporter`, `node-exporter`, `blackbox-exporter`). CI also defines
`mosquitto`, `mosquitto-reloader` and `redis`. Keep their existing sizing; database
client URLs, Postgres exporter names, large heaps, and `/tmp` tmpfs limits alone
do not make a service a PostgreSQL `/dev/shm` consumer. No browser service exists
in these Compose files; Playwright runs on CI hosts.

`scripts/test-infra-compose.sh` contains another YAML-shaped TimescaleDB stub.
It is **safe / exempt**, with unset size and no mounts: the test replaces Docker
with a mock executable that only records arguments. It cannot start Postgres,
so no shm allocation exists and no sizing change is needed.

## Policy and guard

Docker documents a [64m default](https://docs.docker.com/engine/containers/run/#runtime-constraints-on-resources)
and the [Compose `shm_size` setting](https://docs.docker.com/reference/compose-file/services/#shm_size).
PostgreSQL distinguishes its main memory region from dynamic POSIX shared memory;
parallel maintenance has a separate worker setting from parallel queries.
See [PostgreSQL resource consumption](https://www.postgresql.org/docs/17/runtime-config-resource.html)
and [VACUUM's parallel option](https://www.postgresql.org/docs/17/sql-vacuum.html).

**1 GiB is a repository policy floor**, sixteen times the implicit ceiling, with
room beyond a typical 64 MiB maintenance budget for concurrent dynamic segments.
It is not calculated from `shared_buffers`, which normally uses a different
memory region, nor a universal sizing formula. The tmpfs ceiling does not
preallocate 1 GiB; actual usage still consumes container/host memory (see
[Docker tmpfs accounting](https://docs.docker.com/engine/storage/tmpfs/)). Higher
maintenance budgets or concurrency need measurement and potentially more shm
and RAM. Do not use `ipc: host` or a host `/dev/shm` bind to bypass the policy.

Run `python3 -m pip install -r scripts/requirements-checks.txt`, then
`python3 scripts/check-postgres-shm.py` (or add `--json`). CI runs the regression
tests and inventory before the worker/Compose tests. The check:

- Discovers YAML through Git, including new nonignored files; no hardcoded file list.
- Recognizes common PostgreSQL/Timescale/PostGIS/pgvector images and image defaults,
  server commands, server initialization variables and `x-postgres: true` for
  opaque custom Compose images. Database URLs/exporters alone are not servers.
- Requires literal integer byte counts or Docker b/k/m/g sizes of at least
  1,073,741,824 bytes; unset, null, tiny, reset and interpolated sizes fail.
- Reads YAML anchors and Compose tags; detects sizing reductions in same-directory
  service overlays. An overlay can omit sizing when inheriting its chosen base;
  its standalone runtime capacity is reported unknown.
- Rejects IPC sharing and `/dev/shm` mounts that mask the size. Unsupported
  `include`/`extends`, invalid YAML and an empty Postgres inventory fail closed.
- Reports source, service, size, mounts and classification. It never invokes
  Docker, expands environment variables, reads `.env`, or contacts a database.

Limits: it does not evaluate arbitrary shell programs, Docker image internals,
ignored host overrides or external repositories. The two direct Docker launcher
flags were audited and fixed separately; they are outside the YAML guard.
Custom opaque database images must retain the explicit marker/server metadata.
Non-Postgres “exempt” output means outside the guard's enforcement; GPU capacity
remains unknown as classified above.

## Verification

Executed locally, without starting, stopping, restarting or modifying containers:

- `python3 -m unittest discover -s scripts -p 'test_postgres_shm.py' -v` — **PASS**, 10 tests, including CLI failures for missing/64 MiB sizing, overlay reductions, optional profiles, custom servers, image interpolation defaults and masked shm.
- `python3 scripts/check-postgres-shm.py` — **PASS**, inventory 9 / 73 / 4 as above; `--json` also passed.
- `npm --prefix scripts ci --cache "$PWD/.ukmesh-tools/npm-cache" --no-audit --no-fund` — **PASS**, 22 packages installed locally.
- `npm --prefix scripts run typecheck` — **PASS**.
- `TMPDIR="$PWD/.ukmesh-tools/shm-audit/tmp" npm --prefix scripts test` — **PASS**, observer-key, infrastructure-resolver, newuser and website-deploy regression suites. Docker calls in those suites are mocked; all temporary files stayed in this worktree.
- `docker compose --env-file /dev/null -f scripts/fixtures/vacuum-compose.yml config --quiet` — **PASS**; the maintenance script copies this exact fixture.
- `bash -n scripts/restore-drill.sh scripts/test-privacy-fence.sh scripts/test-vacuum-maintenance.sh` — **PASS**.
- `python3 -m py_compile scripts/check-postgres-shm.py scripts/test_postgres_shm.py` — **PASS**.
- `git diff --check` — **PASS**.

Main CI Compose validation also passed using an empty environment, explicit dummy
values and no `.env` access. Exact command:

```bash
env -i PATH="$PATH" POSTGRES_PASSWORD=shm-check REDIS_PASSWORD=shm-check \
  JWT_SECRET=shm-check OPERATOR_SITE_TOKEN=shm-check MQTT_PASSWORD=shm-check \
  ANUBIS_ED25519_PRIVATE_KEY_HEX=0000000000000000000000000000000000000000000000000000000000000001 \
  HEALTHCHECK_SOURCE_REF=952eec8e0c55d0749f9abef211c266f992c0bfc7 \
  GRAFANA_ADMIN_PASSWORD=shm-check \
  docker compose --env-file /dev/null -f docker-compose.yml -f docker-compose.ci.yml config --quiet
```

Exit 0; expected warnings for unset `POSTGRES_USER` and `CLOUDFLARE_TUNNEL_TOKEN`.
Database integration/restore fixtures were not launched. Runtime vacuum behavior
and GPU driver allocations were not measured in this repository-only audit.

## Follow-up outside this worktree

Audit the owning `meshcore-infra` Compose definition and other independently
managed Postgres stacks separately; do not infer their shm sizing from this app
repository. Recommend an explicit minimum 1 GiB there, adjusted to measured
maintenance concurrency and the host/container memory budget. Applying any live
change belongs to a separately authorized maintenance window. No deploy, PR,
push or external publication was performed by this task.
