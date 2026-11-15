# Complete lean runtime

The main Compose configuration supplies resource ceilings and connection-pool
defaults for the complete service set. Larger hosts can raise its documented
environment overrides. Feature switches, history, visibility rules, query
deadlines and public response limits retain their existing meanings.

For a larger host, the main Compose file accepts `BACKEND_MEMORY_LIMIT`,
`DATABASE_POOL_MAX`, `WORKER_DATABASE_POOL_MAX`, `HOPREACH_CPUS`,
`HOPREACH_MEMORY_LIMIT`, and the paired `ALLOY_MEMORY_LIMIT` /
`ALLOY_GOMEMLIMIT` settings. For example:

```sh
BACKEND_MEMORY_LIMIT=2g DATABASE_POOL_MAX=12 WORKER_DATABASE_POOL_MAX=3 \
HOPREACH_CPUS=4 HOPREACH_MEMORY_LIMIT=1536m \
ALLOY_MEMORY_LIMIT=256m ALLOY_GOMEMLIMIT=192MiB \
docker compose --env-file /dev/null -f docker-compose.yml config --quiet
```

Use those overrides with the host's normal main-Compose launch configuration.
The local `dev-all.sh` launcher clears caller environment settings and supplies
fixed local qualification wiring; these examples do not change its profile.

Local DEV starts the complete stack with built frontend assets:

```sh
bash scripts/dev-all.sh build
bash scripts/dev-all.sh start
bash scripts/dev-all.sh status
```

Use `sg docker -c 'bash scripts/dev-all.sh start'` when the current shell has
not picked up membership of the Docker group. Commands use explicit local
configuration and do not read a deployment `.env`.

| Service | Local URL |
| --- | --- |
| Public website | http://127.0.0.1:5173 |
| Dashboard | http://127.0.0.1:3003 |
| DEV website | http://127.0.0.1:3005 |
| UKMesh API | http://127.0.0.1:3100 |
| CoreScope | http://127.0.0.1:3102 |
| Beacon | http://127.0.0.1:5175 |
| Beacon API | http://127.0.0.1:3103 |
| Health Check | http://127.0.0.1:3090 |
| Grafana | http://127.0.0.1:3010 |
| Prometheus | http://127.0.0.1:9090 |
| Protected website/dashboard/health ingress | http://127.0.0.1:8923 / 8924 / 8925 |

`hot ukmesh` or `hot beacon-web` enables Vite while editing. `start` restores
built assets. `stop` stops consumers before infrastructure and retains stores.
The migration, data initialization and historical link-backfill jobs complete
and exit; successful exit is their expected healthy state.

Local overrides provide fixture credentials, owned storage and loopback
ingress. Tagging and alert delivery use the local test endpoint, and Health
Check uses the public `#test` channel on the local broker. The public tunnel is
off. Local Anubis derives client identity from the TCP peer; its downstream
proxies trust that overwritten identity rather than incoming Cloudflare headers.

UKMesh runtime processes use separate API, ingest, worker, owner and monitoring
credentials. Only the bounded migration/bootstrap step receives the local
administrator credential. Explicit relation grants preserve existing business
writes and worker operations. Learning has temporary-table access; it cannot
create permanent schema objects. CoreScope's optional PostgreSQL reader uses
privacy-filtered projections and the narrow metadata needed to revoke copies.

Qualification owns backups, fixture selection, workload generation, probes and
reports separately. Original backup receipts require actual restore checks.
Fixture switching validates cluster identity, ownership markers and source
checksums, then changes targets through brief service restarts. Original stores
and retained lookup fixtures remain available for restoration.

```sh
bash scripts/dev-all.sh qualify backup
bash scripts/dev-all.sh qualify inventory
bash scripts/dev-all.sh qualify smoke
```

The executable service inventory and native work oracles must agree with the
resolved configurations. A running process or HTTP 200 alone does not establish
completed work. Missing, skipped, stale or mismatched evidence prevents a clean
qualification. Reports retain failures and resource settings under ignored
`plans/lean-runtime/`; private archive content must not enter Git.

The VM gates remain 100/250 ms interactive p95 at 1×/10×, eight concurrent
requests, at most 3 GiB runtime working memory, at least 384 MiB host available
memory in 95% of steady samples, ten reserved PostgreSQL connections and
10 GiB free disk. Sustained qualification retains 72,000 ACKs at 30/second for
40 minutes, with explicit native expectations for additional typed traffic.
F2 and deployment to the VPS remain deferred.

Target changes use a separate inherited publication lease and durable per-consumer
intent. A mixed partial start can resume only that same known selection. Stopped
consumers with unknown queues must first resume their retained previous stores;
missing observations never mean empty. Before each changing subsystem applies,
a fresh, bounded broker and application intake observation must pass. CoreScope
reports pending plus active work from its own process; Beacon's existing admin
metrics stay on container loopback. Same-store startup can bootstrap those
observations. This protects managed local operations; it is not an atomic fence
against an external MQTT publisher. Original and failed fixture stores remain
retained.
