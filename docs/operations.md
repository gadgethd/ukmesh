# Operations

## Automated checks

Pull requests and branch pushes run `.github/workflows/ci.yml`. The workflow:

- installs dependencies from lockfiles
- type-checks, tests, and builds the backend
- builds the frontend and runs Playwright desktop/mobile smoke tests
- compiles Python workers
- validates the Docker Compose model with required secrets represented by CI-only placeholders

Dependabot groups weekly dependency updates for the backend, frontend, and GitHub Actions.

## Health endpoints

- `GET /healthz` is a process liveness check. It deliberately remains healthy during an MQTT outage so Docker does not restart an otherwise functioning API in a loop.
- `GET /readyz` checks database access and MQTT connectivity. It returns `503` with per-dependency state when the real-time platform is not ready.
- `GET /api/health` returns worker history and a top-level `status` (`healthy`, `degraded`, or `critical`) plus machine-readable `problems`.

Current problem codes cover stale public ingest, worker queue backlog, disk pressure, and frontend error spikes. External monitoring should alert on `readyz` failures and critical `/api/health` problems, while allowing a short deploy/reconnect grace period.

The `synthetic-monitor` service independently checks liveness, dependency readiness, the scoped stats API, and delivery of a WebSocket `initial_state`. Results and latency are retained for 14 days in `operational_check_results`. Three consecutive failures trigger a structured log alert and, when `ALERT_WEBHOOK_URL` is configured, a JSON webhook. A successful check after an alert emits a recovery notification.

## Load checks

Run bounded HTTP load locally with:

```bash
cd backend
npm run load:realtime -- --duration 30 --concurrency 25 --max-p95-ms 1500
```

Use `--mqtt-messages 5000` to exercise the bounded MQTT ingest queue with isolated, rejected test envelopes, and `--slow-ws-clients 10` to hold non-reading WebSocket clients during the run. MQTT mode uses `MQTT_BROKER_URL`, `MQTT_USERNAME`, and `MQTT_PASSWORD`; never point it at a broker outside the deployment under test.

## Coverage API safety

The legacy unbounded `GET /api/coverage` response has been replaced by a bounded viewport API:

```text
GET /api/coverage?bbox=minLon,minLat,maxLon,maxLat&limit=12&cursor=<node-id>
```

- `bbox` is required and may span at most 20 degrees on either axis.
- `limit` defaults to 12 and is capped at 25.
- Each serialized page has a 5 MiB safety budget.
- A single geometry exceeding that budget is represented by `{node_id, truncated: true}` and can be retrieved through the per-node endpoint.
- `page.nextCursor` continues a partial result when `page.hasMore` is true.
- `GET /api/coverage/:nodeId` remains the preferred interactive-map endpoint.

## Topology API

`GET /api/topology?network=ukmesh&limit=300` returns recent viable repeater relationships, bounded to 500 links and a 30-day observation window. It reports connected components, recently active isolated repeaters, and articulation points labelled as likely bridges because the bounded graph is evidence rather than a complete routing model. Private-node names and positions pass through the same redaction policy used by the node APIs.

## Public API and exports

`GET /api/v1` is the stable discovery endpoint and links to `/api/v1/openapi.yaml`. Positioned nodes can be exported from `/api/v1/exports/nodes.csv` and `/api/v1/exports/nodes.geojson`; both formats are read-only, rate-limited, capped at 5,000 rows, network-scoped, and use the public-node redaction policy.

`GET /api/activity/timeline` is bounded to a 24-hour window and 250 active node IDs per bucket. `GET /api/rf-validation` compares stored terrain-model viability with recent observed evidence and distinguishes likely mismatches, weak evidence, and explicit operator overrides.

## Public status and maintenance

The public `/health` page polls the aggregated `/api/health` contract every minute. Set `MAINTENANCE_ACTIVE=1` and `MAINTENANCE_MESSAGE` during a planned window; the API and page show the notice without leaking hostnames, credentials, addresses, or private node identities.
