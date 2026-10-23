# MeshCore Analytics

A real-time analytics platform for [MeshCore](https://meshcore.io) networks. It ingests MQTT packets from `mctomqtt`-compatible observers, decodes them with `@michaelhart/meshcore-decoder`, stores them in TimescaleDB, and serves interactive dashboards plus public-facing site pages with live mapping, link intelligence, coverage modelling, packet analytics, and worker/system health.

---

## Features

- Real-time node map with animated packet arcs and live WebSocket updates
- Progressive UK-wide HopReach RF coverage with Standard and Precision tiers
- Link intelligence overlay with directional observations and path-loss viability
- Public repeater-topology explorer with hub ranking, graph components, likely bridge repeaters, isolated nodes, and multibyte-evidence filtering
- Map modes, shareable viewport/filter URLs, selected-node popup, and bounded activity replay
- Regional health scoring and predicted-versus-observed RF validation
- Local saved searches/watchlists for nodes, observers, regions, packet types, and incidents
- Privacy-filtered CSV/GeoJSON exports with a versioned OpenAPI contract
- Shared Viterbi path resolver with multibyte evidence, a concurrent worker pool, hourly prior rebuilds and [repeatable accuracy checks](docs/pathing.md)
- Multibyte path-hash support (1-byte, 2-byte, 3-byte) throughout the live ingest and pathing stack
- Decoded live packet feed (Advert, GroupText, DM, ACK, Path, Trace)
- Stats pages and chart endpoints for packet rates, radios, hops, and activity
- Public Health page with worker status/history + server resource metrics
- UK site Feed page for public MQTT observer traffic visibility
- Repeater owner portal with MQTT username/password login and encrypted cookie session
- Owner dashboard with repeater summary, direct sender map, live packets, advert trend, heard-by list, link health, and alerts
- Opt-in owner public MQTT feed with CoreScope-compatible MeshCore packet output
- Multi-network ingestion (`meshcore/*` and `ukmesh/*`) with per-site filtering
- Isolated test-feed support via `meshcore-test/*` and `test.ukmesh.com`
- Multi-observer deduplication by packet hash
- MQTT connection monitor with Mosquitto log parsing and reconnect tracking

---

## Roadmap

### Phase 1 - Core platform (complete)
- MQTT ingestion via `mctomqtt` with multi-observer support
- Packet decoding with `@michaelhart/meshcore-decoder`
- TimescaleDB storage and live WebSocket fan-out
- React dashboard: node map, animated packet arcs, decoded live feed
- Packet deduplication by hash across observers

### Phase 2 - RF coverage and link intelligence (complete)
- HopReach v0.1.32 canonical terrain propagation with reference-parity tests
- Progressive Standard/Precision rasters rendered natively in MapLibre
- Link worker: observed relay-path processing into node-to-node link intelligence
- Directional link counts and path-loss viability modelling
- Versioned GB, Northern Ireland, Isle of Man, Jersey, and Guernsey boundary

### Phase 3 - Path learning and predictions (beta)
- Hourly path-learning prior rebuild worker
- Beta path overlays and confidence scoring
- Historical calibration using observed packet behavior
- Multibyte path-hash aware path resolution
- Concurrent resolve worker pool for high-throughput path matching

### Phase 4 - Public website and operations (complete)
- Separate public-facing website pages (install, MQTT, packets, stats)
- Public Health page with worker/system status and history
- Click-to-explain worker cards
- UK Feed page for live public observer traffic

### Phase 5 - Repeater owner portal (complete)
- MQTT username/password owner login with encrypted cookie session
- Dedicated owner auth database for username → repeater ownership mapping
- Owner-facing dashboard: repeater summary, packet history, advert counts, direct sender map, heard-by list, link health, and alerts
- Planned repeater registration/claim workflow improvements
- Owner alerts for offline duration, low battery voltage, and excessive predicted path loss, with durable delivery history and test delivery

### Phase 6 - Network intelligence expansion (complete)
- Bounded topology graph with hubs, graph components, isolated repeaters, and likely bridge nodes
- Regional health scoring based on traffic freshness and observer redundancy
- Timeline replay, map modes, shareable views, node details, and planning comparisons
- Path explanations with confidence, evidence, alternatives, and limitations
- Saved searches/watchlists plus versioned CSV and GeoJSON exports

### Phase 7 - Predicted vs observed RF model validation (complete)
- Compare terrain-predicted links against real observed relay behavior
- Highlight high-confidence mismatches for network tuning
- Separate operator overrides and weak evidence from likely model mismatches

### Phase 8 - Reliability and operations (complete)
- CI for backend, frontend, browser journeys, Python workers, and Compose configuration
- Independent synthetic HTTP/WebSocket monitoring with failure and recovery webhooks
- Liveness/readiness split, public status page, DB maintenance telemetry, and bounded load tooling
- Restartable, audited production-network label migration with snapshot-based rollback guidance

---

## Current State

- Split worker architecture for resilience:
  - `hopreach` (canonical progressive coverage compute)
  - `link-worker` (link/path-loss processing)
  - `path-learning-worker` (hourly model rebuild)
  - `health-worker` (health snapshots)
  - `link-backfill-worker` (one-shot historical backfill)
- Path resolver runs a concurrent worker pool (`resolveWorker`, `resolvePool`, `resolveCache`) to handle high packet volumes without blocking the main ingest loop.
- Nginx frontend proxies use Docker DNS resolver-based upstreams to avoid stale backend IP issues after container recreates.
- Owner authentication uses MQTT credentials plus a separate owner-auth mapping database rather than public-key login.
- Live coverage is served by HopReach's canonical terrain model; calibrated variants remain disabled pending UK evidence validation.
- Public/test feeds are isolated at the topic level, with `meshcore-test/*` excluded from the public sites.
- MQTT connection state is tracked via Mosquitto log parsing — connect/disconnect events are available in the health feed.

---

## Quick Start

```bash
# 1. Clone and enter the project
git clone https://github.com/gadgethd/ukmesh.git
cd ukmesh

# 2. Copy and configure environment
cp .env.example .env
# Edit .env. Required values are POSTGRES_PASSWORD, JWT_SECRET,
# MQTT_PASSWORD, REDIS_PASSWORD, OPERATOR_SITE_TOKEN,
# ANUBIS_ED25519_PRIVATE_KEY_HEX, GRAFANA_ADMIN_PASSWORD, and the full
# 40-character HEALTHCHECK_SOURCE_REF.

# 3. Create Mosquitto's backend credential and least-privilege ACL
set -a
. ./.env
set +a
scripts/bootstrap-mosquitto.sh

# 4. Start everything
docker compose up -d

# 5. Confirm readiness and inspect logs
curl --fail http://127.0.0.1:3000/readyz
docker compose logs -f backend
```

The calculator starts with the stack, publishes Standard tiles first, and
continues Precision only after Standard is live and the disk gate passes:

```bash
docker compose up -d backend hopreach app-ukmesh link-worker
docker compose logs -f hopreach
```

Local endpoints:

- Backend API/WS: `http://localhost:3000`
- App (ukmesh): `http://localhost:3003`
- Website (ukmesh): `http://localhost:3004`
- Dev/test site: `http://localhost:3006`
- Liveness/readiness: `http://localhost:3000/healthz` and `http://localhost:3000/readyz`
- API discovery/OpenAPI: `http://localhost:3000/api/v1` and `http://localhost:3000/api/v1/openapi.yaml`

To expose it publicly, configure a Cloudflare Tunnel (see below) or reverse proxy of your choice.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values. All variables used by the app:

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `meshcore` | TimescaleDB database name |
| `POSTGRES_USER` | `meshcore` | TimescaleDB user |
| `POSTGRES_PASSWORD` | *(required)* | TimescaleDB password |
| `POSTGRES_MAX_WORKER_PROCESSES` | `24` | PostgreSQL worker slots; keep above the Timescale background-worker setting plus launcher/scheduler headroom |
| `MQTT_BROKER_URL` | `ws://mosquitto:9001` | Mosquitto WebSocket URL (internal) |
| `MQTT_USERNAME` | `backend` | MQTT client username |
| `MQTT_PASSWORD` | *(required)* | MQTT client password |
| `MESHCORE_SITE_ID` | `ukmesh` | Stable identifier added to site-to-site packet relay metadata |
| `REDIS_PASSWORD` | *(required)* | Password for the bundled Redis service; passed separately from the URL so reserved characters are safe |
| `REDIS_URL` | `redis://redis:6379` | Redis URL for WebSocket pub/sub |
| `JWT_SECRET` | *(required)* | Secret for JWT verification |
| `ALLOWED_ORIGINS` | `http://localhost:3001,http://localhost:3002` | Comma-separated browser origins allowed for CORS and WebSocket |
| `API_RATE_LIMIT_MAX` | `120` | Per-client public API requests/minute; raise only in an isolated load-test project |
| `VITE_APP_HOSTNAME` | *(blank — always shows dashboard)* | If set, only this hostname serves the analytics dashboard; all others serve the public website layout |
| `MESHCORE_CHANNEL_SECRETS` | *(blank)* | Confidential channel keys for decrypting GroupText packets, managed through environment configuration. Format: `name:hex` or bare hex. The explicitly public `PUBLIC_CHANNELS` defaults are always included. |
| `OWNER_DATABASE_URL` | *(optional)* | Separate Postgres database URL for owner portal username → repeater mappings |
| `OWNER_COOKIE_SECRET` | *(optional but recommended)* | Secret used to encrypt/sign the owner session cookie |
| `OWNER_MQTT_USERNAME_MAP` | *(empty)* | Operator-managed owner grants in the format `user=nodeId1|nodeId2,...` |
| `OWNER_PACKET_SHARE_ENCRYPTION_KEY` | *(empty)* | Key used to encrypt destination MQTT passwords; required when a destination uses authentication |
| `OWNER_PACKET_SHARE_POLL_INTERVAL_MS` | `5000` | Packet-sharing delivery queue poll interval |
| `OWNER_PACKET_SHARE_BATCH_SIZE` | `25` | Maximum packet-sharing deliveries claimed per poll |
| `PUBLIC_FEED_PUBLISH_ENABLED` | `false` | Operator kill switch for the authentication-locked UKmesh MQTT feed; owner switches remain off by default |
| `PUBLIC_FEED_BROKER_URL` | *(empty)* | Deployment-only MQTT/MQTTS/WS/WSS endpoint shown in the owner dashboard; packets remain on the UKmesh broker and no separate publisher credential is used |
| `OWNER_AUTHORIZATION_MODE` | `shadow` | `shadow` preserves read-only legacy ACL compatibility; `enforce` accepts verified database/config grants only |
| `OWNER_ACL_MODE` | `shadow` | `shadow` renders and validates without changing Mosquitto; `apply` atomically installs and verifies the canonical ACL |
| `OWNER_ACL_UNMANAGED_USERS` | `backend,test,test2` | Exact broker accounts intentionally preserved outside owner grant management |
| `OWNER_ACL_ALLOW_EMPTY_USERS` | *(empty)* | Explicitly reviewed owner accounts allowed to render with no publish grants |
| `OWNER_ACL_PUBLIC_FEED_READ_USERS` | *(empty)* | Operator-approved read-only MQTT subscribers; they receive the curated public feed only |
| `OWNER_ACL_SYSTEM_READWRITE_TOPICS` | `public/#` | Broker topics the internal backend account may publish while rendering the public feed |
| `RF_COVERAGE_ENABLED` | `true` | Compile the native HopReach layer into the production app image |
| `HOPREACH_IMAGE` | `meshcore-analytics-hopreach:local` | Immutable HopReach calculator image override |
| `HOPREACH_CPUS` | `4.0` | Calculator CPU limit |
| `HOPREACH_CPU_WORKERS` | `4` | Go CPU worker limit (`GOMAXPROCS`) |
| `HOPREACH_MEMORY_LIMIT` | `8g` | Calculator memory and swap limit |
| `PUBLIC_FEATURE_INFERRED_NODES_ENABLED` | `true` | Runtime kill switch for privacy-reviewed inferred map nodes |
| `PUBLIC_FEATURE_PACKET_ARCS_ENABLED` | `true` | Runtime kill switch for privacy-filtered live packet arcs |
| `PUBLIC_FEATURE_HEATMAP_ENABLED` | `false` | Runtime kill switch for the packet heatmap |
| `PUBLIC_FEATURE_CONFIG_TTL_SECONDS` | `30` | Client refresh interval for same-origin public feature configuration (bounded to 5–300 seconds) |
| `CLOUDFLARE_TUNNEL_TOKEN` | *(optional)* | Cloudflare Zero Trust tunnel token |
| `PORT` | `3000` | Internal app port |

The inferred-node and packet-arc layers are enabled after privacy/correctness
validation; the heatmap remains disabled by default. Each has an independent
runtime kill switch. To operate an immediate kill switch,
change the relevant value in `.env`, then recreate the backend without building
an image:

```bash
docker compose up -d --no-build --force-recreate backend
```

Browsers load `/api/runtime-config` before the map starts and refresh it within
`PUBLIC_FEATURE_CONFIG_TTL_SECONDS`. A failed, malformed, or timed-out request
disables all three layers.

---

## Owner Packet Sharing

Packet sharing is available to every authenticated UKMesh owner. The owner
dashboard can enable sharing for an owned observer and select only destination
rows that an operator has already provisioned in
`owner_packet_share_destinations`. Toggle and destination changes save
immediately. Each destination can also carry an HTTPS `website_url` and short
`description`; the owner UI renders those as a linked destination name with
hover text, so adding another site only requires another operator-managed
destination row.

Availability does not enable forwarding. New owner/node selections start with
every destination off, and a destination is forwarded only after that owner
explicitly enables it.

Destinations may set `allowed_owner_username` and `allowed_node_id` to restrict
visibility and forwarding to an exact account and observer. These restrictions
apply to API reads, saves, queue insertion and delivery. NULL scopes preserve
destinations available to all owners.

ScotMesh is restricted to **node1**, uses IATA **MME**, and connects over TLS
WebSockets to `scotmesh-mqtt.mm7roq.compute.oarc.uk:443` with the same JWT
audience. Provisioning creates no enabled sharing rule. Its
`tx_mode = 'advert'` matches `set mqtt.tx advert`: received packets are included,
while transmitted packets must be the observer's own adverts.

Prepare an encrypted relay identity for a verified node1 observer with the
deployment's `DATABASE_URL`, owner database settings and
`OWNER_PACKET_SHARE_ENCRYPTION_KEY` in the environment:

```bash
npm --prefix backend run owner-packet-share:prepare-scotmesh -- \
  ACTIVE_OBSERVER_PUBLIC_KEY .private-config/scotmesh-node1.json
```

The command creates a new MeshCore relay signing key, stores only its encrypted
form in a file readable by its owner, and refuses to overwrite an existing
file. It does not change the radio's key or publish any packets. Apply migration
058 and deploy the owner-scoped backend **before** provisioning its destination
row; the previous backend cannot hide restricted destinations. Then run:

```bash
npm --prefix backend run owner-packet-share:provision-scotmesh -- \
  .private-config/scotmesh-node1.json
```

The provisioning command validates the encrypted key, checks the verified
node1 grant again, and imports the complete configuration without enabling
forwarding. The schema
migration deliberately does not insert the restricted destination while the
previous backend may still be running. For `auth_mode = 'meshcore_jwt'`,
`broker_password_ciphertext` contains the encrypted 64-byte MeshCore signing
key, rather than a static broker password. The relay signs a fresh one-hour
token on each connection and renews it before expiry. ScotMesh's MQTT topic
and `origin_id` use the generated relay public key, while the original raw
radio packet and observer name are preserved. Turn the ScotMesh switch on in
node1's dashboard after provisioning.

The relay uses outbound MQTT. `mqtts://` is the expected production transport;
`wss://`, `mqtt://`, and `ws://` are accepted for controlled testing. The
dashboard browser WebSocket remains a separate live-display transport.

Destination rows are deliberately not writable through the owner API. Broker
passwords are stored as AES-GCM ciphertext. To prepare a password for an
operator SQL insert after `OWNER_PACKET_SHARE_ENCRYPTION_KEY` has been set:

```bash
printf '%s' 'destination-password' \
  | OWNER_PACKET_SHARE_ENCRYPTION_KEY='deployment-secret' \
    npm --prefix backend run owner-packet-share:encrypt-secret
```

Store the resulting ciphertext in `broker_password_ciphertext`, then insert an
enabled destination with its fixed broker URL and topic template. Supported
template values are `{network}`, `{iata}`, `{observerId}`, and `{packetHash}`.
The owner toggle remains unavailable while no complete destination exists.

For a broker that permits an anonymous publisher, the operator provisioning
shape is:

```sql
INSERT INTO owner_packet_share_destinations (
  destination_id, display_name, website_url, description,
  broker_url, topic_template, enabled
) VALUES (
  'remote-site', 'Remote site', 'https://destination.example',
  'Short destination description', 'mqtts://broker.example:8883',
  'replicated/{network}/{observerId}/packets', TRUE
);
```

## Owner Public Feed

The owner dashboard also has a separate account-level `Public feed` switch.
It is off by default and applies to every node currently owned by that account.
New or revoked owner-node grants are reconciled from the verified owner-auth
database automatically. Disabling the switch removes its read ACL immediately.
An account can enable the
switch only after at least one eligible public packet has been observed from
one of its nodes in the previous 24 hours.

The feed is served by the authentication-locked UKmesh MQTT broker at the
deployment-configured endpoint. The backend republishes only eligible packets
from opted-in active owners into the curated CoreScope-compatible namespace
`public/<IATA_REGION>/<OBSERVER_ID>/packets` with the normal MeshCore JSON
payload. The canonical ingest namespace remains private to its owners and the
backend.

An owner who has opted in and remains active receives one read-only ACL filter
for `public/+/+/packets`, so the owner can use the existing MQTT username and
password used to publish observer packets. A named subscriber configured in
`OWNER_ACL_PUBLIC_FEED_READ_USERS` receives the same read-only filter without
publish permission. The read grant is removed when the owner disables the
switch or has no eligible packet activity for 24 hours. The dashboard lists
the curated filter and explains that packets are observable over the internet
and that the site operator cannot verify what external subscribers do with the
data.
The public endpoint is deployment configuration and is not stored with
credentials in source control.

For an ACL-based broker, the intended public-feed permission shape is:

```text
user <opted-in-active-owner>
topic read public/+/+/packets
```

Only the internal backend account receives `topic readwrite public/#` so it can
republish approved packets. The generated owner ACL applies directly to the
Mosquitto broker managed by this deployment.

External subscriber accounts remain operator-managed and read-only. The
backend never receives their passwords and cannot inspect what a subscriber
does after receiving a packet.

Authenticated destinations also set `broker_username` and the encrypted
`broker_password_ciphertext` value from the command above.

---

## Mosquitto Setup

Mosquitto is configured for WebSocket-only access with password authentication.
Create the backend credential and least-privilege read ACL before first startup:

```bash
# Reads MQTT_USERNAME/MQTT_PASSWORD from the environment.
set -a
. ./.env
set +a
scripts/bootstrap-mosquitto.sh
docker compose up -d
```

The bootstrap is idempotent, sets credential files to mode `0640`, refuses
symlinks or incomplete existing state, and never overwrites live credentials.

Do not create observer passwords manually. Provision every observer with
`~/bin/newuser`. Supply one or more full 64-character node public keys when
known; if omitted, the helper prints the credential and watches fresh broker
logs for a bounded discovery window. A credential-only account may authenticate
before it has a publish grant. The helper verifies the owner rows and exact
per-key publish ACLs before reporting provisioning complete. `--watch` resumes
first-device discovery and `--link` adds a device for an existing owner.

For built-in MQTT firmware, set IATA and use the complete Topic root
`meshcore/{IATA}/{PUBLIC_KEY}/packets`. The default `meshcore` is denied even
when login succeeds. Keep both tokens in the firmware template; the actual full
key is needed for the server-side grant. See [MQTT onboarding and invisible-device
troubleshooting](docs/mqtt-onboarding.md) for the exact flasher/serial fix and
[generic owner linking](docs/designs/generic-owner-linking.md) for the remaining
automation gap.

---

## Cloudflare Tunnel (optional)

To expose the app and MQTT broker publicly without opening firewall ports:

1. Go to [Cloudflare Zero Trust](https://one.dash.cloudflare.com/) → Networks → Tunnels
2. Create a tunnel and copy the token
3. Add to `.env`: `CLOUDFLARE_TUNNEL_TOKEN=<token>`
4. Start with the tunnel profile: `docker compose --profile tunnel up -d`
5. Configure public hostnames in the Cloudflare dashboard (example):
   - `app.example.com` → `http://anubis-app-ukmesh:8923`
   - `www.example.com` → `http://anubis-website-ukmesh:8923`
   - `mqtt.example.com` → `http://mosquitto:9001`
   - `healthcheck.example.com` → `http://anubis-mesh-health-check:8923`

For UKMesh health checks, point `healthcheck.ukmesh.com` at
`http://anubis-mesh-health-check:8923` in the same tunnel. The container uses the
internal Mosquitto WebSocket listener and persists observer/result state in the
`mesh_health_check_data` Docker volume. A network-isolated one-shot Compose
initializer normalizes that volume's ownership before the capability-free,
non-root application starts. By default it uses
`HEALTHCHECK_TEST_CHANNEL_NAME=ukmeshtest` and reuses the existing `test:...`
entry from `MESHCORE_CHANNEL_SECRETS` via
`HEALTHCHECK_TEST_CHANNEL_SECRET_SOURCE_NAME=test`.

---

## MQTT Topic Structure

The backend subscribes to `meshcore/#`, `ukmesh/#`, and `meshcore-test/#`. MeshCore observers publish `mctomqtt`-compatible JSON envelopes to topics of the form:

```
meshcore/<IATA>/<observer-public-key>/packets   # received/transmitted packets
meshcore/<IATA>/<observer-public-key>/status    # node status advertisement
ukmesh/<IATA>/<observer-public-key>/packets
ukmesh/<IATA>/<observer-public-key>/status
meshcore-test/<IATA>/<observer-public-key>/packets
meshcore-test/<IATA>/<observer-public-key>/status
```

Payloads are JSON envelopes containing a `raw` hex field (the MeshCore packet) plus metadata such as RSSI, SNR, direction, and hash. The ingest path supports 1-byte, 2-byte, and 3-byte path hashes carried inside the raw packet.

---

## Architecture

```
MeshCore Devices
     │ LoRa RF
     ▼
 mctomqtt-compatible observer
     │ MQTT over WebSocket/TLS
     ▼
 Mosquitto ─────────────────────────────── (optional Cloudflare Tunnel)
     │ subscribe meshcore/# + ukmesh/#
     ▼
 Backend (Node.js/TypeScript)
     │
     ├─ meshcore-decoder → TimescaleDB (packets, nodes, observed links, priors, health snapshots)
     │
     ├─ Path resolver worker pool (concurrent resolve workers + LRU cache)
     │
     ├─ Redis pub/sub
     │
     ├─ WebSocket → frontend live updates
     └─ REST API /api/*

 App/Web Frontends (Nginx + React)
     └─ app-ukmesh / website-ukmesh / website-dev (interactive dashboard + public site + owner portal)

 RF and Link Workers
     ├─ HopReach (persistent DEM cache, resumable Standard/Precision rasters)
     ├─ backend private compatibility API (positioned repeaters + observed evidence)
     └─ link-worker → node_links from observed paths

 Backend Workers (Node.js)
     ├─ path-learning-worker (hourly prior rebuild)
     ├─ health-worker (minute snapshots)
     └─ link-backfill-worker (one-shot historical backfill)

 Owner Auth
     └─ separate Postgres DB for MQTT username → repeater ownership mapping
```

---

## Services

| Service | Image | Purpose |
|---|---|---|
| `timescaledb` | Digest-pinned TimescaleDB/PostgreSQL image from Compose | Time-series and relational data storage |
| `mosquitto` | Digest-pinned Eclipse Mosquitto image from Compose | MQTT broker (WebSocket only) |
| `mosquitto-reloader` | Locally built, least-privilege helper | Authenticated broker-local ACL reload |
| `redis` | Digest-pinned Redis image from Compose | WebSocket fan-out pub/sub and bounded job queues |
| `backend` | Built from `Dockerfile.backend` | MQTT ingest, decoding, API, WebSocket |
| `path-learning-worker` | Built from `Dockerfile.backend` | Hourly path-learning model rebuilds |
| `health-worker` | Built from `Dockerfile.backend` | Periodic health snapshot capture |
| `link-backfill-worker` | Built from `Dockerfile.backend` | One-shot historical link backfill |
| `synthetic-monitor` | Built from `Dockerfile.backend` | Independent HTTP/WebSocket journey checks and alert delivery |
| `link-worker` | Built from `viewshed-worker/Dockerfile` | Link/path-loss processing from observed paths |
| `hopreach` | Built from `third_party/hopreach/Dockerfile` | Canonical terrain RF calculation and progressive raster publication |
| `app-ukmesh` | Built from `Dockerfile.app` | Interactive dashboard frontend |
| `website-ukmesh` | Built from `Dockerfile.website` | Public website frontend |
| `mesh-health-check` | Built from the configured `gadgethd/meshcore-health-check` ref | MeshCore observer coverage health-check app |
| `website-dev` | Built from `Dockerfile.website` | Isolated test/status site for `meshcore-test/*` traffic |
| `cloudflared` | `cloudflare/cloudflared` | Optional Cloudflare Tunnel (use `--profile tunnel`) |
| `alloy`, `loki` | Digest-pinned observability images | Bounded journal/container log collection and storage |
| `prometheus`, `alertmanager`, `alert-receiver` | Digest-pinned observability images plus local receiver | Metrics, rules, grouped alert delivery, and delivery history |
| `grafana` | Digest-pinned Grafana image | Provisioned read-only operational dashboards |
| `postgres-exporter`, `redis-exporter`, `node-exporter`, `blackbox-exporter` | Digest-pinned exporters | Database, queue, host, and endpoint telemetry |

---

## Data Retention

- The reviewed core telemetry windows are 30 days for raw packets, 180 days
  for status samples, and seven days for neighbour samples. Content-stripped
  `packet_paths` remain available for path reconstruction and are never
  retention-deleted. Privacy-safe aggregates and current node/link/model state
  remain longer lived.
- Migration 050 declares the core Timescale retention policies. Repairing a
  missing policy and enabling compression require a fresh signed
  backup/restore receipt and per-table approval. When lifecycle actions are
  enabled, startup fails closed if required core targets are missing from the
  configured allowlists.
- Expired forensic data is available only by restoring a suitable pre-expiry
  backup into an isolated, read-only environment; do not restore it over
  production.
- Operational row-table cleanup is bounded and runs only when both
  `DATA_LIFECYCLE_RETENTION_ENABLED=true` and the exact table appears in
  `DATA_LIFECYCLE_RETENTION_TARGETS`.
- See `docs/db-lifecycle.md` for the exact dry-run inventory, compression,
  retention, owner export, and restore requirements.

---

## Acknowledgements

This project is built on the following open source libraries and tools:

### Frontend
| Package | License |
|---|---|
| [React](https://react.dev) | MIT |
| [Vite](https://vitejs.dev) | MIT |
| [TypeScript](https://www.typescriptlang.org) | Apache 2.0 |
| [MapLibre GL JS](https://maplibre.org/maplibre-gl-js/docs/) | BSD 3-Clause |
| [deck.gl](https://deck.gl) | MIT |
| [react-router-dom](https://reactrouter.com) | MIT |
| [Recharts](https://recharts.org) | MIT |
| [polygon-clipping](https://github.com/mfogel/polygon-clipping) | MIT |

### Backend
| Package | License |
|---|---|
| [Express](https://expressjs.com) | MIT |
| [MQTT.js](https://github.com/mqttjs/MQTT.js) | MIT |
| [ws](https://github.com/websockets/ws) | MIT |
| [ioredis](https://github.com/redis/ioredis) | MIT |
| [node-postgres](https://node-postgres.com) | MIT |
| [cors](https://github.com/expressjs/cors) | MIT |
| [express-rate-limit](https://github.com/express-rate-limit/express-rate-limit) | MIT |
| [@michaelhart/meshcore-decoder](https://www.npmjs.com/package/@michaelhart/meshcore-decoder) | MIT |

### RF and link workers
| Package | License |
|---|---|
| [HopReach v0.1.32-ukmesh.3](https://github.com/gadgethd/hopreach/tree/v0.1.32-ukmesh.3) | AGPL-3.0 plus Commons Clause |
| [NumPy](https://numpy.org) | BSD 3-Clause |
| [SciPy](https://scipy.org) | BSD 3-Clause |
| [Shapely](https://shapely.readthedocs.io) | BSD 3-Clause |
| [GDAL](https://gdal.org) | MIT/X |
| [psycopg2](https://www.psycopg.org) | LGPL v3 |
| [redis-py](https://github.com/redis/redis-py) | MIT |
| [Requests](https://requests.readthedocs.io) | Apache 2.0 |

### Infrastructure
| Tool | License |
|---|---|
| [TimescaleDB](https://www.timescale.com) | Apache 2.0 (Community) |
| [Redis](https://redis.io) | BSD 3-Clause |
| [Eclipse Mosquitto](https://mosquitto.org) | EPL 2.0 / EDL 1.0 |
| [Docker](https://www.docker.com) | Apache 2.0 |

### Data
| Source | License |
|---|---|
| [SRTM Elevation Data](https://registry.opendata.aws/terrain-tiles) | Public Domain (NASA) |
| [Natural Earth](https://www.naturalearthdata.com) | Public Domain |

---

## License

UK Mesh-authored code is licensed under MIT — see [LICENSE](LICENSE).
Vendored HopReach and derived RF integration files retain AGPL-3.0 plus the
Commons Clause; see `third_party/hopreach/LICENSE` and
`rf-coverage/SOURCE-OFFER.md`.

**Note on dependencies:** Eclipse Mosquitto (EPL 2.0) is used as a dependency but not modified. Other runtime dependencies use MIT, BSD, or Apache 2.0 licenses.
