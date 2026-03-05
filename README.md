# MeshCore Analytics

A real-time analytics platform for [MeshCore](https://meshcore.co.uk) networks. It ingests MQTT packets via `mctomqtt`, decodes them with `@michaelhart/meshcore-decoder`, stores them in TimescaleDB, and serves two production sites (`teesside` + `ukmesh`) with live mapping, link intelligence, coverage modelling, packet analytics, and worker/system health.

---

## Features

- Real-time node map with animated packet arcs and live WebSocket updates
- RF coverage viewshed polygons per repeater using SRTM terrain data
- Link intelligence overlay with directional observations and path-loss viability
- Beta path prediction model with hourly path-learning prior rebuilds
- Decoded live packet feed (Advert, GroupText, DM, ACK, Path, Trace)
- Stats pages and chart endpoints for packet rates, radios, hops, and activity
- Public Health page with worker status/history + server resource metrics
- Multi-network ingestion (`meshcore/*` and `ukmesh/*`) with per-site filtering
- Multi-observer deduplication by packet hash

---

## Current State

- Multi-site deployment:
  - `app.teessidemesh.com` / `www.teessidemesh.com`
  - `app.ukmesh.com` / `www.ukmesh.com`
- Split worker architecture for resilience:
  - `viewshed-worker` (coverage compute)
  - `link-worker` (link/path-loss processing)
  - `path-learning-worker` (hourly model rebuild)
  - `health-worker` (health snapshots)
  - `link-backfill-worker` (one-shot historical backfill)
- Nginx frontend proxies use Docker DNS resolver-based upstreams to avoid stale backend IP issues after container recreates.

---

## Quick Start

```bash
# 1. Clone and enter the project
git clone https://github.com/gadgethd/meshcore-analytics.git
cd meshcore-analytics

# 2. Copy and configure environment
cp .env.example .env
# Edit .env — at minimum set POSTGRES_PASSWORD, JWT_SECRET, MQTT_PASSWORD

# 3. Start everything
docker compose up -d

# 4. Check logs
docker compose logs -f backend
```

Local endpoints:

- Backend API/WS: `http://localhost:3000`
- Teesside app: `http://localhost:3001`
- Teesside website: `http://localhost:3002`
- UKMesh app: `http://localhost:3003`
- UKMesh website: `http://localhost:3004`

To expose it publicly, configure a Cloudflare Tunnel (see below) or reverse proxy of your choice.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values. All variables used by the app:

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `meshcore` | TimescaleDB database name |
| `POSTGRES_USER` | `meshcore` | TimescaleDB user |
| `POSTGRES_PASSWORD` | *(required)* | TimescaleDB password |
| `MQTT_BROKER_URL` | `ws://mosquitto:9001` | Mosquitto WebSocket URL (internal) |
| `MQTT_USERNAME` | `backend` | MQTT client username |
| `MQTT_PASSWORD` | *(required)* | MQTT client password |
| `REDIS_URL` | `redis://redis:6379` | Redis URL for WebSocket pub/sub |
| `JWT_SECRET` | *(required)* | Secret for JWT verification |
| `ALLOWED_ORIGINS` | `http://localhost:3001,http://localhost:3002` | Comma-separated browser origins allowed for CORS and WebSocket |
| `VITE_APP_HOSTNAME` | *(blank — always shows dashboard)* | If set, only this hostname serves the analytics dashboard; all others serve the public website layout |
| `MESHCORE_CHANNEL_SECRETS` | *(blank)* | Comma-separated channel secrets for decrypting GroupText packets. Format: `name:hex` or bare hex. The default MeshCore public channel key is always included. |
| `OPENTOPODATA_API` | `https://api.opentopodata.org` | Elevation API endpoint for viewshed computation |
| `CLOUDFLARE_TUNNEL_TOKEN` | *(optional)* | Cloudflare Zero Trust tunnel token |
| `PORT` | `3000` | Internal app port |

---

## Mosquitto Setup

Mosquitto is configured for WebSocket-only access with password authentication. After first starting the stack, add a password for the backend client and any node clients:

```bash
# Add the backend client password (must match MQTT_PASSWORD in .env)
docker exec meshcore-analytics-mosquitto-1 \
  mosquitto_passwd -b /mosquitto/config/passwd backend your_password

# Add a node client
docker exec meshcore-analytics-mosquitto-1 \
  mosquitto_passwd -b /mosquitto/config/passwd node1 another_password

docker compose restart mosquitto
```

Edit `mosquitto/acl` to grant the appropriate topic permissions to each user.

---

## Cloudflare Tunnel (optional)

To expose the app and MQTT broker publicly without opening firewall ports:

1. Go to [Cloudflare Zero Trust](https://one.dash.cloudflare.com/) → Networks → Tunnels
2. Create a tunnel and copy the token
3. Add to `.env`: `CLOUDFLARE_TUNNEL_TOKEN=<token>`
4. Start with the tunnel profile: `docker compose --profile tunnel up -d`
5. Configure public hostnames in the Cloudflare dashboard (example):
   - `app.teessidemesh.com` → `http://app:80`
   - `www.teessidemesh.com` → `http://website:80`
   - `app.ukmesh.com` → `http://app-ukmesh:80`
   - `www.ukmesh.com` → `http://website-ukmesh:80`
   - `mqtt.teessidemesh.com` → `http://mosquitto:9001`
   - `mqtt.ukmesh.com` → `http://mosquitto:9001`

---

## MQTT Topic Structure

The backend subscribes to both `meshcore/#` and `ukmesh/#`. MeshCore devices publish via `mctomqtt` to topics of the form:

```
meshcore/<IATA>/<observer-public-key>/packets   # received/transmitted packets
meshcore/<IATA>/<observer-public-key>/status    # node status advertisement
ukmesh/<IATA>/<observer-public-key>/packets
ukmesh/<IATA>/<observer-public-key>/status
```

Payloads are JSON envelopes containing a `raw` hex field (the MeshCore packet) plus metadata (RSSI, SNR, direction, hash, etc.).

---

## Architecture

```
MeshCore Devices
     │ LoRa RF
     ▼
 mctomqtt (on node machine)
     │ MQTT over WebSocket/TLS
     ▼
 Mosquitto ─────────────────────────────── (optional Cloudflare Tunnel)
     │ subscribe meshcore/# + ukmesh/#
     ▼
 Backend (Node.js/TypeScript)
     │
     ├─ meshcore-decoder → TimescaleDB (packets, nodes, coverage, priors, health snapshots)
     │
     ├─ Redis pub/sub
     │
     ├─ WebSocket → frontend live updates
     └─ REST API /api/*
     
 App/Web Frontends (Nginx + React)
     ├─ app / app-ukmesh (interactive dashboard)
     └─ website / website-ukmesh (public site + health/stats pages)

 Python Workers
     ├─ viewshed-worker (meshcore:viewshed_jobs)
     ├─ link-worker (meshcore:link_jobs)
     ├─ SRTM terrain tiles (auto-downloaded)
     └─ node_coverage + node_links updates

 Backend Workers (Node.js)
     ├─ path-learning-worker (hourly prior rebuild)
     ├─ health-worker (minute snapshots)
     └─ link-backfill-worker (one-shot historical backfill)
```

---

## Services

| Service | Image | Purpose |
|---|---|---|
| `timescaledb` | `timescale/timescaledb:latest-pg16` | Time-series and relational data storage |
| `mosquitto` | `eclipse-mosquitto:2` | MQTT broker (WebSocket only) |
| `redis` | `redis:7-alpine` | WebSocket fan-out pub/sub and job queue |
| `backend` | Built from `Dockerfile.backend` | MQTT ingest, decoding, API, WebSocket |
| `path-learning-worker` | Built from `Dockerfile.backend` | Hourly path-learning model rebuilds |
| `health-worker` | Built from `Dockerfile.backend` | Periodic health snapshot capture |
| `link-backfill-worker` | Built from `Dockerfile.backend` | One-shot historical link backfill |
| `viewshed-worker` | Built from `viewshed-worker/Dockerfile` | Terrain-aware RF coverage computation |
| `link-worker` | Built from `viewshed-worker/Dockerfile` | Link/path-loss processing from observed paths |
| `app` | Built from `Dockerfile.app` | Teesside interactive dashboard frontend |
| `website` | Built from `Dockerfile.website` | Teesside public website frontend |
| `app-ukmesh` | Built from `Dockerfile.app` | UKMesh interactive dashboard frontend |
| `website-ukmesh` | Built from `Dockerfile.website` | UKMesh public website frontend |
| `cloudflared` | `cloudflare/cloudflared` | Optional Cloudflare Tunnel (use `--profile tunnel`) |

---

## Data Retention

- Packet retention policy is currently disabled. Historical data is kept indefinitely unless explicitly pruned.
- Node/link/coverage/path-learning/health tables are also retained indefinitely by default.

---

## Acknowledgements

This project is built on the following open source libraries and tools:

### Frontend
| Package | License |
|---|---|
| [React](https://react.dev) | MIT |
| [Vite](https://vitejs.dev) | MIT |
| [TypeScript](https://www.typescriptlang.org) | Apache 2.0 |
| [Leaflet](https://leafletjs.com) | BSD 2-Clause |
| [react-leaflet](https://react-leaflet.js.org) | Hippocratic 2.1 |
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

### Viewshed worker (Python)
| Package | License |
|---|---|
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

This project is licensed under MIT — see [LICENSE](LICENSE).

**Note on dependencies:** react-leaflet (Hippocratic License 2.1) and Eclipse Mosquitto (EPL 2.0) are used as dependencies but not modified or redistributed. All other runtime dependencies use MIT, BSD, or Apache 2.0 licenses. The Hippocratic License adds ethical use clauses not present in standard open source licenses.
