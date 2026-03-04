# MeshCore Analytics

A real-time mesh network analytics platform for [MeshCore](https://meshcore.co.uk) networks. Ingests MQTT packets via `mctomqtt`, decodes them with `@michaelhart/meshcore-decoder`, stores them in TimescaleDB, and presents a live RF/signals-intelligence-style dashboard with node mapping, coverage viewshed polygons, packet statistics, and a decoded live feed.

---

## Features

- Live map of repeater nodes with animated packet arcs between observers
- RF coverage viewshed polygons computed per node using SRTM terrain data
- Gap detection overlay showing areas without coverage
- Decoded live packet feed (Adverts, Group messages, DMs, ACKs, Trace routes)
- Statistics page with charts: packet rates, unique radios, hop distribution, top chatters
- 28-day rolling packet retention via TimescaleDB
- Multi-observer support: duplicate packets deduplicated by hash

---

## Roadmap

### Phase 1 — Core platform (complete)
- MQTT ingestion via `mctomqtt` with multi-observer support
- Packet decoding with `@michaelhart/meshcore-decoder`
- TimescaleDB storage with 28-day rolling retention
- Live WebSocket feed to browser clients
- React dashboard: node map, animated packet arc trails, decoded live feed
- TX/RX deduplication by packet hash

### Phase 2 — RF coverage (complete)
- Viewshed worker: SRTM terrain-aware radio horizon computation per repeater
- Coverage polygons served as GeoJSON and rendered on the map
- Gap detection overlay highlighting areas with no coverage
- Dynamic radius calculation based on node elevation
- UK mainland clipping to remove sea coverage artefacts

### Phase 3 — Repeater owner portal (planned)
- Ed25519 JWT authentication for repeater owners
- Owner-facing dashboard: per-node packet history, advert counts, RSSI/SNR trends from observers that heard it
- Planned node placement tool: drop a marker on the map, preview estimated RF coverage before deploying hardware
- Repeater registration: owners can claim a node by public key, add contact details and notes. Nodes must be actively publishing to MQTT to be claimable.

### Phase 4 — Public website (complete)
- Separate public-facing site at a different hostname from the analytics dashboard
- Node documentation, install guides, MQTT connection instructions
- Network statistics page with charts
- Packet type reference page

### Phase 5 — Network intelligence (planned)
- Network topology graph: visualise which repeaters most frequently relay for each other
- Multi-observer correlation: show all observers that heard the same packet on the map simultaneously
- Multi-network support: remove the hardcoded IATA filter and add a network selector, making the platform usable by other MeshCore communities out of the box

### Phase 6 — Predicted vs actual coverage (planned, stretch goal)
- Overlay viewshed coverage polygons against where packets from each repeater have actually been received
- Highlight discrepancies between terrain model predictions and observed real-world range
- Requires a sufficient number of nodes publishing to MQTT to build a meaningful received-signal dataset

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
docker compose logs -f app
```

The app will be available at `http://localhost:3000`.

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
| `ALLOWED_ORIGINS` | `http://localhost:3000,http://localhost:3001` | Comma-separated browser origins allowed for CORS and WebSocket |
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
5. Configure public hostnames in the Cloudflare dashboard:
   - `app.yourdomain.com` → `http://app:3000`
   - `mqtt.yourdomain.com` → `http://mosquitto:9001` (WebSocket)

---

## MQTT Topic Structure

The backend subscribes to `meshcore/#`. MeshCore devices publish via `mctomqtt` to topics of the form:

```
meshcore/<IATA>/<observer-public-key>/packets   # received/transmitted packets
meshcore/<IATA>/<observer-public-key>/status    # node status advertisement
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
     │ subscribe meshcore/#
     ▼
 App (Node.js/TypeScript)
     │
     ├─ meshcore-decoder → TimescaleDB (packets · 28d retention)
     │                     (nodes, planned_nodes, observers · persistent)
     │
     ├─ Redis pub/sub
     │
     ├─ WebSocket → Frontend live updates
     └─ REST API /api/*
          │
          └─ Static Frontend (React + Leaflet + deck.gl)

 Viewshed Worker (Python)
     ├─ Redis job queue
     ├─ SRTM terrain tiles (auto-downloaded)
     └─ node_coverage table → coverage polygons served via /api/coverage
```

---

## Services

| Service | Image | Purpose |
|---|---|---|
| `timescaledb` | `timescale/timescaledb:latest-pg16` | Time-series packet storage with retention |
| `mosquitto` | `eclipse-mosquitto:2` | MQTT broker (WebSocket only) |
| `redis` | `redis:7-alpine` | WebSocket fan-out pub/sub and job queue |
| `app` | Built from `Dockerfile` | Backend API + frontend static files |
| `viewshed-worker` | Built from `viewshed-worker/Dockerfile` | Terrain-aware RF coverage computation |
| `cloudflared` | `cloudflare/cloudflared` | Optional Cloudflare Tunnel (use `--profile tunnel`) |

---

## Data Retention

- **Packets** hypertable: automatic 28-day retention via TimescaleDB retention policy applied on first startup
- **Nodes**, **planned\_nodes**, **observers**, **node\_coverage**: persist indefinitely

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
| [rasterio](https://rasterio.readthedocs.io) | BSD 3-Clause |
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
