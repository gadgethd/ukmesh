import React from 'react';

interface LibEntry {
  name: string;
  role: string;
  url: string;
}

const LibCard: React.FC<LibEntry> = ({ name, role, url }) => (
  <a className="lib-card" href={url} target="_blank" rel="noopener noreferrer">
    <span className="lib-card__name">{name}</span>
    <span className="lib-card__role">{role}</span>
  </a>
);

const FRONTEND: LibEntry[] = [
  { name: 'React 18',            role: 'Component-based UI framework',                              url: 'https://react.dev' },
  { name: 'Vite',                role: 'Fast build tool and dev server',                             url: 'https://vitejs.dev' },
  { name: 'TypeScript',          role: 'Static typing across the entire codebase',                   url: 'https://www.typescriptlang.org' },
  { name: 'MapLibre GL JS',      role: 'GPU-rendered vector/raster map engine',                      url: 'https://maplibre.org' },
  { name: 'deck.gl',             role: 'WebGL overlay for animated packet arc trails',               url: 'https://deck.gl' },
  { name: '@deck.gl/mapbox',     role: 'Native deck.gl integration with the MapLibre map',           url: 'https://deck.gl/docs/api-reference/mapbox/overview' },
  { name: 'Zustand',             role: 'Lightweight client state management for UI atoms',           url: 'https://zustand-demo.pmnd.rs' },
  { name: 'react-router',        role: 'Client-side routing between pages',                          url: 'https://reactrouter.com' },
  { name: 'Recharts',            role: 'Chart components for stats and history graphs',              url: 'https://recharts.org' },
  { name: 'polygon-clipping',    role: 'Client-side geospatial polygon operations',                   url: 'https://github.com/mfogel/polygon-clipping' },
];

const BACKEND: LibEntry[] = [
  { name: 'Node.js',                          role: 'JavaScript runtime',                                      url: 'https://nodejs.org' },
  { name: 'Express',                          role: 'HTTP API server',                                          url: 'https://expressjs.com' },
  { name: 'TypeScript',                       role: 'Static typing across the entire codebase',                 url: 'https://www.typescriptlang.org' },
  { name: '@michaelhart/meshcore-decoder',    role: 'Community decoder for raw MeshCore LoRa packets',          url: 'https://www.npmjs.com/package/@michaelhart/meshcore-decoder' },
  { name: 'MQTT.js',                          role: 'MQTT broker client for packet ingestion',                  url: 'https://github.com/mqttjs/MQTT.js' },
  { name: 'ws',                               role: 'WebSocket server for live dashboard updates',              url: 'https://github.com/websockets/ws' },
  { name: 'ioredis',                          role: 'Redis pub/sub for cross-process live events',              url: 'https://github.com/redis/ioredis' },
  { name: 'pg',                               role: 'PostgreSQL client',                                        url: 'https://node-postgres.com' },
  { name: 'compression',                      role: 'Gzip compression for API responses',                       url: 'https://github.com/expressjs/compression' },
  { name: 'cors',                             role: 'CORS middleware for cross-origin API access',              url: 'https://github.com/expressjs/cors' },
  { name: 'express-rate-limit',               role: 'API rate limiting',                                        url: 'https://github.com/express-rate-limit/express-rate-limit' },
  { name: 'dockerode',                        role: 'Docker API client for dynamic Mosquitto ACL management',   url: 'https://github.com/apocas/dockerode' },
];

const INFRA: LibEntry[] = [
  { name: 'TimescaleDB',                        role: 'Time-series PostgreSQL for packet, node, and link storage',  url: 'https://www.timescale.com' },
  { name: 'Redis',                              role: 'Pub/sub bus for real-time updates between processes',        url: 'https://redis.io' },
  { name: 'Mosquitto',                          role: 'Lightweight MQTT broker',                                    url: 'https://mosquitto.org' },
  { name: 'Docker + Compose',                   role: 'Service containerisation and orchestration',                 url: 'https://docs.docker.com/compose' },
  { name: 'Cloudflare Tunnel',                  role: 'Zero-config secure public access without open ports',        url: 'https://developers.cloudflare.com/cloudflare-one/connections/connect-networks' },
  { name: 'Grafana',                            role: 'Log and metrics dashboards',                                 url: 'https://grafana.com' },
  { name: 'Loki',                               role: 'Log aggregation backend',                                    url: 'https://grafana.com/oss/loki' },
  { name: 'Promtail',                           role: 'Log collector and shipper to Loki',                          url: 'https://grafana.com/docs/loki/latest/send-data/promtail' },
  { name: 'meshcore-health-check',              role: 'Observer coverage tool powering healthcheck.ukmesh.com',     url: 'https://github.com/yellowcooln/meshcore-health-check' },
];

const RF_AND_GEOSPATIAL: LibEntry[] = [
  { name: 'HopReach v0.1.32',      role: 'Canonical terrain propagation and progressive RF rasters (AGPL-3.0 + Commons Clause)', url: 'https://github.com/A13xB0/hopreach' },
  { name: 'Go',                    role: 'HopReach calculator runtime and CPU raster engine',                    url: 'https://go.dev' },
  { name: 'Natural Earth',         role: 'Versioned GB, Northern Ireland, Isle of Man, Jersey, and Guernsey boundary', url: 'https://www.naturalearthdata.com' },
  { name: 'AWS Terrain Tiles',     role: 'Public-domain elevation tiles used by HopReach',                       url: 'https://registry.opendata.aws/terrain-tiles' },
  { name: 'scipy + numpy',         role: 'Observed-link path-loss processing retained in the link worker',       url: 'https://scipy.org' },
  { name: 'GDAL',                  role: 'Terrain raster access retained by the observed-link worker',           url: 'https://gdal.org' },
  { name: 'psycopg2',              role: 'PostgreSQL client for observed-link processing',                       url: 'https://www.psycopg.org' },
  { name: 'redis-py',              role: 'Redis client for the bounded observed-link queue',                     url: 'https://github.com/redis/redis-py' },
];

const Section: React.FC<{ title: string; items: LibEntry[] }> = ({ title, items }) => (
  <section className="prose-section">
    <h2>{title}</h2>
    <div className="lib-grid">
      {items.map(lib => <LibCard key={lib.name} {...lib} />)}
    </div>
  </section>
);

export const OpenSourcePage: React.FC = () => (
  <>

    <div className="site-content site-prose">

      <section className="prose-section">
        <div className="oss-banner">
          <span className="oss-banner__icon">⚗️</span>
          <div>
            <strong>The source code is on GitHub</strong>
            <p>
              The full source code for this dashboard is publicly available: backend, frontend,
              HopReach integration, observed-link worker, and Docker setup. If you run a MeshCore network and want to set up
              your own analytics instance, everything you need is there.
            </p>
            <a
              href="https://github.com/gadgethd/ukmesh"
              target="_blank"
              rel="noopener noreferrer"
              className="site-btn site-btn--primary"
            >
              View on GitHub →
            </a>
          </div>
        </div>
      </section>

      <Section title="Frontend" items={FRONTEND} />
      <Section title="Backend" items={BACKEND} />
      <Section title="Infrastructure" items={INFRA} />
      <Section title="RF & Geospatial" items={RF_AND_GEOSPATIAL} />

      <section className="prose-section">
        <h2>MeshCore itself</h2>
        <p>
          The network runs on{' '}
          <a href="https://github.com/meshcore-dev/MeshCore" target="_blank" rel="noopener noreferrer">MeshCore</a>,
          an open-source LoRa mesh firmware project. Without it, none of this would exist.
        </p>
        <p>
          The packet decoder we use,{' '}
          <code>@michaelhart/meshcore-decoder</code>, is a separate community project that
          reverse-engineered the MeshCore wire format, and is what lets us decode raw radio
          packets into structured data in real time.
        </p>
        <p>
          The MeshCore community Discord is at{' '}
          <a href="https://meshcore.gg/" target="_blank" rel="noopener noreferrer">
            meshcore.gg
          </a>.
        </p>
      </section>

    </div>
  </>
);
