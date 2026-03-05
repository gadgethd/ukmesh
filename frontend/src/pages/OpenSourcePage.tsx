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
  { name: 'Leaflet',             role: 'Interactive tile-based map rendering',                       url: 'https://leafletjs.com' },
  { name: 'react-leaflet',       role: 'React bindings for Leaflet',                                 url: 'https://react-leaflet.js.org' },
  { name: 'deck.gl',             role: 'WebGL overlay for animated packet arc trails',               url: 'https://deck.gl' },
  { name: 'react-router-dom',    role: 'Client-side routing between pages',                          url: 'https://reactrouter.com' },
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
];

const INFRA: LibEntry[] = [
  { name: 'TimescaleDB',          role: 'Time-series PostgreSQL for packet, node, and link storage', url: 'https://www.timescale.com' },
  { name: 'Redis',                role: 'Pub/sub bus for real-time updates between processes',       url: 'https://redis.io' },
  { name: 'Mosquitto',            role: 'Lightweight MQTT broker',                                  url: 'https://mosquitto.org' },
  { name: 'Docker + Compose',     role: 'Service containerisation and orchestration',                url: 'https://docs.docker.com/compose' },
  { name: 'Cloudflare Tunnel',    role: 'Zero-config secure public access without open ports',       url: 'https://developers.cloudflare.com/cloudflare-one/connections/connect-networks' },
];

const GEOSPATIAL: LibEntry[] = [
  { name: 'world-atlas',          role: 'Natural Earth 10m country boundary data',                   url: 'https://github.com/topojson/world-atlas' },
  { name: 'topojson-client',      role: 'Decoding TopoJSON to GeoJSON coordinates',                  url: 'https://github.com/topojson/topojson-client' },
  { name: 'scipy + numpy',        role: 'Viewshed raycasting for terrain line-of-sight calculations',  url: 'https://scipy.org' },
  { name: 'Shapely',              role: 'Polygon intersection for terrain clip and gap detection',    url: 'https://shapely.readthedocs.io' },
  { name: 'SRTM elevation data',  role: 'NASA shuttle radar terrain model via AWS Terrain Tiles',    url: 'https://registry.opendata.aws/terrain-tiles' },
  { name: 'rasterio',            role: 'Reading SRTM .hgt raster elevation tiles',                  url: 'https://rasterio.readthedocs.io' },
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
    <section className="site-page-hero">
      <div className="site-content">
        <h1 className="site-page-hero__title">Open Source</h1>
        <p className="site-page-hero__sub">
          This project is built entirely on free, open-source software, and the full source code is available on GitHub.
        </p>
      </div>
    </section>

    <div className="site-content site-prose">

      <section className="prose-section">
        <div className="oss-banner">
          <span className="oss-banner__icon">⚗️</span>
          <div>
            <strong>The source code is on GitHub</strong>
            <p>
              The full source code for this dashboard is publicly available: backend, frontend,
              viewshed worker, and Docker setup. If you run a MeshCore network and want to set up
              your own analytics instance, everything you need is there.
            </p>
            <a
              href="https://github.com/gadgethd/teesside-mesh"
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
      <Section title="Geospatial & Viewshed" items={GEOSPATIAL} />

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
          <a href="https://discord.gg/bSuST8xvet" target="_blank" rel="noopener noreferrer">
            discord.gg/bSuST8xvet
          </a>.
        </p>
      </section>

    </div>
  </>
);
