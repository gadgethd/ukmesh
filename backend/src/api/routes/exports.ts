import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { createHash } from 'node:crypto';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import fs from 'node:fs';
import path from 'node:path';
import { csvRow } from '../utils/csv.js';
import {
  parseBoundedInteger,
  parseEnum,
  parseHexIdentifier,
} from '../utils/input.js';
export { csvCell } from '../utils/csv.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;
type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  exportLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  /** Current public privacy generation; a cache entry is only reusable for the
   * generation it was produced under, so a redaction must invalidate exports. */
  getPublicVisibilityGeneration: () => Promise<number>;
};

/** Exports are cacheable, but only for the privacy generation they were built
 * under. `v` lets a client name the generation it expects: a mismatch is
 * answered with 412 + no-store instead of replaying data from a scope that has
 * since been redacted. */
function exportTag(
  generation: number,
  scope: string,
  format: string,
  limit: number,
): string {
  const digest = createHash('sha256')
    .update(JSON.stringify([generation, scope, format, limit]))
    .digest('base64url')
    .slice(0, 22);
  return `W/"nodes-${generation}-${digest}"`;
}

export function registerExportRoutes(router: Router, deps: Deps): void {
  router.get('/v1', (_req, res) => {
    res.setHeader('X-API-Version', '1');
    res.json({
      version: 1,
      documentation: '/api/v1/openapi.yaml',
      resources: ['/api/v1/exports/nodes.csv', '/api/v1/exports/nodes.geojson', '/api/v1/exports/path.gpx?packet=…'],
    });
  });

  router.get('/v1/openapi.yaml', (_req, res) => {
    const candidates = [path.resolve(process.cwd(), 'openapi.yaml'), path.resolve(process.cwd(), '../docs/openapi.yaml')];
    const file = candidates.find((candidate) => fs.existsSync(candidate));
    if (!file) {
      res.status(404).type('text/plain').send('OpenAPI document unavailable');
      return;
    }
    res.setHeader('X-API-Version', '1');
    res.type('application/yaml').sendFile(file);
  });

  router.get('/v1/exports/path.gpx', deps.exportLimiter, async (req, res) => {
    const packet = parseHexIdentifier(req.query['packet'], {
      name: 'packet',
      maxLength: 128,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const result = await deps.query<{ node_id: string; name: string | null; lat: number; lon: number; ord: number }>(
        `WITH target AS (
           SELECT p.path_hashes
           FROM packets p
           WHERE p.packet_hash = $${filters.params.length + 1}
             ${filters.packetsAlias('p')}
           ORDER BY p.time DESC
           LIMIT 1
         ),
         hops AS (
           SELECT upper(hash) AS hash, ord::integer
           FROM target, unnest(path_hashes) WITH ORDINALITY AS value(hash, ord)
         )
         SELECT DISTINCT ON (h.ord) n.node_id, n.name, n.lat, n.lon, h.ord
         FROM hops h
         JOIN node_identity_nodes n ON upper(n.node_id) LIKE h.hash || '%'
         WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
           ${filters.nodesAlias('n')}
         ORDER BY h.ord, n.last_seen DESC NULLS LAST`,
        [...filters.params, packet],
      );
      if (result.rows.length === 0) {
        res.status(404).json({ error: 'No public positioned path is available for this packet' });
        return;
      }
      const xml = (value: string) => value.replace(/[<>&'"]/g, (char) => ({
        '<': '&lt;', '>': '&gt;', '&': '&amp;', "'": '&apos;', '"': '&quot;',
      })[char]!);
      const points = result.rows.map((row) => (
        `      <trkpt lat="${Number(row.lat).toFixed(6)}" lon="${Number(row.lon).toFixed(6)}"><name>${xml(row.name ?? row.node_id.slice(0, 12))}</name></trkpt>`
      )).join('\n');
      res.setHeader('Content-Disposition', `attachment; filename="meshcore-path-${packet.slice(0, 16)}.gpx"`);
      res.type('application/gpx+xml').send(
        `<?xml version="1.0" encoding="UTF-8"?>\n<gpx version="1.1" creator="MeshCore Analytics" xmlns="http://www.topografix.com/GPX/1/1">\n  <trk><name>Packet ${xml(packet)}</name><trkseg>\n${points}\n  </trkseg></trk>\n</gpx>\n`,
      );
    } catch (error) {
      console.error('[api] GET /v1/exports/path.gpx', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/v1/exports/nodes.:format', deps.exportLimiter, async (req, res) => {
    const rawFormat = req.params['format'];
    const format = parseEnum(
      (Array.isArray(rawFormat) ? rawFormat[0] : rawFormat)?.toLowerCase(),
      {
        name: 'format',
        values: ['csv', 'geojson'] as const,
      },
    );
    if (!format) {
      res.status(404).json({ error: 'Supported formats are csv and geojson' });
      return;
    }
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 5_000,
      min: 1,
      max: 5_000,
    });
    const requestedGeneration = req.query['v'] === undefined || req.query['v'] === ''
      ? undefined
      : parseBoundedInteger(req.query['v'], {
        name: 'v',
        min: 1,
        max: Number.MAX_SAFE_INTEGER,
      });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      let visibilityGeneration: number;
      try {
        visibilityGeneration = await deps.getPublicVisibilityGeneration();
      } catch (generationError) {
        console.error('[api] GET /v1/exports/nodes visibility generation', (generationError as Error).message);
        res.setHeader('Cache-Control', 'no-store');
        res.status(503).json({ error: 'Export visibility state is unavailable' });
        return;
      }
      if (requestedGeneration !== undefined && requestedGeneration !== visibilityGeneration) {
        res.setHeader('Cache-Control', 'no-store');
        res.setHeader('Vary', 'Accept-Encoding');
        res.setHeader('X-API-Version', '1');
        res.setHeader('X-Visibility-Generation', String(visibilityGeneration));
        res.status(412).json({
          error: 'Export scope changed',
          visibility_generation: visibilityGeneration,
        });
        return;
      }
      const limitParam = `$${filters.params.length + 1}`;
      const result = await deps.query<{
        node_id: string; name: string | null; lat: number | null; lon: number | null;
        role: number | null; iata: string | null; last_seen: string | null; hardware_model: string | null;
      }>(
        `SELECT node_id, name, lat, lon, role, iata, last_seen::text, hardware_model
         FROM node_identity_nodes AS nodes
         WHERE lat IS NOT NULL AND lon IS NOT NULL
           AND (name IS NULL OR name NOT LIKE '%🚫%')
           ${filters.nodes}
         ORDER BY last_seen DESC NULLS LAST, node_id
         LIMIT ${limitParam}`,
        [...filters.params, limit],
      );
      const nodes = result.rows;
      const confirmedGeneration = await deps.getPublicVisibilityGeneration();
      res.setHeader('X-API-Version', '1');
      res.setHeader('X-Visibility-Generation', String(confirmedGeneration));
      // A redaction that lands while the rows are being read must not be
      // published as cacheable: the body may predate it.
      res.setHeader(
        'Cache-Control',
        confirmedGeneration === visibilityGeneration
          ? 'public, max-age=60, stale-while-revalidate=60'
          : 'no-store',
      );
      res.setHeader('ETag', exportTag(confirmedGeneration, network, format, limit));
      res.setHeader('Vary', 'Accept-Encoding');
      if (format === 'geojson') {
        res.type('application/geo+json').json({
          type: 'FeatureCollection',
          generatedAt: new Date().toISOString(),
          features: nodes.map((node) => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [node.lon, node.lat] },
            properties: {
              node_id: node.node_id,
              name: node.name,
              role: node.role,
              iata: node.iata,
              last_seen: node.last_seen,
              hardware_model: node.hardware_model,
            },
          })),
        });
        return;
      }
      const columns = ['node_id', 'name', 'lat', 'lon', 'role', 'iata', 'last_seen', 'hardware_model'] as const;
      const lines = [
        csvRow(columns),
        ...nodes.map((node) => csvRow(columns.map((column) => node[column]))),
      ];
      res.setHeader('Content-Disposition', 'attachment; filename="ukmesh-nodes.csv"');
      res.type('text/csv').send(`${lines.join('\n')}\n`);
    } catch (err) {
      console.error('[api] GET /v1/exports/nodes', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
