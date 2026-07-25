import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { isPrivateNode, redactPrivateNode } from '../utils/privateNode.js';
import fs from 'node:fs';
import path from 'node:path';
import { csvRow } from '../utils/csv.js';
export { csvCell } from '../utils/csv.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;
type Deps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export function registerExportRoutes(router: Router, deps: Deps): void {
  router.get('/v1', (_req, res) => {
    res.setHeader('X-API-Version', '1');
    res.json({
      version: 1,
      documentation: '/api/v1/openapi.yaml',
      resources: ['/api/v1/exports/nodes.csv', '/api/v1/exports/nodes.geojson'],
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

  router.get('/v1/exports/nodes.:format', deps.limiter, async (req, res) => {
    try {
      const format = String(req.params['format'] ?? '').toLowerCase();
      if (format !== 'csv' && format !== 'geojson') {
        res.status(404).json({ error: 'Supported formats are csv and geojson' });
        return;
      }
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers, 'ukmesh');
      const network = requestedNetwork === 'test' ? 'test' : 'ukmesh';
      const requestedLimit = Number(req.query['limit'] ?? 5_000);
      const limit = Number.isFinite(requestedLimit) ? Math.min(5_000, Math.max(1, Math.round(requestedLimit))) : 5_000;
      const filters = deps.networkFilters(network);
      const limitParam = `$${filters.params.length + 1}`;
      const result = await deps.query<{
        node_id: string; name: string | null; lat: number | null; lon: number | null;
        role: number | null; iata: string | null; last_seen: string | null; hardware_model: string | null;
      }>(
        `SELECT node_id, name, lat, lon, role, iata, last_seen::text, hardware_model
         FROM nodes
         WHERE lat IS NOT NULL AND lon IS NOT NULL
           ${filters.nodes}
         ORDER BY last_seen DESC NULLS LAST, node_id
         LIMIT ${limitParam}`,
        [...filters.params, limit],
      );
      const nodes = result.rows
        .filter((node) => !isPrivateNode(node.name))
        .map((node) => redactPrivateNode(node));
      res.setHeader('X-API-Version', '1');
      res.setHeader('Cache-Control', 'public, max-age=300, stale-while-revalidate=600');
      if (format === 'geojson') {
        res.type('application/geo+json').json({
          type: 'FeatureCollection',
          generatedAt: new Date().toISOString(),
          features: nodes.map((node) => ({
            type: 'Feature',
            geometry: node.lat == null || node.lon == null
              ? null
              : { type: 'Point', coordinates: [node.lon, node.lat] },
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
