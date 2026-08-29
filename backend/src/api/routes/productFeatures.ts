import type { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { expandResolverScope } from '../../networks.js';
import { parseBoundedInteger, parseBoundedString } from '../utils/input.js';
import {
  linkHistoryRows,
  normalizeObserverRegistration,
  observerHealthRows,
  repeaterFirmwareRows,
  submitObserverRegistration,
  visibleLinkNodeIds,
} from '../../repositories/productFeatures.js';
import { API_ERROR_CODES, sendApiError } from '../errors.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(text: string, params?: unknown[]) => Promise<{ rows: T[] }>;

const registrationLimiter = rateLimit({
  windowMs: 60 * 60_000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many registration requests; try again later' },
});

export function registerProductFeatureRoutes(router: Router, query: QueryFn): void {
  router.get('/observers/health', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const networks = expandResolverScope(network);
      const result = await observerHealthRows(query, networks);
      const maxPackets = Math.max(1, ...result.rows.map((row) => Number(row.packets_48h)));
      res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
      res.json(result.rows.map((row) => {
        const uptime = Math.min(100, (Number(row.active_hours) / 48) * 100);
        const packetCoverage = Math.min(100, (Number(row.packets_48h) / maxPackets) * 100);
        const uniqueSources = Number(row.unique_src_48h);
        const score = Math.round(uptime * 0.5 + packetCoverage * 0.25 + Math.min(100, uniqueSources * 2) * 0.25);
        return {
          node_id: row.node_id,
          name: row.name,
          lat: Number(row.lat),
          lon: Number(row.lon),
          uptime_pct: Math.round(uptime * 10) / 10,
          packet_coverage: Math.round(packetCoverage * 10) / 10,
          unique_src_48h: uniqueSources,
          score,
          quality: score >= 75 ? 'good' : score >= 45 ? 'watch' : 'poor',
        };
      }));
    } catch (error) {
      console.error('[api] GET /observers/health', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/links/:id/history', async (req, res) => {
    const linkId = parseBoundedString(req.params['id'], {
      name: 'link id',
      required: true,
      maxLength: 132,
      pattern: /^[0-9a-fA-F:-]+$/,
    })!;
    const parts = linkId.split(/:|--/).map((part) => part.trim().toUpperCase());
    if (parts.length !== 2 || parts.some((part) => !/^[0-9A-F]{6,64}$/.test(part))) {
      sendApiError(res, 400, 'Link id must be nodeA:nodeB', API_ERROR_CODES.invalidLinkId);
      return;
    }
    if (parts[0] === parts[1]) {
      sendApiError(
        res,
        400,
        'Link id must contain two different nodes',
        API_ERROR_CODES.invalidLinkId,
      );
      return;
    }
    const hours = parseBoundedInteger(req.query['hours'], {
      name: 'hours',
      defaultValue: 72,
      min: 1,
      max: 168,
    });
    try {
      const [nodeA, nodeB] = parts.sort();
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const networks = expandResolverScope(network);
      const visible = await visibleLinkNodeIds(query, [nodeA, nodeB], networks);
      if (new Set(visible.rows.map((row) => row.node_id.toUpperCase())).size !== 2) {
        res.status(404).json({ error: 'Link not found' });
        return;
      }
      const result = await linkHistoryRows(query, nodeA, nodeB, hours);
      res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
      res.json({ linkId: `${nodeA}:${nodeB}`, network, hours, points: result.rows });
    } catch (error) {
      console.error('[api] GET /links/:id/history', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/repeaters/firmware', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const networks = expandResolverScope(network);
      const result = await repeaterFirmwareRows(query, networks);
      const versions = result.rows.map((row) => ({ ...row, count: Number(row.count) }));
      res.setHeader('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
      res.json({ total: versions.reduce((sum, row) => sum + row.count, 0), versions });
    } catch (error) {
      console.error('[api] GET /repeaters/firmware', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/observers/register', registrationLimiter, async (req, res) => {
    let input;
    try {
      input = normalizeObserverRegistration(req.body);
    } catch {
      sendApiError(
        res,
        400,
        'A 64-character public key, IATA region, and contact are required',
        API_ERROR_CODES.invalidObserverRegistration,
      );
      return;
    }
    try {
      const result = await submitObserverRegistration(query, input);
      res.status(202).json({
        ok: true,
        requestId: result.requestId,
        status: result.accepted ? 'pending' : 'already_received',
      });
    } catch (error) {
      console.error('[api] POST /observers/register', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
