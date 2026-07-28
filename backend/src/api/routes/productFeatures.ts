import type { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import { expandResolverScope } from '../../networks.js';

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
      const result = await query<{
        node_id: string; name: string | null; lat: number; lon: number;
        active_hours: string; packets_48h: string; unique_src_48h: string;
      }>(
        `WITH observer_activity AS (
           SELECT p.rx_node_id,
             COUNT(DISTINCT date_trunc('hour', p.time)) AS active_hours,
             COUNT(*) AS packets_48h,
             COUNT(DISTINCT p.src_node_id) FILTER (WHERE p.src_node_id IS NOT NULL) AS unique_src_48h
           FROM packets p
           WHERE p.time > NOW() - INTERVAL '48 hours'
             AND p.network = ANY($1::text[])
             AND p.rx_node_id IS NOT NULL
           GROUP BY p.rx_node_id
         )
         SELECT n.node_id, n.name, n.lat, n.lon,
           oa.active_hours::text, oa.packets_48h::text, oa.unique_src_48h::text
         FROM observer_activity oa
         JOIN nodes n ON n.node_id = oa.rx_node_id
         WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
           AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')`,
        [networks],
      );
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
    const parts = decodeURIComponent(String(req.params['id'] ?? '')).split(/:|--/).map((part) => part.trim().toUpperCase());
    if (parts.length !== 2 || parts.some((part) => !/^[0-9A-F]{6,64}$/.test(part))) {
      res.status(400).json({ error: 'Link id must be nodeA:nodeB' });
      return;
    }
    const hours = Math.max(1, Math.min(168, Math.floor(Number(req.query['hours'] ?? 72) || 72)));
    try {
      const [nodeA, nodeB] = parts.sort();
      const result = await query<{
        time: string; snr: number | null; rssi: number | null; path_loss: number | null; sample_count: number;
      }>(
        `SELECT reports.last_seen::text AS time, reports.last_snr_db AS snr,
                NULL::double precision AS rssi, links.itm_path_loss_db AS path_loss,
                reports.sample_count
         FROM node_link_radio_reports reports
         LEFT JOIN node_links links
           ON links.node_a_id = reports.node_a_id AND links.node_b_id = reports.node_b_id
         WHERE reports.node_a_id = $1 AND reports.node_b_id = $2
           AND reports.last_seen > NOW() - ($3::text || ' hours')::interval
         ORDER BY reports.last_seen ASC`,
        [nodeA, nodeB, String(hours)],
      );
      res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
      res.json({ linkId: `${nodeA}:${nodeB}`, hours, points: result.rows });
    } catch (error) {
      console.error('[api] GET /links/:id/history', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/repeaters/firmware', async (req, res) => {
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const networks = expandResolverScope(network);
      const result = await query<{ hardware_model: string | null; firmware_version: string | null; count: string }>(
        `SELECT COALESCE(hardware_model, 'Unknown') AS hardware_model,
                COALESCE(NULLIF(firmware_version, ''), 'Unknown') AS firmware_version,
                COUNT(*)::text AS count
         FROM nodes
         WHERE network = ANY($1::text[])
           AND (role IS NULL OR role = 2)
           AND last_seen > NOW() - INTERVAL '30 days'
           AND (name IS NULL OR name NOT LIKE '%🚫%')
         GROUP BY 1, 2
         ORDER BY COUNT(*) DESC, 1, 2`,
        [networks],
      );
      const versions = result.rows.map((row) => ({ ...row, count: Number(row.count) }));
      res.setHeader('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
      res.json({ total: versions.reduce((sum, row) => sum + row.count, 0), versions });
    } catch (error) {
      console.error('[api] GET /repeaters/firmware', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/observers/register', registrationLimiter, async (req, res) => {
    const body = req.body as { publicKey?: string; iata?: string; name?: string; contact?: string } | undefined;
    const publicKey = String(body?.publicKey ?? '').trim().toUpperCase();
    const iata = String(body?.iata ?? '').trim().toUpperCase();
    const name = String(body?.name ?? '').trim().slice(0, 100);
    const contact = String(body?.contact ?? '').trim().slice(0, 200);
    if (!/^[0-9A-F]{64}$/.test(publicKey) || !/^[A-Z0-9]{2,8}$/.test(iata) || !contact) {
      res.status(400).json({ error: 'A 64-character public key, IATA region, and contact are required' });
      return;
    }
    try {
      const result = await query<{ id: string }>(
        `INSERT INTO observer_registration_requests (public_key, iata, display_name, contact)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (public_key) DO UPDATE SET
           iata = EXCLUDED.iata, display_name = EXCLUDED.display_name,
           contact = EXCLUDED.contact, status = 'pending', updated_at = NOW()
         RETURNING id::text`,
        [publicKey, iata, name || null, contact],
      );
      res.status(202).json({ ok: true, requestId: result.rows[0]?.id, status: 'pending' });
    } catch (error) {
      console.error('[api] POST /observers/register', (error as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
