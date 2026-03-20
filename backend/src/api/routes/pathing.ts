import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolveRequestNetwork } from '../../http/requestScope.js';
import { normalizeObserverQuery } from '../utils/observer.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type ResolvePoolFn = {
  run<T>(job: { type: 'resolve'; packetHash: string; network: string; observer?: string | null } | { type: 'resolveMulti'; packetHash: string; network: string }): Promise<T | null>;
};

type PathingRouteDeps = {
  pathBetaLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathHistoryLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathLearningLimiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
  pathHistoryCache: Map<string, { ts: number; data: unknown }>;
  pathHistoryCacheTtlMs: number;
  getResolveCache: (key: string) => unknown;
  setResolveCache: (key: string, value: unknown) => void;
  resolvePool: ResolvePoolFn;
  getPathHistoryCache: (scope: string) => Promise<{
    window_start: string | null;
    updated_at: string | null;
    packet_count: number;
    resolved_packet_count: number;
    segment_counts: Array<{ count?: number }> | null;
  } | null>;
  query: QueryFn;
};

export function registerPathingRoutes(router: Router, deps: PathingRouteDeps): void {
  const {
    pathBetaLimiter,
    pathHistoryLimiter,
    pathLearningLimiter,
    pathHistoryCache,
    pathHistoryCacheTtlMs,
    getResolveCache,
    setResolveCache,
    resolvePool,
    getPathHistoryCache,
    query,
  } = deps;

  router.get('/path-beta/resolve', pathBetaLimiter, async (req, res) => {
    try {
      const packetHash = String(req.query['hash'] ?? '').trim();
      if (!packetHash) {
        res.status(400).json({ error: 'Missing hash query parameter' });
        return;
      }
      if (!/^[0-9a-fA-F]{1,128}$/.test(packetHash)) {
        res.status(400).json({ error: 'Invalid hash format' });
        return;
      }
      const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
      const observer = normalizeObserverQuery(req.query['observer']);
      const ck = `r|${packetHash}|${network}|${observer ?? ''}`;
      const hit = getResolveCache(ck);
      if (hit) {
        res.json(hit);
        return;
      }
      const resolved = await resolvePool.run<unknown>({ type: 'resolve', packetHash, network, observer });
      if (!resolved) {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      setResolveCache(ck, resolved);
      res.json(resolved);
    } catch (err) {
      console.error('[api] GET /path-beta/resolve', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/resolve-multi', pathBetaLimiter, async (req, res) => {
    try {
      const packetHash = String(req.query['hash'] ?? '').trim();
      if (!packetHash) {
        res.status(400).json({ error: 'Missing hash query parameter' });
        return;
      }
      if (!/^[0-9a-fA-F]{1,128}$/.test(packetHash)) {
        res.status(400).json({ error: 'Invalid hash format' });
        return;
      }
      const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
      const ck = `m|${packetHash}|${network}`;
      const hit = getResolveCache(ck);
      if (hit) {
        res.json(hit);
        return;
      }
      const resolved = await resolvePool.run<unknown>({ type: 'resolveMulti', packetHash, network });
      if (!resolved) {
        res.status(404).json({ error: 'Packet not found' });
        return;
      }
      setResolveCache(ck, resolved);
      res.json(resolved);
    } catch (err) {
      console.error('[api] GET /path-beta/resolve-multi', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-beta/history', pathHistoryLimiter, async (req, res) => {
    try {
      const requestedNetwork = resolveRequestNetwork(req.query['network'], req.headers);
      const scope = requestedNetwork === 'all' ? 'all' : (requestedNetwork ?? 'teesside');

      const historyCached = pathHistoryCache.get(scope);
      if (historyCached && Date.now() - historyCached.ts < pathHistoryCacheTtlMs) {
        res.json(historyCached.data);
        return;
      }

      const cached = await getPathHistoryCache(scope);
      let responseData: unknown;
      if (!cached) {
        responseData = {
          ok: true,
          scope,
          windowStart: null,
          updatedAt: null,
          packetCount: 0,
          resolvedPacketCount: 0,
          maxCount: 0,
          segments: [],
        };
      } else {
        const segments = Array.isArray(cached.segment_counts) ? cached.segment_counts : [];
        const maxCount = segments.reduce((max, segment) => Math.max(max, Number(segment.count ?? 0)), 0);
        responseData = {
          ok: true,
          scope,
          windowStart: cached.window_start,
          updatedAt: cached.updated_at,
          packetCount: cached.packet_count,
          resolvedPacketCount: cached.resolved_packet_count,
          maxCount,
          segments,
        };
      }

      pathHistoryCache.set(scope, { ts: Date.now(), data: responseData });
      res.json(responseData);
    } catch (err) {
      console.error('[api] GET /path-beta/history', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/path-learning', pathLearningLimiter, async (req, res) => {
    try {
      const network = resolveRequestNetwork(req.query['network'], req.headers, 'teesside') ?? 'teesside';
      const limit = Math.min(12000, Math.max(1000, Number(req.query['limit'] ?? 6000)));
      const [prefixRows, transitionRows, edgeRows, motifRows, calibrationRows] = await Promise.all([
        query<{
          prefix: string;
          receiver_region: string;
          prev_prefix: string | null;
          node_id: string;
          probability: number;
          count: number;
        }>(
          `SELECT prefix, receiver_region, prev_prefix, node_id, probability, count
           FROM path_prefix_priors
           WHERE network = $1
           ORDER BY count DESC
           LIMIT $2`,
          [network, limit],
        ),
        query<{
          from_node_id: string;
          to_node_id: string;
          receiver_region: string;
          probability: number;
          count: number;
        }>(
          `SELECT from_node_id, to_node_id, receiver_region, probability, count
           FROM path_transition_priors
           WHERE network = $1
           ORDER BY count DESC
           LIMIT $2`,
          [network, limit],
        ),
        query<{
          from_node_id: string;
          to_node_id: string;
          receiver_region: string;
          hour_bucket: number;
          observed_count: number;
          expected_count: number;
          missing_count: number;
          directional_support: number;
          recency_score: number;
          reliability: number;
          itm_path_loss_db: number | null;
          score: number;
          consistency_penalty: number;
        }>(
          `SELECT from_node_id, to_node_id, receiver_region, hour_bucket,
                  observed_count, expected_count, missing_count, directional_support,
                  recency_score, reliability, itm_path_loss_db, score, consistency_penalty
           FROM path_edge_priors
           WHERE network = $1
           ORDER BY score DESC, observed_count DESC
           LIMIT $2`,
          [network, limit],
        ),
        query<{
          receiver_region: string;
          hour_bucket: number;
          motif_len: number;
          node_ids: string;
          probability: number;
          count: number;
        }>(
          `SELECT receiver_region, hour_bucket, motif_len, node_ids, probability, count
           FROM path_motif_priors
           WHERE network = $1
           ORDER BY count DESC
           LIMIT $2`,
          [network, limit],
        ),
        query<{
          evaluated_packets: number;
          top1_accuracy: number;
          mean_pred_confidence: number;
          confidence_scale: number;
          confidence_bias: number;
          recommended_threshold: number;
        }>(
          `SELECT evaluated_packets, top1_accuracy, mean_pred_confidence, confidence_scale, confidence_bias, recommended_threshold
           FROM path_model_calibration
           WHERE network = $1`,
          [network],
        ),
      ]);

      const calibration = calibrationRows.rows[0] ?? {
        evaluated_packets: 0,
        top1_accuracy: 0,
        mean_pred_confidence: 0,
        confidence_scale: 1,
        confidence_bias: 0,
        recommended_threshold: 0.5,
      };

      res.json({
        network,
        calibration,
        prefixPriors: prefixRows.rows,
        transitionPriors: transitionRows.rows,
        edgePriors: edgeRows.rows,
        motifPriors: motifRows.rows,
      });
    } catch (err) {
      console.error('[api] GET /path-learning', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
