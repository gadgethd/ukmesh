import { Router, type Request, type Response } from 'express';
import { isIP } from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
import type { QueryResultRow } from 'pg';
import {
  operatorTokenIsConfigured,
  verifyOperatorToken,
} from '../security/operatorAuth.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type BackendSiteDeps = {
  query: QueryFn;
};

function loadBackendSiteTemplate(): string {
  const directory = path.dirname(fileURLToPath(import.meta.url));
  const candidates = [
    path.join(directory, 'template.html'),
    path.join(directory, '..', '..', 'src', 'backend-site', 'template.html'),
  ];
  const templatePath = candidates.find((candidate) => fs.existsSync(candidate));
  if (!templatePath) throw new Error('Backend site template is unavailable');
  return fs.readFileSync(templatePath, 'utf8');
}

const BACKEND_SITE_TEMPLATE = loadBackendSiteTemplate();

function firstHeaderIp(value: unknown): string {
  const raw = Array.isArray(value) ? value[0] : value;
  return String(raw ?? '').split(',')[0]?.trim() ?? '';
}

function normalizeIp(value: string | undefined): string {
  const raw = String(value ?? '').trim();
  if (!raw) return '';
  if (raw.startsWith('::ffff:')) return raw.slice(7);
  return raw;
}

function isPrivateOrLoopback(ip: string): boolean {
  const normalized = normalizeIp(ip);
  if (!normalized) return false;
  if (normalized === 'localhost' || normalized === '::1' || normalized === '127.0.0.1') return true;
  if (normalized.startsWith('10.')) return true;
  if (normalized.startsWith('192.168.')) return true;
  if (/^172\.(1[6-9]|2\d|3[0-1])\./.test(normalized)) return true;
  if (/^(fc|fd)/i.test(normalized)) return true;
  if (/^fe80:/i.test(normalized)) return true;
  return false;
}

function requireBackendSiteLocalOnly(req: Request, res: Response): boolean {
  const forwarded = [
    firstHeaderIp(req.headers['cf-connecting-ip']),
    firstHeaderIp(req.headers['x-forwarded-for']),
    firstHeaderIp(req.headers['x-real-ip']),
  ].filter(Boolean);

  // Public proxy traffic carries forwarded client IPs.  Do not allow a private
  // cloudflared/docker socket address to satisfy the local-only check.
  if (forwarded.some((ip) => !isPrivateOrLoopback(ip))) {
    res.status(403).type('text/plain').send('Local access only');
    return false;
  }

  const candidates = [
    normalizeIp(req.ip),
    normalizeIp(req.socket.remoteAddress ?? ''),
    ...forwarded.map(normalizeIp),
  ].filter(Boolean);

  if (candidates.some((ip) => isPrivateOrLoopback(ip) || (isIP(ip) === 0 && ip === 'localhost'))) {
    return true;
  }

  res.status(403).type('text/plain').send('Local access only');
  return false;
}

function requireOperatorAuth(req: Request, res: Response): boolean {
  const expected = process.env['OPERATOR_SITE_TOKEN'];
  if (!operatorTokenIsConfigured(expected)) {
    res.status(503).type('text/plain').send('Operator site is not configured');
    return false;
  }
  const authorization = String(req.headers.authorization ?? '');
  const bearer = authorization.startsWith('Bearer ') ? authorization.slice(7).trim() : '';
  const provided = String(req.headers['x-operator-token'] ?? bearer);
  if (!verifyOperatorToken(expected, provided)) {
    res.setHeader('WWW-Authenticate', 'Bearer realm="meshcore-operator"');
    res.status(401).type('text/plain').send('Operator authentication required');
    return false;
  }
  return true;
}

function localOnly(handler: (req: Request, res: Response) => void | Promise<void>) {
  return async (req: Request, res: Response) => {
    if (!requireBackendSiteLocalOnly(req, res)) return;
    if (!requireOperatorAuth(req, res)) return;
    try {
      await handler(req, res);
    } catch (err) {
      console.error('[backend-site] request failed:', err);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Backend dashboard query failed' });
      }
    }
  };
}

function sendBackendSiteHtml(_req: Request, res: Response): void {
  const nonce = randomBytes(18).toString('base64url');
  res.setHeader(
    'Content-Security-Policy',
    `default-src 'self'; script-src 'self' 'nonce-${nonce}'; style-src 'self' 'nonce-${nonce}'; img-src 'self' data:; connect-src 'self'; font-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'`,
  );
  res.type('html').send(BACKEND_SITE_TEMPLATE.replaceAll('{{CSP_NONCE}}', nonce));
}

export function createBackendSiteRoutes(deps: BackendSiteDeps): Router {
  const { query } = deps;
  const router = Router();

  router.get('/', localOnly(sendBackendSiteHtml));
  router.get('/ml-path-learner', localOnly(sendBackendSiteHtml));
  router.get('/backend', localOnly((_req, res) => res.redirect(302, '/')));
  router.get('/backend/ml-path-learner', localOnly((_req, res) => res.redirect(302, '/ml-path-learner')));

  router.get('/local-api/ml-path-learner', localOnly(async (_req, res) => {
    const latestRun = await query<{
      training_run_id: string;
      generation: number;
      population_size: number;
      variants_completed: number;
      best_gold_replay_full_path_accuracy: number;
      best_heldout_full_path_accuracy: number;
      updated_at: string;
    }>(
      `SELECT training_run_id,
              generation,
              MAX(population_size)::int AS population_size,
              COUNT(*)::int AS variants_completed,
              MAX(complete_path_accuracy)::float AS best_gold_replay_full_path_accuracy,
              MAX(val_complete_path_accuracy)::float AS best_heldout_full_path_accuracy,
              MAX(created_at) AS updated_at
         FROM ml_model_variant_runs
        GROUP BY training_run_id, generation
        ORDER BY updated_at DESC
        LIMIT 1`,
    );
    const latestRunSummary = latestRun.rows[0] ?? null;
    const trainingRunId = latestRunSummary?.training_run_id ?? null;

    const activeModel = await query(
      `SELECT version, network, generation, variant_rank, is_active,
              population_size,
              evaluated_packets, evaluated_hops,
              top1_accuracy AS hop_accuracy,
              top3_accuracy AS hop_top3_accuracy,
              complete_path_accuracy, mean_path_completion,
              promoted_at
         FROM ml_model_versions
        WHERE is_active = TRUE
        ORDER BY promoted_at DESC
        LIMIT 1`,
    );
    const active = activeModel.rows[0] ?? null;

    const activeChampionVariant = active
      ? await query(
          `SELECT training_run_id, model_network, generation, variant_rank,
                  population_size, evaluated_packets, evaluated_hops,
                  hop_accuracy, hop_top3_accuracy,
                  complete_path_accuracy, mean_path_completion,
                  val_evaluated_packets, val_evaluated_hops,
                  val_hop_accuracy, val_hop_top3_accuracy,
                  val_complete_path_accuracy, val_mean_path_completion,
                  hyperparams, created_at
             FROM ml_model_variant_runs
            WHERE generation = $1
              AND variant_rank = $2
              AND model_network = $3
            ORDER BY created_at DESC
            LIMIT 1`,
          [active.generation, active.variant_rank, active.network],
        )
      : { rows: [] };

    const variantRuns = trainingRunId
      ? await query(
          `SELECT training_run_id, model_network, generation, variant_rank,
                  population_size, evaluated_packets, evaluated_hops,
                  hop_accuracy, hop_top3_accuracy,
                  complete_path_accuracy, mean_path_completion,
                  val_evaluated_packets, val_evaluated_hops,
                  val_hop_accuracy, val_hop_top3_accuracy,
                  val_complete_path_accuracy, val_mean_path_completion,
                  hyperparams, created_at
             FROM ml_model_variant_runs
            WHERE training_run_id = $1
            ORDER BY variant_rank ASC`,
          [trainingRunId],
        )
      : { rows: [] };

    const packetResultsSummary = trainingRunId
      ? await query(
          `SELECT variant_rank,
                  packet_network AS network,
                  COUNT(*)::int AS packets,
                  SUM(CASE WHEN complete_path THEN 1 ELSE 0 END)::int AS complete_paths,
                  SUM(correct_hops)::int AS correct_hops,
                  SUM(expected_hops)::int AS expected_hops,
                  AVG(path_completion)::float AS mean_path_completion
             FROM ml_model_variant_packet_results
            WHERE training_run_id = $1
            GROUP BY variant_rank, packet_network
            ORDER BY variant_rank, packet_network`,
          [trainingRunId],
        )
      : { rows: [] };

    const scoreSummary = await query(
      `SELECT network,
              COUNT(*)::int AS scores,
              COUNT(*) FILTER (WHERE score >= 0.80)::int AS usable_scores,
              MIN(score)::float AS min_score,
              MAX(score)::float AS max_score,
              AVG(score)::float AS avg_score,
              SUM(observation_count)::int AS observations,
              SUM(correct_count)::int AS correct
         FROM ml_path_prefix_scores
        GROUP BY network
        ORDER BY network`,
    );

    const goldSummary = await query(
      `SELECT network,
              COUNT(*)::int AS gold_hops,
              COUNT(DISTINCT packet_hash)::int AS packets,
              COUNT(DISTINCT true_node_id)::int AS unique_nodes,
              COUNT(DISTINCT hash_2char)::int AS prefixes
         FROM ml_gold_paths
        GROUP BY network
        ORDER BY network`,
    );

    const prefixCoverage = await query(
      `WITH gold_prefixes AS (
          SELECT network, COUNT(DISTINCT hash_2char)::int AS gold_prefixes
            FROM ml_gold_paths
           GROUP BY network
        ),
        node_prefixes AS (
          SELECT network,
                 COUNT(DISTINCT upper(left(node_id, 2)))::int AS node_prefixes,
                 COUNT(*) FILTER (WHERE lat IS NOT NULL AND lon IS NOT NULL)::int AS positioned_nodes
            FROM nodes
           GROUP BY network
        ),
        score_prefixes AS (
          SELECT network,
                 COUNT(DISTINCT hash_2char)::int AS scored_prefixes,
                 COUNT(*)::int AS score_rows
            FROM ml_path_prefix_scores
           GROUP BY network
        )
        SELECT COALESCE(n.network, g.network, s.network) AS network,
               COALESCE(n.positioned_nodes, 0)::int AS positioned_nodes,
               COALESCE(n.node_prefixes, 0)::int AS node_prefixes,
               COALESCE(g.gold_prefixes, 0)::int AS gold_prefixes,
               COALESCE(s.scored_prefixes, 0)::int AS scored_prefixes,
               COALESCE(s.score_rows, 0)::int AS score_rows
          FROM node_prefixes n
          FULL JOIN gold_prefixes g USING (network)
          FULL JOIN score_prefixes s USING (network)
         WHERE COALESCE(n.network, g.network, s.network) = 'ukmesh'
         ORDER BY network`,
    );

    const worstPackets = await query(
      `WITH active AS (
          SELECT generation, variant_rank
            FROM ml_model_versions
           WHERE is_active = TRUE
           ORDER BY promoted_at DESC
           LIMIT 1
        ),
        active_run AS (
          SELECT r.training_run_id, r.variant_rank
            FROM ml_model_variant_runs r
            JOIN active a ON a.generation = r.generation AND a.variant_rank = r.variant_rank
           ORDER BY r.created_at DESC
           LIMIT 1
        )
        SELECT p.packet_network AS network,
               p.packet_hash,
               p.expected_hops,
               p.predicted_hops,
               p.correct_hops,
               p.complete_path,
               p.path_completion
          FROM ml_model_variant_packet_results p
          JOIN active_run r
            ON r.training_run_id = p.training_run_id
           AND r.variant_rank = p.variant_rank
         ORDER BY p.complete_path ASC, p.path_completion ASC, p.expected_hops DESC
         LIMIT 24`,
    );

    const accuracyHistory = await query(
      `SELECT generation,
              COUNT(*)::int AS variants,
              MAX(complete_path_accuracy)::float AS best_gold_replay_full_path_accuracy,
              MAX(val_complete_path_accuracy)::float AS best_heldout_full_path_accuracy,
              MAX(created_at) AS updated_at
         FROM ml_model_variant_runs
        GROUP BY generation
        ORDER BY generation ASC`,
    );

    const championHistory = await query(
      `SELECT generation,
              variant_rank,
              complete_path_accuracy::float AS champion_full_path_accuracy,
              top1_accuracy::float AS champion_hop_accuracy,
              promoted_at
         FROM ml_model_versions
        ORDER BY promoted_at ASC`,
    );

    res.json({
      generatedAt: new Date().toISOString(),
      trainingRunId,
      latestRun: latestRunSummary,
      activeModel: active,
      activeChampionVariant: activeChampionVariant.rows[0] ?? null,
      variantRuns: variantRuns.rows,
      packetResultsSummary: packetResultsSummary.rows,
      scoreSummary: scoreSummary.rows,
      goldSummary: goldSummary.rows,
      prefixCoverage: prefixCoverage.rows,
      worstPackets: worstPackets.rows,
      accuracyHistory: accuracyHistory.rows,
      championHistory: championHistory.rows,
    });
  }));

  return router;
}
