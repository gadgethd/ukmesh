import { Router, type Request, type Response } from 'express';
import { isIP } from 'node:net';
import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

type BackendSiteDeps = {
  query: QueryFn;
};

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

function localOnly(handler: (req: Request, res: Response) => void | Promise<void>) {
  return async (req: Request, res: Response) => {
    if (!requireBackendSiteLocalOnly(req, res)) return;
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
  res.setHeader(
    'Content-Security-Policy',
    "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; font-src 'self' data:",
  );
  res.type('html').send(BACKEND_SITE_HTML);
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
         WHERE COALESCE(n.network, g.network, s.network) IN ('teesside', 'ukmesh')
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

const BACKEND_SITE_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>UKMesh Backend</title>
  <style>
    :root {
      color-scheme: dark;
      --ink: #edf5f0;
      --muted: #9daaa4;
      --line: #2f3a35;
      --surface: #151b18;
      --surface-2: #101511;
      --wash: #0b0f0d;
      --green: #43c789;
      --red: #ee6b6b;
      --yellow: #d6ab38;
      --teal: #4bb9b4;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: radial-gradient(circle at 20% 0%, #16221c 0, #0b0f0d 34%, #0b0f0d 100%);
    }
    .shell { min-height: 100vh; display: grid; grid-template-columns: 230px 1fr; }
    .side {
      background: #101511;
      border-right: 1px solid var(--line);
      padding: 22px 16px;
      position: sticky;
      top: 0;
      height: 100vh;
    }
    .brand { font-size: 18px; font-weight: 800; margin-bottom: 6px; }
    .sub { color: var(--muted); font-size: 13px; margin-bottom: 24px; }
    .nav { display: grid; gap: 8px; }
    .nav button {
      border: 1px solid var(--line);
      background: #121815;
      color: var(--ink);
      text-align: left;
      border-radius: 8px;
      padding: 10px 12px;
      font: inherit;
      cursor: pointer;
    }
    .nav button.active { border-color: var(--green); background: #193829; }
    .main { padding: 22px; }
    .top {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 18px;
    }
    h1 { margin: 0; font-size: 26px; line-height: 1.15; }
    h2 { margin: 0 0 12px; font-size: 17px; }
    .status { color: var(--muted); font-size: 13px; text-align: right; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    .metrics-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    .chart-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    .panel {
      background: linear-gradient(180deg, var(--surface), var(--surface-2));
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      min-width: 0;
    }
    .metric-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0; }
    .metric-value { font-size: 25px; font-weight: 800; margin-top: 6px; }
    .metric-note { color: var(--muted); font-size: 12px; margin-top: 4px; }
    .wide { grid-column: span 2; }
    .full { grid-column: 1 / -1; }
    .bars { display: grid; gap: 9px; }
    .bar-row { display: grid; grid-template-columns: 88px 1fr 72px; gap: 10px; align-items: center; font-size: 13px; }
    .bar-track { height: 12px; background: #222b26; border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
    .bar-fill { height: 100%; background: var(--green); border-radius: 8px; transition: width 180ms ease; }
    .bar-fill.warn { background: var(--yellow); }
    .bar-fill.bad { background: var(--red); }
    .line-chart { width: 100%; height: 260px; display: block; }
    .line-chart text { fill: var(--muted); font-size: 11px; }
    .line-chart .axis { stroke: var(--line); stroke-width: 1; }
    .line-chart .grid-line { stroke: #24302a; stroke-width: 1; }
    .line-chart .gold-line { fill: none; stroke: var(--green); stroke-width: 3; }
    .line-chart .held-line { fill: none; stroke: var(--yellow); stroke-width: 2; }
    .line-chart .champ-line { fill: none; stroke: var(--teal); stroke-width: 3; stroke-dasharray: 6 5; }
    .legend { display: flex; flex-wrap: wrap; gap: 12px; color: var(--muted); font-size: 12px; margin-bottom: 8px; }
    .legend span::before { content: ''; display: inline-block; width: 18px; height: 3px; margin-right: 6px; vertical-align: middle; background: var(--green); }
    .legend .held::before { background: var(--yellow); }
    .legend .champ::before { background: var(--teal); }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid var(--line); padding: 8px 6px; text-align: left; }
    tr:hover td { background: #121a16; }
    th { color: var(--muted); font-size: 12px; font-weight: 700; }
    .pill { display: inline-flex; border-radius: 8px; padding: 3px 7px; background: #193829; color: #7be0aa; font-size: 12px; font-weight: 700; }
    .pill.bad { background: #3a1b1b; color: #ff9b9b; }
    .row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    .empty { color: var(--muted); padding: 18px 0; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
    @media (max-width: 1000px) {
      .shell { grid-template-columns: 1fr; }
      .side { position: static; height: auto; }
      .grid, .metrics-grid, .chart-grid, .row { grid-template-columns: 1fr; }
      .wide { grid-column: span 1; }
      .status { text-align: left; }
      .top { flex-direction: column; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="side">
      <div class="brand">UKMesh Backend</div>
      <div class="sub">Local operator tools</div>
      <nav class="nav">
        <button data-page="ml">ML Path Learner</button>
        <button data-page="overview">Overview</button>
      </nav>
    </aside>
    <main class="main">
      <div class="top">
        <div>
          <h1 id="title">Overview</h1>
          <div class="sub" id="subtitle">Backend tools home</div>
        </div>
        <div class="status">
          <div id="refreshState">Loading</div>
          <div id="runState"></div>
        </div>
      </div>
      <section id="app"></section>
    </main>
  </div>
  <script>
    const app = document.getElementById('app');
    const refreshState = document.getElementById('refreshState');
    const runState = document.getElementById('runState');
    const title = document.getElementById('title');
    const subtitle = document.getElementById('subtitle');
    const navButtons = [...document.querySelectorAll('.nav button')];
    let state = null;
    let currentPage = location.pathname.endsWith('/ml-path-learner') ? 'ml' : 'overview';

    if (currentPage === 'ml') {
      title.textContent = 'ML Path Learner';
      subtitle.textContent = 'Full packet path scoring from multibyte gold paths';
    }

    function num(value) {
      const n = Number(value);
      return Number.isFinite(n) ? n : 0;
    }
    function pct(value) {
      return (num(value) * 100).toFixed(1) + '%';
    }
    function int(value) {
      return Math.round(num(value)).toLocaleString();
    }
    function esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
      }[ch]));
    }
    function bar(value) {
      const v = Math.max(0, Math.min(1, num(value)));
      const cls = v >= 0.8 ? '' : v >= 0.65 ? ' warn' : ' bad';
      return '<div class="bar-track"><div class="bar-fill' + cls + '" style="width:' + (v * 100).toFixed(2) + '%"></div></div>';
    }
    function metric(label, value, note) {
      return '<div class="panel"><div class="metric-label">' + esc(label) + '</div><div class="metric-value">' + value + '</div><div class="metric-note">' + esc(note || '') + '</div></div>';
    }
    function renderBars(rows, field, label) {
      if (!rows.length) return '<div class="empty">No variants yet</div>';
      return '<div class="bars">' + rows.map((row) => {
        const value = num(row[field]);
        return '<div class="bar-row"><div>v' + esc(row.variant_rank) + '</div>' + bar(value) + '<strong>' + pct(value) + '</strong></div>';
      }).join('') + '</div>';
    }
    function pointsFor(rows, field, width, height, pad) {
      if (!rows.length) return '';
      const minGen = Math.min(...rows.map((r) => num(r.generation)));
      const maxGen = Math.max(...rows.map((r) => num(r.generation)));
      const span = Math.max(1, maxGen - minGen);
      return rows.map((row) => {
        const x = pad + ((num(row.generation) - minGen) / span) * (width - pad * 2);
        const y = height - pad - (Math.max(0, Math.min(1, num(row[field]))) * (height - pad * 2));
        return x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' ');
    }
    function renderAccuracyHistory(rows, champions) {
      if (!rows.length) return '<div class="empty">No generation history yet</div>';
      const width = 900;
      const height = 260;
      const pad = 34;
      const goldPoints = pointsFor(rows, 'best_gold_replay_full_path_accuracy', width, height, pad);
      const heldPoints = pointsFor(rows, 'best_heldout_full_path_accuracy', width, height, pad);
      const champRows = (champions || []).map((champ) => ({
        generation: champ.generation,
        champion_full_path_accuracy: champ.champion_full_path_accuracy,
      }));
      const champPoints = pointsFor(champRows, 'champion_full_path_accuracy', width, height, pad);
      const minGen = Math.min(...rows.map((r) => num(r.generation)));
      const maxGen = Math.max(...rows.map((r) => num(r.generation)));
      const y80 = height - pad - (0.8 * (height - pad * 2));
      const y60 = height - pad - (0.6 * (height - pad * 2));
      return '<div class="legend"><span>Best gold replay</span><span class="held">Best held-back</span><span class="champ">Promoted champion</span></div>' +
        '<svg class="line-chart" viewBox="0 0 ' + width + ' ' + height + '" role="img" aria-label="Accuracy by generation">' +
          '<line class="grid-line" x1="' + pad + '" y1="' + y80.toFixed(1) + '" x2="' + (width - pad) + '" y2="' + y80.toFixed(1) + '"></line>' +
          '<line class="grid-line" x1="' + pad + '" y1="' + y60.toFixed(1) + '" x2="' + (width - pad) + '" y2="' + y60.toFixed(1) + '"></line>' +
          '<line class="axis" x1="' + pad + '" y1="' + pad + '" x2="' + pad + '" y2="' + (height - pad) + '"></line>' +
          '<line class="axis" x1="' + pad + '" y1="' + (height - pad) + '" x2="' + (width - pad) + '" y2="' + (height - pad) + '"></line>' +
          '<text x="4" y="' + (y80 + 4).toFixed(1) + '">80%</text><text x="4" y="' + (y60 + 4).toFixed(1) + '">60%</text>' +
          '<text x="' + pad + '" y="' + (height - 8) + '">gen ' + esc(minGen) + '</text>' +
          '<text x="' + (width - pad - 52) + '" y="' + (height - 8) + '">gen ' + esc(maxGen) + '</text>' +
          '<polyline class="gold-line" points="' + goldPoints + '"></polyline>' +
          '<polyline class="held-line" points="' + heldPoints + '"></polyline>' +
          (champPoints ? '<polyline class="champ-line" points="' + champPoints + '"></polyline>' : '') +
        '</svg>';
    }
    function renderMl() {
      title.textContent = 'ML Path Learner';
      subtitle.textContent = 'Full packet path scoring from multibyte gold paths';
      const active = state.activeModel || {};
      const championVariant = state.activeChampionVariant || {};
      const latest = state.latestRun || {};
      const variants = state.variantRuns || [];
      const scores = state.scoreSummary || [];
      const gold = state.goldSummary || [];
      const coverage = state.prefixCoverage || [];
      const worst = state.worstPackets || [];
      const accuracyHistory = state.accuracyHistory || [];
      const championHistory = state.championHistory || [];
      const latestBest = variants.reduce((best, row) => (
        num(row.complete_path_accuracy) > num(best?.complete_path_accuracy) ? row : best
      ), null);

      app.innerHTML =
        '<div class="metrics-grid">' +
          metric('Latest generation', esc(latest.generation ?? active.generation ?? 'none'), int(latest.variants_completed || variants.length) + '/' + int(latest.population_size || active.population_size || variants.length) + ' variants') +
          metric('Latest best replay', pct(latest.best_gold_replay_full_path_accuracy), latestBest ? 'gen ' + esc(latestBest.generation) + ' v' + esc(latestBest.variant_rank) : 'no variants') +
          metric('Active champion', 'gen ' + esc(active.generation ?? '-') + ' v' + esc(active.variant_rank ?? '-'), esc(active.version || 'none')) +
          metric('Champion replay', pct(active.complete_path_accuracy), int(active.evaluated_packets) + ' packets') +
          metric('Hop accuracy', pct(active.hop_accuracy), int(active.evaluated_hops) + ' hops') +
          metric('Resolver scores', int(scores.reduce((a, r) => a + num(r.scores), 0)), int(scores.reduce((a, r) => a + num(r.usable_scores), 0)) + ' at 0.80+') +
        '</div>' +
        '<div class="chart-grid">' +
          '<div class="panel wide"><h2>Gold Replay Full-Path Accuracy</h2>' + renderBars(variants, 'complete_path_accuracy') + '</div>' +
          '<div class="panel wide"><h2>Held-Back Full-Path Accuracy</h2>' + renderBars(variants, 'val_complete_path_accuracy') + '</div>' +
        '</div>' +
        '<div class="panel full"><h2>Accuracy History</h2>' + renderAccuracyHistory(accuracyHistory, championHistory) + '</div>' +
        '<div class="row">' +
          '<div class="panel"><h2>Score Rows</h2>' + renderScoreTable(scores) + '</div>' +
          '<div class="panel"><h2>Gold Corpus</h2>' + renderGoldTable(gold) + '</div>' +
        '</div>' +
        '<div class="row">' +
          '<div class="panel"><h2>Prefix Coverage</h2>' + renderCoverageTable(coverage) + '</div>' +
          '<div class="panel"><h2>Active Champion Details</h2>' + renderPromoted(championVariant, active) + '</div>' +
        '</div>' +
        '<div class="panel full"><h2>Hard Packets</h2>' + renderWorstPackets(worst) + '</div>';
    }
    function renderScoreTable(rows) {
      if (!rows.length) return '<div class="empty">No scores yet</div>';
      return '<table><thead><tr><th>Network</th><th>Rows</th><th>0.80+</th><th>Max</th><th>Observations</th><th>Correct</th></tr></thead><tbody>' +
        rows.map((r) => '<tr><td>' + esc(r.network) + '</td><td>' + int(r.scores) + '</td><td>' + int(r.usable_scores) + '</td><td>' + pct(r.max_score) + '</td><td>' + int(r.observations) + '</td><td>' + int(r.correct) + '</td></tr>').join('') +
      '</tbody></table>';
    }
    function renderGoldTable(rows) {
      if (!rows.length) return '<div class="empty">No gold rows yet</div>';
      return '<table><thead><tr><th>Network</th><th>Packets</th><th>Hops</th><th>Nodes</th><th>Prefixes</th></tr></thead><tbody>' +
        rows.map((r) => '<tr><td>' + esc(r.network) + '</td><td>' + int(r.packets) + '</td><td>' + int(r.gold_hops) + '</td><td>' + int(r.unique_nodes) + '</td><td>' + int(r.prefixes) + '</td></tr>').join('') +
      '</tbody></table>';
    }
    function renderCoverageTable(rows) {
      if (!rows.length) return '<div class="empty">No coverage data yet</div>';
      return '<table><thead><tr><th>Network</th><th>Nodes</th><th>Node Prefixes</th><th>Gold Prefixes</th><th>Scored</th></tr></thead><tbody>' +
        rows.map((r) => '<tr><td>' + esc(r.network) + '</td><td>' + int(r.positioned_nodes) + '</td><td>' + int(r.node_prefixes) + '</td><td>' + int(r.gold_prefixes) + '</td><td>' + int(r.scored_prefixes) + '</td></tr>').join('') +
      '</tbody></table>';
    }
    function renderPromoted(row, active) {
      const merged = Object.assign({}, active || {}, row || {});
      return '<table><tbody>' +
        '<tr><th>Generation</th><td>' + esc(merged.generation ?? '-') + '</td></tr>' +
        '<tr><th>Variant</th><td>v' + esc(merged.variant_rank ?? '-') + '</td></tr>' +
        '<tr><th>Gold replay full paths</th><td>' + pct(merged.complete_path_accuracy) + '</td></tr>' +
        '<tr><th>Held-back full paths</th><td>' + (row?.val_complete_path_accuracy == null ? 'n/a' : pct(row.val_complete_path_accuracy)) + '</td></tr>' +
        '<tr><th>Gold replay hop accuracy</th><td>' + pct(merged.hop_accuracy) + '</td></tr>' +
        '<tr><th>Mean completion</th><td>' + pct(merged.mean_path_completion) + '</td></tr>' +
      '</tbody></table>';
    }
    function renderWorstPackets(rows) {
      if (!rows.length) return '<div class="empty">No packet audit rows yet</div>';
      return '<table><thead><tr><th>Network</th><th>Packet</th><th>Hops</th><th>Correct</th><th>Completion</th><th>Path</th></tr></thead><tbody>' +
        rows.map((r) => '<tr><td>' + esc(r.network) + '</td><td><code>' + esc(r.packet_hash) + '</code></td><td>' + int(r.expected_hops) + '</td><td>' + int(r.correct_hops) + '</td><td>' + pct(r.path_completion) + '</td><td>' + (r.complete_path ? '<span class="pill">complete</span>' : '<span class="pill bad">miss</span>') + '</td></tr>').join('') +
      '</tbody></table>';
    }
    function renderOverview() {
      title.textContent = 'Overview';
      subtitle.textContent = 'Backend tools home';
      const variants = state?.variantRuns || [];
      app.innerHTML =
        '<div class="grid">' +
          metric('Current tool', 'ML', 'Path learner audit is active') +
          metric('Variants this run', int(variants.length), 'Updates as each variant finishes') +
          metric('Training run', esc(state?.trainingRunId || 'none'), 'Latest run id') +
          metric('Updated', esc(new Date(state?.generatedAt || Date.now()).toLocaleTimeString()), 'Local browser time') +
        '</div>' +
        '<div class="panel full"><h2>Next slots</h2><div class="empty">Add backend tools here as needed.</div></div>';
    }
    function render() {
      if (!state) {
        app.innerHTML = '<div class="panel full"><div class="empty">Loading backend state</div></div>';
        return;
      }
      runState.textContent = state.trainingRunId ? 'run ' + state.trainingRunId : 'no run yet';
      if (currentPage === 'overview') renderOverview();
      else renderMl();
    }
    async function loadState() {
      try {
        const response = await fetch('/local-api/ml-path-learner', { cache: 'no-store' });
        if (!response.ok) throw new Error('HTTP ' + response.status);
        state = await response.json();
        refreshState.textContent = 'Updated ' + new Date().toLocaleTimeString();
        render();
      } catch (err) {
        refreshState.textContent = 'Update failed: ' + err.message;
      }
    }
    navButtons.forEach((button) => {
      button.addEventListener('click', () => {
        currentPage = button.dataset.page;
        navButtons.forEach((b) => b.classList.toggle('active', b === button));
        history.replaceState(null, '', currentPage === 'overview' ? '/' : '/ml-path-learner');
        render();
      });
    });
    navButtons.forEach((button) => button.classList.toggle('active', button.dataset.page === currentPage));
    loadState();
    setInterval(loadState, 2000);
  </script>
</body>
</html>`;
