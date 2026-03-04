import { Router } from 'express';
import { getNodes, getNodeHistory, getRecentPackets, query, MIN_LINK_OBSERVATIONS } from '../db/index.js';

const router = Router();

type NetworkFilters = {
  params: string[];
  packets: string;
  nodes: string;
  nodesAlias: (alias: string) => string;
};

function networkFilters(network?: string): NetworkFilters {
  return {
    params: network ? [network] : [],
    packets: network ? 'AND network = $1' : '',
    nodes: network ? 'AND network = $1' : '',
    nodesAlias: (alias: string) => (network ? `AND ${alias}.network = $1` : ''),
  };
}

// GET /api/nodes — all known nodes
router.get('/nodes', async (_req, res) => {
  try {
    const nodes = await getNodes();
    res.json(nodes);
  } catch (err) {
    console.error('[api] GET /nodes', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/nodes/:id/links — ITM-viable neighbours for a node
router.get('/nodes/:id/links', async (req, res) => {
  try {
    const id = req.params['id']!;
    const result = await query<{
      peer_id: string; peer_name: string | null; observed_count: number;
      itm_path_loss_db: number | null;
      count_this_to_peer: number; count_peer_to_this: number;
    }>(
      `SELECT
         CASE WHEN node_a_id = $1 THEN node_b_id ELSE node_a_id END AS peer_id,
         n.name AS peer_name,
         observed_count,
         itm_path_loss_db,
         CASE WHEN node_a_id = $1 THEN count_a_to_b ELSE count_b_to_a END AS count_this_to_peer,
         CASE WHEN node_a_id = $1 THEN count_b_to_a ELSE count_a_to_b END AS count_peer_to_this
       FROM node_links
       LEFT JOIN nodes n ON n.node_id = CASE WHEN node_a_id = $1 THEN node_b_id ELSE node_a_id END
       WHERE (node_a_id = $1 OR node_b_id = $1) AND (itm_viable = true OR force_viable = true) AND observed_count >= $2
       ORDER BY observed_count DESC`,
      [id, MIN_LINK_OBSERVATIONS],
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /nodes/:id/links', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/nodes/:id/history?hours=24
router.get('/nodes/:id/history', async (req, res) => {
  try {
    const hours = Math.min(Number(req.query['hours'] ?? 24), 672); // max 28 days
    const history = await getNodeHistory(req.params['id']!, hours);
    res.json(history);
  } catch (err) {
    console.error('[api] GET /nodes/:id/history', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/packets/recent?limit=200
router.get('/packets/recent', async (req, res) => {
  try {
    const limit = Math.min(Number(req.query['limit'] ?? 200), 1000);
    const packets = await getRecentPackets(limit);
    res.json(packets);
  } catch (err) {
    console.error('[api] GET /packets/recent', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/stats
router.get('/stats', async (req, res) => {
  try {
    const network = req.query['network'] as string | undefined;
    const filters = networkFilters(network);
    const [mqttCount, packetCount, staleCount, totalNodeCount, longestHopCount] = await Promise.all([
      query(`SELECT COUNT(DISTINCT rx_node_id) AS count FROM packets WHERE time > NOW() - INTERVAL '10 minutes' AND rx_node_id IS NOT NULL ${filters.packets}`, filters.params),
      query(`SELECT COUNT(DISTINCT packet_hash) AS count FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE lat IS NOT NULL AND lon IS NOT NULL
               AND (role IS NULL OR role = 2)
               AND (name IS NULL OR name NOT LIKE '%🚫%')
               AND last_seen <= NOW() - INTERVAL '7 days'
               AND last_seen >  NOW() - INTERVAL '14 days'
               ${filters.nodes}`, filters.params),
      query(`SELECT COUNT(*) AS count FROM nodes
             WHERE (name IS NULL OR name NOT LIKE '%🚫%')
               AND (role IS NULL OR role != 4)
               ${filters.nodes}`, filters.params),
      query(`SELECT hop_count AS count,
               COALESCE(
                 CASE WHEN payload->>'hash' ~ '^[0-9A-Fa-f]{16}$' THEN payload->>'hash' END,
                 CASE WHEN packet_hash   ~ '^[0-9A-Fa-f]{16}$' THEN packet_hash END
               ) AS hash
             FROM packets
             WHERE hop_count IS NOT NULL
               AND (payload->>'hash' ~ '^[0-9A-Fa-f]{16}$'
                    OR packet_hash   ~ '^[0-9A-Fa-f]{16}$')
               ${filters.packets}
             ORDER BY hop_count DESC LIMIT 1`, filters.params),
    ]);
    res.json({
      mqttNodes:      Number(mqttCount.rows[0]?.count ?? 0),
      staleNodes:     Number(staleCount.rows[0]?.count ?? 0),
      packetsDay:     Number(packetCount.rows[0]?.count ?? 0),
      totalNodes:     Number(totalNodeCount.rows[0]?.count ?? 0),
      longestHop:     Number(longestHopCount.rows[0]?.count ?? 0),
      longestHopHash: (longestHopCount.rows[0]?.hash as string | undefined) ?? null,
    });
  } catch (err) {
    console.error('[api] GET /stats', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});


// GET /api/coverage — all stored viewshed polygons (excludes hidden 🚫 nodes)
router.get('/coverage', async (req, res) => {
  try {
    const network = req.query['network'] as string | undefined;
    const filters = networkFilters(network);
    const result = await query(
      `SELECT nc.node_id, nc.geom, nc.antenna_height_m, nc.radius_m, nc.calculated_at
       FROM node_coverage nc
       JOIN nodes n ON n.node_id = nc.node_id
       WHERE (n.name IS NULL OR n.name NOT LIKE '%🚫%')
         AND (n.role IS NULL OR n.role = 2)
         ${filters.nodesAlias('n')}`,
      filters.params
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /coverage', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/planned-nodes
router.get('/planned-nodes', async (_req, res) => {
  try {
    const result = await query(
      'SELECT id, owner_pubkey, name, lat, lon, height_m, notes, created_at FROM planned_nodes ORDER BY created_at DESC'
    );
    res.json(result.rows);
  } catch (err) {
    console.error('[api] GET /planned-nodes', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/path-learning
router.get('/path-learning', async (req, res) => {
  try {
    const network = (req.query['network'] as string | undefined) ?? 'teesside';
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
         LIMIT 8000`,
        [network],
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
         LIMIT 8000`,
        [network],
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
         LIMIT 12000`,
        [network],
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
         LIMIT 12000`,
        [network],
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

// GET /api/stats/charts
router.get('/stats/charts', async (req, res) => {
  try {
    const network = req.query['network'] as string | undefined;
    const filters = networkFilters(network);

    const PAYLOAD_LABELS: Record<number, string> = {
      0: 'Request', 1: 'Response', 2: 'DM', 3: 'Ack',
      4: 'Advert', 5: 'GroupText', 6: 'GroupData',
      7: 'AnonReq', 8: 'Path', 9: 'Trace',
    };

    const [
      phResult, pdResult, rhResult, rdResult,
      ptResult, rpResult, hdResult, tcResult, sumResult,
    ] = await Promise.all([
      // packets per hour — last 24h (deduplicated by hash)
      query(`
        SELECT time_bucket('1 hour', time) AS hour, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}
        GROUP BY hour ORDER BY hour
      `, filters.params),
      // packets per day — last 7d (deduplicated by hash)
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      // unique radios heard per hour — last 24h (distinct transmitting nodes)
      query(`
        SELECT time_bucket('1 hour', time) AS hour, COUNT(DISTINCT src_node_id) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' AND src_node_id IS NOT NULL ${filters.packets}
        GROUP BY hour ORDER BY hour
      `, filters.params),
      // unique radios heard per day — last 7d
      query(`
        SELECT time_bucket('1 day', time) AS day, COUNT(DISTINCT src_node_id) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days' AND src_node_id IS NOT NULL ${filters.packets}
        GROUP BY day ORDER BY day
      `, filters.params),
      // packet types — last 24h (deduplicated by hash)
      query(`
        SELECT packet_type, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}
        GROUP BY packet_type ORDER BY count DESC
      `, filters.params),
      // total known repeaters at each hour — cumulative count from nodes.created_at — last 7d
      query(`
        WITH hours AS (
          SELECT generate_series(
            date_trunc('hour', NOW() - INTERVAL '7 days'),
            date_trunc('hour', NOW()),
            INTERVAL '1 hour'
          ) AS hour
        )
        SELECT h.hour,
          (SELECT COUNT(*)
           FROM nodes n
           WHERE (n.role IS NULL OR n.role = 2)
             AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
             ${filters.nodesAlias('n')}
             AND n.created_at <= h.hour + INTERVAL '1 hour') AS count
        FROM hours h
        ORDER BY h.hour
      `, filters.params),
      // hop count distribution — last 7d (deduplicated by hash)
      query(`
        SELECT hop_count AS hops, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE time > NOW() - INTERVAL '7 days'
          AND hop_count IS NOT NULL
          ${filters.packets}
        GROUP BY hop_count ORDER BY hop_count
      `, filters.params),
      // top 10 public channel chatters by decoded sender name — last 7d (deduplicated by hash)
      query(`
        SELECT payload->'decrypted'->>'sender' AS name, COUNT(DISTINCT packet_hash) AS count
        FROM packets
        WHERE packet_type = 5
          AND time > NOW() - INTERVAL '7 days'
          AND payload->'decrypted'->>'sender' IS NOT NULL
          ${filters.packets}
        GROUP BY 1 ORDER BY count DESC LIMIT 10
      `, filters.params),
      // summary
      query(`
        SELECT
          (SELECT COUNT(DISTINCT packet_hash) FROM packets WHERE time > NOW() - INTERVAL '24 hours' ${filters.packets}) AS total_24h,
          (SELECT COUNT(DISTINCT packet_hash) FROM packets WHERE time > NOW() - INTERVAL '7 days' ${filters.packets}) AS total_7d,
          (SELECT COUNT(DISTINCT src_node_id) FROM packets WHERE time > NOW() - INTERVAL '24 hours' AND src_node_id IS NOT NULL ${filters.packets}) AS unique_radios_24h,
          (SELECT COUNT(*) FROM nodes n WHERE (n.role IS NULL OR n.role = 2) AND n.last_seen > NOW() - INTERVAL '7 days' ${filters.nodesAlias('n')}) AS active_repeaters,
          (SELECT COUNT(*) FROM nodes n WHERE (n.role IS NULL OR n.role = 2) AND n.last_seen <= NOW() - INTERVAL '7 days' AND n.last_seen > NOW() - INTERVAL '14 days' ${filters.nodesAlias('n')}) AS stale_repeaters
      `, filters.params),
    ]);

    const peakRow = phResult.rows.reduce(
      (best: any, r: any) => (Number(r.count) > Number(best?.count ?? 0) ? r : best),
      null
    );

    const fmtHour = (ts: Date | string) => {
      const d = new Date(ts);
      return `${d.getHours().toString().padStart(2, '0')}:00`;
    };
    const fmtDay = (ts: Date | string) => {
      const d = new Date(ts);
      return d.toLocaleDateString('en-GB', { weekday: 'short', month: 'short', day: 'numeric' });
    };

    res.json({
      packetsPerHour:  phResult.rows.map(r => ({ hour: fmtHour(r.hour), count: Number(r.count) })),
      packetsPerDay:   pdResult.rows.map(r => ({ day: fmtDay(r.day), count: Number(r.count) })),
      radiosPerHour:   rhResult.rows.map(r => ({ hour: fmtHour(r.hour), count: Number(r.count) })),
      radiosPerDay:    rdResult.rows.map(r => ({ day: fmtDay(r.day), count: Number(r.count) })),
      packetTypes:     ptResult.rows.map(r => ({ label: PAYLOAD_LABELS[Number(r.packet_type)] ?? `Type${r.packet_type}`, count: Number(r.count) })),
      repeatersPerDay: rpResult.rows.map(r => ({ hour: fmtDay(r.hour), count: Number(r.count ?? 0) })),
      hopDistribution: hdResult.rows.map(r => ({ hops: Number(r.hops), count: Number(r.count) })),
      topChatters:     tcResult.rows.map(r => ({ name: r.name ?? 'Unknown', count: Number(r.count) })),
      summary: {
        totalPackets24h:  Number(sumResult.rows[0].total_24h),
        totalPackets7d:   Number(sumResult.rows[0].total_7d),
        uniqueRadios24h:  Number(sumResult.rows[0].unique_radios_24h),
        activeRepeaters:  Number(sumResult.rows[0].active_repeaters ?? 0),
        staleRepeaters:   Number(sumResult.rows[0].stale_repeaters ?? 0),
        peakHour:         peakRow ? fmtHour(peakRow.hour) : null,
        peakHourCount:    peakRow ? Number(peakRow.count) : 0,
      },
    });
  } catch (err) {
    console.error('[api] GET /stats/charts', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
