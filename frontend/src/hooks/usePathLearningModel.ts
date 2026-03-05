import { useEffect, useState } from 'react';
import { withNetworkParam, uncachedEndpoint } from '../utils/api.js';
import type { PathLearningModel } from '../utils/betaPathing.js';

type PathLearningApiResponse = {
  calibration: {
    confidence_scale: number;
    confidence_bias: number;
    recommended_threshold: number;
  };
  prefixPriors: Array<{
    prefix: string;
    receiver_region: string;
    prev_prefix: string | null;
    node_id: string;
    probability: number;
  }>;
  transitionPriors: Array<{
    from_node_id: string;
    to_node_id: string;
    receiver_region: string;
    probability: number;
  }>;
  edgePriors: Array<{
    from_node_id: string;
    to_node_id: string;
    receiver_region: string;
    hour_bucket: number;
    score: number;
  }>;
  motifPriors: Array<{
    receiver_region: string;
    hour_bucket: number;
    motif_len: number;
    node_ids: string;
    probability: number;
  }>;
};

export function usePathLearningModel(network?: string): PathLearningModel | null {
  const [model, setModel] = useState<PathLearningModel | null>(null);

  useEffect(() => {
    const load = () => {
      const endpoint = withNetworkParam('/api/path-learning?limit=6000', network);
      fetch(uncachedEndpoint(endpoint), { cache: 'no-store' })
        .then((response) => response.json())
        .then((data: PathLearningApiResponse) => {
          const prefixProbabilities = new Map<string, number>();
          for (const row of data.prefixPriors) {
            const key = `${row.receiver_region}|${row.prefix}|${row.prev_prefix ?? ''}|${row.node_id}`;
            prefixProbabilities.set(key, Number(row.probability));
          }

          const transitionProbabilities = new Map<string, number>();
          for (const row of data.transitionPriors) {
            const key = `${row.receiver_region}|${row.from_node_id}|${row.to_node_id}`;
            transitionProbabilities.set(key, Number(row.probability));
          }

          const edgeScores = new Map<string, number>();
          const edgeTotals = new Map<string, { sum: number; count: number }>();
          for (const row of data.edgePriors ?? []) {
            const region = row.receiver_region;
            const hourBucket = Number(row.hour_bucket);
            const from = row.from_node_id;
            const to = row.to_node_id;
            const score = Number(row.score);
            const key = `${region}|${hourBucket}|${from}|${to}`;
            edgeScores.set(key, score);

            const aggregateKey = `${region}|${from}|${to}`;
            const agg = edgeTotals.get(aggregateKey) ?? { sum: 0, count: 0 };
            agg.sum += score;
            agg.count += 1;
            edgeTotals.set(aggregateKey, agg);
          }
          for (const [aggregateKey, agg] of edgeTotals) {
            if (agg.count <= 0) continue;
            const [region, from, to] = aggregateKey.split('|');
            edgeScores.set(`${region}|-1|${from}|${to}`, agg.sum / agg.count);
          }

          const motifProbabilities = new Map<string, number>();
          const motifTotals = new Map<string, { sum: number; count: number }>();
          for (const row of data.motifPriors ?? []) {
            const region = row.receiver_region;
            const hourBucket = Number(row.hour_bucket);
            const motifLen = Number(row.motif_len);
            const nodeIds = row.node_ids;
            const probability = Number(row.probability);
            const key = `${region}|${hourBucket}|${motifLen}|${nodeIds}`;
            motifProbabilities.set(key, probability);

            const aggregateKey = `${region}|${motifLen}|${nodeIds}`;
            const agg = motifTotals.get(aggregateKey) ?? { sum: 0, count: 0 };
            agg.sum += probability;
            agg.count += 1;
            motifTotals.set(aggregateKey, agg);
          }
          for (const [aggregateKey, agg] of motifTotals) {
            if (agg.count <= 0) continue;
            const [region, motifLen, nodeIds] = aggregateKey.split('|');
            motifProbabilities.set(`${region}|-1|${motifLen}|${nodeIds}`, agg.sum / agg.count);
          }

          setModel({
            prefixProbabilities,
            transitionProbabilities,
            edgeScores,
            motifProbabilities,
            confidenceScale: Number(data.calibration?.confidence_scale ?? 1),
            confidenceBias: Number(data.calibration?.confidence_bias ?? 0),
            recommendedThreshold: Number(data.calibration?.recommended_threshold ?? 0.5),
            bucketHours: 6,
          });
        })
        .catch(() => {});
    };

    load();
    const interval = setInterval(load, 5 * 60_000);
    return () => clearInterval(interval);
  }, [network]);

  return model;
}
