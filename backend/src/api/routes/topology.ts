import type { Router } from 'express';
import type { QueryResultRow } from 'pg';
import { resolvePublicNetworkScope } from '../../http/requestScope.js';
import type { NetworkFilters } from '../utils/networkFilters.js';
import { parseBoundedInteger } from '../utils/input.js';
import { isPrivateNode } from '../utils/privateNode.js';
import {
  standaloneTopologyRows,
  topologyRows,
  type StandaloneNodeRow,
  type TopologyRow,
} from '../../repositories/networkAnalysis.js';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type TopologyNode = {
  nodeId: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  degree: number;
  observations: number;
  region?: string | null;
};

export type TopologyLink = {
  source: string;
  target: string;
  observations: number;
  strongObservations: number;
  pathLossDb: number | null;
  lastObserved: string;
};

export type TopologyAnalysis = {
  connectedComponents: number;
  bridgeNodeIds: string[];
  isolatedNodeIds: string[];
};

/** Finds articulation points in the bounded observed graph. */
export function analyzeTopology(nodes: TopologyNode[], links: TopologyLink[]): TopologyAnalysis {
  const adjacency = new Map(nodes.map((node) => [node.nodeId, new Set<string>()]));
  for (const link of links) {
    adjacency.get(link.source)?.add(link.target);
    adjacency.get(link.target)?.add(link.source);
  }

  const visited = new Set<string>();
  const discovery = new Map<string, number>();
  const low = new Map<string, number>();
  const parent = new Map<string, string>();
  const bridges = new Set<string>();
  let time = 0;
  let connectedComponents = 0;

  const visit = (nodeId: string) => {
    visited.add(nodeId);
    discovery.set(nodeId, ++time);
    low.set(nodeId, time);
    let children = 0;
    for (const neighbour of adjacency.get(nodeId) ?? []) {
      if (!visited.has(neighbour)) {
        children += 1;
        parent.set(neighbour, nodeId);
        visit(neighbour);
        low.set(nodeId, Math.min(low.get(nodeId)!, low.get(neighbour)!));
        if (!parent.has(nodeId) && children > 1) bridges.add(nodeId);
        if (parent.has(nodeId) && low.get(neighbour)! >= discovery.get(nodeId)!) bridges.add(nodeId);
      } else if (parent.get(nodeId) !== neighbour) {
        low.set(nodeId, Math.min(low.get(nodeId)!, discovery.get(neighbour)!));
      }
    }
  };

  for (const node of nodes) {
    if (!visited.has(node.nodeId)) {
      connectedComponents += 1;
      visit(node.nodeId);
    }
  }
  return {
    connectedComponents,
    bridgeNodeIds: [...bridges].sort(),
    isolatedNodeIds: nodes.filter((node) => (adjacency.get(node.nodeId)?.size ?? 0) === 0).map((node) => node.nodeId).sort(),
  };
}

export function shapeTopology(rows: TopologyRow[], standaloneRows: StandaloneNodeRow[] = []): { nodes: TopologyNode[]; links: TopologyLink[]; analysis: TopologyAnalysis } {
  const nodes = new Map<string, TopologyNode>();
  const links: TopologyLink[] = [];

  const addNode = (nodeId: string, name: string | null, lat: number | null, lon: number | null, observations: number, region?: string | null) => {
    const existing = nodes.get(nodeId);
    if (existing) {
      existing.degree += 1;
      existing.observations += observations;
      return;
    }
    nodes.set(nodeId, {
      nodeId,
      name,
      lat,
      lon,
      degree: 1,
      observations,
      region: region ?? null,
    });
  };

  for (const row of rows) {
    if (isPrivateNode(row.name_a) || isPrivateNode(row.name_b)) continue;
    const observations = Number(row.observed_count) || 0;
    addNode(row.node_a_id, row.name_a, row.lat_a, row.lon_a, observations, row.iata_a);
    addNode(row.node_b_id, row.name_b, row.lat_b, row.lon_b, observations, row.iata_b);
    links.push({
      source: row.node_a_id,
      target: row.node_b_id,
      observations,
      strongObservations: Number(row.multibyte_observed_count) || 0,
      pathLossDb: row.itm_path_loss_db,
      lastObserved: row.last_observed,
    });
  }

  for (const row of standaloneRows) {
    if (isPrivateNode(row.name)) continue;
    if (nodes.has(row.node_id)) continue;
    nodes.set(row.node_id, {
      nodeId: row.node_id,
      name: row.name,
      lat: row.lat,
      lon: row.lon,
      degree: 0,
      observations: 0,
      region: row.iata ?? null,
    });
  }

  const shapedNodes = Array.from(nodes.values()).sort((a, b) => b.degree - a.degree || b.observations - a.observations);

  return {
    nodes: shapedNodes,
    links,
    analysis: analyzeTopology(shapedNodes, links),
  };
}

type TopologyRouteDeps = {
  query: QueryFn;
  networkFilters: (network?: string, observer?: string) => NetworkFilters;
  limiter: ReturnType<typeof import('express-rate-limit').rateLimit>;
};

export function registerTopologyRoutes(router: Router, deps: TopologyRouteDeps): void {
  router.get('/topology', deps.limiter, async (req, res) => {
    const limit = parseBoundedInteger(req.query['limit'], {
      name: 'limit',
      defaultValue: 300,
      min: 50,
      max: 500,
    });
    try {
      const network = resolvePublicNetworkScope(req.query['network'], req.headers);
      const filters = deps.networkFilters(network);
      const result = await topologyRows(deps.query, filters, limit);
      const standaloneResult = await standaloneTopologyRows(deps.query, filters);
      const topology = shapeTopology(result.rows, standaloneResult.rows);
      const generatedAt = new Date().toISOString();
      res.setHeader('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
      res.json({
        generatedAt,
        windowDays: 30,
        limited: result.rows.length === limit,
        summary: {
          nodes: topology.nodes.length,
          links: topology.links.length,
          observations: topology.links.reduce((sum, link) => sum + link.observations, 0),
          connectedComponents: topology.analysis.connectedComponents,
          likelyBridges: topology.analysis.bridgeNodeIds.length,
          isolatedNodes: topology.analysis.isolatedNodeIds.length,
        },
        ...topology,
      });
    } catch (err) {
      console.error('[api] GET /topology', (err as Error).message);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
}
