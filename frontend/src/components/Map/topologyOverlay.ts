import { filterTopologyLinks } from '../../pages/topologyModel.js';

export type TopologyMapNode = {
  nodeId: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  degree: number;
  observations: number;
  region?: string | null;
};

export type TopologyMapLink = {
  source: string;
  target: string;
  observations: number;
  strongObservations: number;
  pathLossDb: number | null;
  lastObserved: string;
};

type TopologyFeatureProperties = {
  [key: string]: string | number | boolean | null;
};

function hasCoordinates(node: TopologyMapNode): node is TopologyMapNode & { lat: number; lon: number } {
  const { lat, lon } = node;
  return lat != null && lon != null
    && Number.isFinite(lat) && Number.isFinite(lon)
    && lat >= -90 && lat <= 90
    && lon >= -180 && lon <= 180;
}

function linkColor(link: TopologyMapLink, selected: boolean): string {
  if (selected) return '#f8fafc';
  if (link.strongObservations > 0) return '#22d3ee';
  if (link.pathLossDb == null) return '#70d7ef';
  return link.pathLossDb <= 121.5 ? '#34d399' : '#fbbf24';
}

export function buildTopologyLinksGeoJSON(
  nodes: readonly TopologyMapNode[],
  links: readonly TopologyMapLink[],
  selectedNodeId: string | null,
  strongOnly: boolean,
): GeoJSON.FeatureCollection<GeoJSON.LineString, TopologyFeatureProperties> {
  const locatedNodes = nodes.filter(hasCoordinates);
  const locatedById = new Map(locatedNodes.map((node) => [node.nodeId, node]));
  const visibleLinks = filterTopologyLinks(locatedById.keys(), links)
    .filter((link) => !strongOnly || link.strongObservations > 0);
  const maxObservations = Math.max(1, ...visibleLinks.map((link) => Number(link.observations) || 0));

  return {
    type: 'FeatureCollection',
    features: visibleLinks.map((link) => {
      const source = locatedById.get(link.source)!;
      const target = locatedById.get(link.target)!;
      const observations = Math.max(0, Number(link.observations) || 0);
      const selected = selectedNodeId === link.source || selectedNodeId === link.target;
      const strength = Math.log2(observations + 1) / Math.log2(maxObservations + 1);
      return {
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [[source.lon, source.lat], [target.lon, target.lat]],
        },
        properties: {
          source: link.source,
          target: link.target,
          observations,
          strong_observations: Number(link.strongObservations) || 0,
          color: linkColor(link, selected),
          width: Math.min(5.5, 0.9 + strength * 3.2),
          opacity: selected ? 0.96 : 0.34 + strength * 0.42,
          halo_opacity: selected ? 0.24 : 0.08 + strength * 0.08,
        },
      };
    }),
  };
}

export function buildTopologyNodesGeoJSON(
  nodes: readonly TopologyMapNode[],
  selectedNodeId: string | null,
  bridgeNodeIds: ReadonlySet<string>,
  isolatedNodeIds: ReadonlySet<string>,
): GeoJSON.FeatureCollection<GeoJSON.Point, TopologyFeatureProperties> {
  return {
    type: 'FeatureCollection',
    features: nodes.filter(hasCoordinates).map((node) => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [node.lon, node.lat],
      },
      properties: {
        node_id: node.nodeId,
        name: node.name,
        label: node.name ?? node.nodeId.slice(0, 8),
        degree: Math.max(0, Number(node.degree) || 0),
        observations: Math.max(0, Number(node.observations) || 0),
        selected: node.nodeId === selectedNodeId,
        bridge: bridgeNodeIds.has(node.nodeId),
        isolated: isolatedNodeIds.has(node.nodeId),
      },
    })),
  };
}
