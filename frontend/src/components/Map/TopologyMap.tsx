import { useCallback, useEffect, useState } from 'react';
import * as maplibregl from 'maplibre-gl';
import { MapLibreMap } from './MapLibreMap.js';
import { DEFAULT_CENTER } from './mapConfig.js';
import {
  buildTopologyLinksGeoJSON,
  buildTopologyNodesGeoJSON,
  type TopologyMapLink,
  type TopologyMapNode,
} from './topologyOverlay.js';

const TOPOLOGY_LINK_HALO_LAYER = 'topology-links-halo';
const TOPOLOGY_LINK_LAYER = 'topology-links';
const TOPOLOGY_NODE_HIT_LAYER = 'topology-node-hit';
const TOPOLOGY_NODE_HALO_LAYER = 'topology-node-halo';
const TOPOLOGY_NODE_LAYER = 'topology-node';
const TOPOLOGY_INITIAL_VIEW = { lat: DEFAULT_CENTER[0], lon: -2.4, zoom: 6 } as const;

type TopologyMapProps = {
  nodes: readonly TopologyMapNode[];
  links: readonly TopologyMapLink[];
  selectedNodeId: string | null;
  bridgeNodeIds: ReadonlySet<string>;
  isolatedNodeIds: ReadonlySet<string>;
  strongOnly: boolean;
  network?: string;
  observer?: string;
  privacyGeneration: number;
  onNodeSelect: (nodeId: string) => void;
};

type TopologyMapOverlayProps = Omit<TopologyMapProps, 'network' | 'observer' | 'privacyGeneration' | 'onNodeSelect'> & {
  map: maplibregl.Map;
  onNodeSelect: (nodeId: string) => void;
};

function addTopologyLayer(map: maplibregl.Map, layer: maplibregl.LayerSpecification): void {
  if (map.getLayer(layer.id)) return;
  const beforeId = map.getLayer('map-labels-water') ? 'map-labels-water' : undefined;
  if (beforeId) map.addLayer(layer, beforeId);
  else map.addLayer(layer);
}

const topologyLinkLayerBase = {
  type: 'line' as const,
  source: 'topology-links',
  layout: {
    'line-cap': 'round' as const,
    'line-join': 'round' as const,
  },
};

function TopologyMapOverlay({
  map,
  nodes,
  links,
  selectedNodeId,
  bridgeNodeIds,
  isolatedNodeIds,
  strongOnly,
  onNodeSelect,
}: TopologyMapOverlayProps): null {
  useEffect(() => {
    map.addSource('topology-links', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    });
    map.addSource('topology-nodes', {
      type: 'geojson',
      data: { type: 'FeatureCollection', features: [] },
    });

    addTopologyLayer(map, {
      ...topologyLinkLayerBase,
      id: TOPOLOGY_LINK_HALO_LAYER,
      paint: {
        'line-color': ['get', 'color'],
        'line-width': ['+', ['get', 'width'], 4],
        'line-opacity': ['get', 'halo_opacity'],
      },
    });
    addTopologyLayer(map, {
      ...topologyLinkLayerBase,
      id: TOPOLOGY_LINK_LAYER,
      paint: {
        'line-color': ['get', 'color'],
        'line-width': ['get', 'width'],
        'line-opacity': ['get', 'opacity'],
      },
    });
    addTopologyLayer(map, {
      id: TOPOLOGY_NODE_HIT_LAYER,
      type: 'circle',
      source: 'topology-nodes',
      paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 10, 10, 14, 14, 18],
        'circle-color': '#ffffff',
        'circle-opacity': 0,
        'circle-stroke-opacity': 0,
      },
    });
    addTopologyLayer(map, {
      id: TOPOLOGY_NODE_HALO_LAYER,
      type: 'circle',
      source: 'topology-nodes',
      paint: {
        'circle-radius': ['+', ['get', 'radius'], 5],
        'circle-color': [
          'case',
          ['get', 'selected'], '#22e0ff',
          ['get', 'bridge'], '#f59e0b',
          '#00c4ff',
        ],
        'circle-opacity': [
          'case',
          ['get', 'selected'], 0.32,
          ['get', 'bridge'], 0.22,
          0.08,
        ],
        'circle-blur': 0.55,
      },
    });
    addTopologyLayer(map, {
      id: TOPOLOGY_NODE_LAYER,
      type: 'circle',
      source: 'topology-nodes',
      paint: {
        'circle-radius': [
          'interpolate', ['linear'], ['get', 'degree'],
          0, 3.5, 1, 4.5, 4, 6, 10, 8, 24, 11,
        ],
        'circle-color': [
          'case',
          ['get', 'selected'], '#f8fafc',
          ['get', 'bridge'], '#fbbf24',
          '#00c4ff',
        ],
        'circle-opacity': ['case', ['get', 'isolated'], 0, 1],
        'circle-stroke-color': [
          'case',
          ['get', 'selected'], '#ffffff',
          ['get', 'bridge'], '#f59e0b',
          ['get', 'isolated'], '#d7e8f7',
          '#020617',
        ],
        'circle-stroke-width': [
          'case',
          ['get', 'selected'], 2.5,
          ['get', 'bridge'], 2.2,
          1.2,
        ],
        'circle-stroke-opacity': 0.92,
      },
    });

    const handleNodeClick = (event: maplibregl.MapLayerMouseEvent) => {
      const nodeId = event.features?.[0]?.properties?.['node_id'];
      if (typeof nodeId === 'string' && nodeId.length > 0) onNodeSelect(nodeId);
    };
    const handlePointerEnter = () => { map.getCanvas().style.cursor = 'pointer'; };
    const handlePointerLeave = () => { map.getCanvas().style.cursor = ''; };
    map.on('click', TOPOLOGY_NODE_HIT_LAYER, handleNodeClick);
    map.on('mouseenter', TOPOLOGY_NODE_HIT_LAYER, handlePointerEnter);
    map.on('mouseleave', TOPOLOGY_NODE_HIT_LAYER, handlePointerLeave);

    const navigation = new maplibregl.NavigationControl({ showCompass: false });
    map.addControl(navigation, 'top-right');

    return () => {
      map.off('click', TOPOLOGY_NODE_HIT_LAYER, handleNodeClick);
      map.off('mouseenter', TOPOLOGY_NODE_HIT_LAYER, handlePointerEnter);
      map.off('mouseleave', TOPOLOGY_NODE_HIT_LAYER, handlePointerLeave);
      try { map.removeControl(navigation); } catch { /* map may already be removing */ }
      try {
        for (const layerId of [
          TOPOLOGY_NODE_LAYER,
          TOPOLOGY_NODE_HALO_LAYER,
          TOPOLOGY_NODE_HIT_LAYER,
          TOPOLOGY_LINK_LAYER,
          TOPOLOGY_LINK_HALO_LAYER,
        ]) {
          if (map.getLayer(layerId)) map.removeLayer(layerId);
        }
        if (map.getSource('topology-nodes')) map.removeSource('topology-nodes');
        if (map.getSource('topology-links')) map.removeSource('topology-links');
      } catch { /* MapLibreMap owns final map teardown */ }
    };
  }, [map, onNodeSelect]);

  useEffect(() => {
    const linksSource = map.getSource('topology-links') as maplibregl.GeoJSONSource | undefined;
    const nodesSource = map.getSource('topology-nodes') as maplibregl.GeoJSONSource | undefined;
    linksSource?.setData(buildTopologyLinksGeoJSON(nodes, links, selectedNodeId, strongOnly));
    nodesSource?.setData(buildTopologyNodesGeoJSON(nodes, selectedNodeId, bridgeNodeIds, isolatedNodeIds));
  }, [map, nodes, links, selectedNodeId, bridgeNodeIds, isolatedNodeIds, strongOnly]);

  return null;
}

export function TopologyMap({
  nodes,
  links,
  selectedNodeId,
  bridgeNodeIds,
  isolatedNodeIds,
  strongOnly,
  network,
  observer,
  privacyGeneration,
  onNodeSelect,
}: TopologyMapProps): React.ReactElement {
  const [map, setMap] = useState<maplibregl.Map | null>(null);
  const handleMapReady = useCallback((nextMap: maplibregl.Map) => setMap(nextMap), []);
  const positionedNodeCount = nodes.filter((node) => Number.isFinite(node.lat) && Number.isFinite(node.lon)).length;

  return (
    <div className="topology-page__map" aria-label="Geographic repeater topology map">
      <MapLibreMap
        showLinks={false}
        showTerrain={false}
        showClientNodes={false}
        showHexClashes={false}
        maxHexClashHops={3}
        viewshedEnabled={false}
        rfCoverageEnabled={false}
        initialView={TOPOLOGY_INITIAL_VIEW}
        mapLight={false}
        network={network}
        observer={observer}
        privacyGeneration={privacyGeneration}
        onMapReady={handleMapReady}
        showMapChrome={false}
      />
      {map && (
        <TopologyMapOverlay
          map={map}
          nodes={nodes}
          links={links}
          selectedNodeId={selectedNodeId}
          bridgeNodeIds={bridgeNodeIds}
          isolatedNodeIds={isolatedNodeIds}
          strongOnly={strongOnly}
          onNodeSelect={onNodeSelect}
        />
      )}
      <div className="topology-page__map-meta" aria-live="polite">
        <strong>Live topology</strong>
        <span>{positionedNodeCount} mapped repeaters · {links.length} observed relationships</span>
        <span>{strongOnly ? 'Showing multibyte-backed relationships' : 'Showing all viable relationships'}</span>
      </div>
      <div className="topology-page__map-legend" aria-label="Topology map legend">
        <span><i className="topology-page__legend-swatch topology-page__legend-swatch--link" /> Relationship · line weight = observations</span>
        <span><i className="topology-page__legend-swatch topology-page__legend-swatch--strong" /> Multibyte evidence</span>
        <span><i className="topology-page__legend-swatch topology-page__legend-swatch--bridge" /> Likely bridge</span>
        <span><i className="topology-page__legend-swatch topology-page__legend-swatch--isolated" /> Isolated</span>
      </div>
    </div>
  );
}
