import type * as maplibregl from 'maplibre-gl';

export interface MapLibreMapProps {
  showLinks: boolean;
  showTerrain: boolean;
  showClientNodes: boolean;
  showHexClashes: boolean;
  maxHexClashHops: number;
  viewshedEnabled: boolean;
  rfCoverageEnabled: boolean;
  selectedRfCoverageNodeKey?: string | null;
  getRfCoverageNodeState?: (publicKey: string) => import('../../features/rf/model.js').RfNodeCoverageState;
  onShowRfCoverage?: (publicKey: string) => void;
  onClearRfCoverage?: () => void;
  initialView?: { lat: number; lon: number; zoom: number } | null;
  /** Currently selected node (drives the docked node panel + map highlight). */
  selectedNodeId?: string | null;
  onNodeSelect?: (nodeId: string | null) => void;
  onMapReady?: (map: maplibregl.Map) => void;
  mapLight: boolean;
  network?: string;
  observer?: string;
  region?: string;
  privacyGeneration: number;
  /** Hide dashboard-only search, legend, and planning controls for embedded maps. */
  showMapChrome?: boolean;
}
