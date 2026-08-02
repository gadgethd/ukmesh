import type maplibregl from 'maplibre-gl';

export interface NodeLink {
  peer_id: string;
  peer_name: string | null;
  observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number;
  count_peer_to_this: number;
}

export interface PopupState {
  nodeId: string;
  lngLat: maplibregl.LngLatLike;
}

export interface NodeFeatureProps {
  node_id: string;
  name: string | null;
  role: number;
  is_online: boolean;
  is_stale: boolean;
  is_link_only_stale: boolean;
  is_prohibited: boolean;
  is_inferred: boolean;
  replay_active: boolean;
  replay_mode: boolean;
  hex_clash_state: 'offender' | 'relay' | null;
  visible: boolean;
  last_seen: string;
  public_key: string | null;
  advert_count: number | null;
  elevation_m: number | null;
  hardware_model: string | null;
}

export type LatLonPosition = [number, number];

export interface ClashPathLine {
  key: string;
  positions: LatLonPosition[];
}

export interface ClashComputation {
  clashOffenderNodeIds: Set<string>;
  clashRelayIds: Set<string>;
  clashPathLines: ClashPathLine[];
  clashModeActive: boolean;
}

export interface LosProfile {
  peer_id: string;
  peer_name: string | null;
  itm_path_loss_db: number | null;
  itm_viable: boolean;
  profile: [number, number, number][];  // [lon, lat, elev_m]
}

export interface CustomLosPoint {
  lat: number;
  lon: number;
  elevation_m: number;
}

export interface CustomLosSegment {
  path: [number, number, number][];
  obstructed: boolean;
}

export interface MapLibreMapProps {
  inferredNodes: import('../../hooks/useNodes.js').MeshNode[];
  inferredActiveNodeIds: Set<string>;
  showLinks: boolean;
  showTerrain: boolean;
  showCoverage: boolean;
  showClientNodes: boolean;
  showHexClashes: boolean;
  maxHexClashHops: number;
  viewshedEnabled: boolean;
  initialView?: { lat: number; lon: number; zoom: number } | null;
  /** Currently selected node (drives the docked node panel + map highlight). */
  selectedNodeId?: string | null;
  onNodeSelect?: (nodeId: string | null) => void;
  onMapReady?: (map: maplibregl.Map) => void;
  mapLight: boolean;
  network?: string;
  observer?: string;
  privacyGeneration: number;
}

export interface PopupNodeView {
  props: NodeFeatureProps;
  maskedLat: number;
  maskedLon: number;
}

export interface PredictedLink {
  peer_id: string;
  peer_name: string | null;
  itm_path_loss_db: number | null;
  itm_viable: boolean;
  distance_km: number | null;
}

export interface PlannedRepeater {
  id: string;        // plan_<16hex>
  lat: number;
  lon: number;
  status: 'queued' | 'ready' | 'error';
  coverage?: import('../../hooks/useCoverage.js').NodeCoverage;
}
