import type * as maplibregl from 'maplibre-gl';

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
  is_prohibited: boolean;
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
  showLinks: boolean;
  showTerrain: boolean;
  showClientNodes: boolean;
  showHexClashes: boolean;
  maxHexClashHops: number;
  viewshedEnabled: boolean;
  rfCoverageEnabled: boolean;
  selectedRfCoverageNodeKey?: string | null;
  getRfCoverageNodeState?: (publicKey: string) => import('../../hooks/useRfCoverage.js').RfNodeCoverageState;
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
  privacyGeneration: number;
  /** Hide dashboard-only search, legend, and planning controls for embedded maps. */
  showMapChrome?: boolean;
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

// Dormant rollback shape used only by the hidden planned-repeater code. The
// live RF layer never consumes this legacy polygon contract.
export interface LegacyCoverageGeometry {
  node_id: string;
  geom: { type: string; coordinates: unknown };
  strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  antenna_height_m?: number;
  radius_m?: number;
  predicted_links?: PredictedLink[];
  calculated_at?: string;
}

export interface PlannedRepeater {
  id: string;        // plan_<16hex>
  lat: number;
  lon: number;
  status: 'queued' | 'ready' | 'error';
  coverage?: LegacyCoverageGeometry;
}
