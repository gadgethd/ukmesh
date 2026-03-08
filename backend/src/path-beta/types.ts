export type MeshNode = {
  node_id: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  iata: string | null;
  role: number | null;
  elevation_m: number | null;
};

export type LinkMetrics = {
  observed_count: number;
  itm_path_loss_db: number | null;
  count_a_to_b: number | null;
  count_b_to_a: number | null;
};

export type NodeCoverage = {
  node_id: string;
  radius_m: number | null;
};

export type PathLearningModel = {
  prefixProbabilities: Map<string, number>;
  transitionProbabilities: Map<string, number>;
  edgeScores: Map<string, number>;
  motifProbabilities: Map<string, number>;
  confidenceScale: number;
  confidenceBias: number;
  bucketHours: number;
};

export type PathPacket = {
  packet_hash: string;
  rx_node_id: string | null;
  src_node_id: string | null;
  packet_type: number | null;
  hop_count: number | null;
  path_hashes: string[] | null;
};

export type ObserverHopHint = {
  observerNode: MeshNode;
  hopCount: number;
  hopDelta: number;
};

export type BetaResolveContext = {
  loadedAt: number;
  nodesById: Map<string, MeshNode>;
  coverageByNode: Map<string, number>;
  linkPairs: Set<string>;
  linkMetrics: Map<string, LinkMetrics>;
  learningModel: PathLearningModel;
};
