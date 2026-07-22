export type MeshNode = {
  node_id: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  iata: string | null;
  role: number | null;
  elevation_m: number | null;
  last_seen: string | null;
};

export type LinkMetrics = {
  observed_count: number;
  multibyte_observed_count: number;
  neighbor_report_count?: number;
  neighbor_best_snr_db?: number | null;
  itm_path_loss_db: number | null;
  itm_viable: boolean | null;
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

export type NeighborAffinityMetrics = {
  count: number;
  observerCount: number;
  avgSnr: number | null;
  lastSeen: string | null;
  score: number;
};

export type MlPrefixScore = {
  score: number;
  observationCount: number;
};

export type PathPacket = {
  packet_hash: string;
  rx_node_id: string | null;
  src_node_id: string | null;
  packet_type: number | null;
  hop_count: number | null;
  path_hashes: string[] | null;
  path_hash_size_bytes: number | null;
};

export type ObserverHopHint = {
  observerNode: MeshNode;
  hopCount: number;
  hopDelta: number;
};

export type BetaResolveContext = {
  loadedAt: number;
  nodesById: Map<string, MeshNode>;
  /** Repeater candidates with valid coordinates, prefiltered once per context refresh. */
  repeaterNodes: MeshNode[];
  /** Node IDs indexed by their supported path-hash prefixes. */
  repeaterPathHashIndex: Map<string, MeshNode[]>;
  coverageByNode: Map<string, number>;
  /** Links with itm_viable=true OR force_viable=true — theoretical viability from ITM model. */
  linkPairs: Set<string>;
  /** Subset of linkPairs where observed_count > 0 — links confirmed by actual packet observations. */
  observedLinkPairs: Set<string>;
  /** Highest-trust observed multibyte links that are also terrain/radio viable. */
  trustedPathPairs: Set<string>;
  /** Adjacency for trustedPathPairs, used to keep live path candidate sets small. */
  trustedPathNeighbors: Map<string, Set<string>>;
  linkMetrics: Map<string, LinkMetrics>;
  /** Candidate adjacency used by the clash penalty in the path solver. */
  clashAdjacency: Map<string, Set<string>>;
  /** Packet-derived first-hop affinity, resolved only when both endpoints are known full node IDs. */
  neighborAffinity: Map<string, NeighborAffinityMetrics>;
  /** Adjacency built from packet-derived first-hop affinity for shared-neighbor scoring. */
  neighborAffinityNeighbors: Map<string, Set<string>>;
  /** High-confidence ML mapping from 1-byte path hash prefix to likely node IDs. */
  mlPrefixScores: Map<string, Map<string, MlPrefixScore>>;
  learningModel: PathLearningModel;
};
