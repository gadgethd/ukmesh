export type LearningNode = {
  node_id: string;
  lat: number;
  lon: number;
  elevation_m: number | null;
  iata: string | null;
};

export type LearningPacket = {
  direction?: string | null;
  time: string;
  rx_node_id: string;
  src_node_id: string | null;
  path_hashes: string[] | null;
};

export type LearningLink = {
  node_a_id: string;
  node_b_id: string;
  itm_path_loss_db: number | null;
  count_a_to_b: number;
  count_b_to_a: number;
};

export type ResolvedHop = {
  position: number;
  prefix: string;
  node: LearningNode;
  isUnique: boolean;
};

export type LearningPathNode = {
  node: LearningNode;
  isVerified: boolean;
};

export type PrefixPriorWriteRow = {
  prefix: string;
  receiver_region: string;
  prev_prefix: string;
  node_id: string;
  count: number;
  probability: number;
};

export type TransitionPriorWriteRow = {
  from_node_id: string;
  to_node_id: string;
  receiver_region: string;
  count: number;
  probability: number;
};

export type PositionPrefixPriorWriteRow = {
  prefix: string;
  position: number;
  node_id: string;
  count: number;
  probability: number;
};

export type CorridorPriorWriteRow = {
  src_node_id: string;
  rx_node_id: string;
  position: number;
  node_id: string;
  count: number;
  probability: number;
};

export type PositionTransitionPriorWriteRow = {
  position: number;
  from_node_id: string;
  to_node_id: string;
  count: number;
  probability: number;
};

export type EdgePriorWriteRow = {
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
};

export type MotifPriorWriteRow = {
  receiver_region: string;
  hour_bucket: number;
  motif_len: number;
  node_ids: string;
  count: number;
  probability: number;
};

export type PathLearningCalibration = {
  evaluatedPackets: number;
  top1Accuracy: number;
  meanPredConfidence: number;
  confidenceScale: number;
  confidenceBias: number;
  recommendedThreshold: number;
};

export type LearningDatasets = [PrefixPriorWriteRow[], PositionPrefixPriorWriteRow[], CorridorPriorWriteRow[], TransitionPriorWriteRow[], PositionTransitionPriorWriteRow[], EdgePriorWriteRow[], MotifPriorWriteRow[]];
