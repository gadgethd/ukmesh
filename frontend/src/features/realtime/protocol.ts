export type WSReadyState = 'connecting' | 'connected' | 'disconnected';
export type WSConnectionState = {
  readyState: WSReadyState;
  lastMessageAt: number | null;
};

export interface WSMessage {
  type: 'packet' | 'node_update' | 'node_upsert' | 'initial_state' | 'link_update';
  data: unknown;
  ts: number;
  scopeEpoch?: number;
}
