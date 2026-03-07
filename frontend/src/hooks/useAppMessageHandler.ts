import { useCallback } from 'react';
import type { WSMessage } from './useWebSocket.js';
import type { LivePacketData, MeshNode } from './useNodes.js';
import type { ViableLinkSnapshot } from './useLinkState.js';

type UseAppMessageHandlerParams = {
  handleInitialState: (data: {
    nodes: MeshNode[];
    packets: Array<{
      time: string;
      packet_hash: string;
      rx_node_id?: string;
      src_node_id?: string;
      packet_type?: number;
      hop_count?: number;
      payload?: Record<string, unknown>;
      advert_count?: number | null;
      path_hashes?: string[] | null;
    }>;
  }) => void;
  handlePacket: (data: LivePacketData) => void;
  handleNodeUpdate: (data: { nodeId: string; ts: number }) => void;
  handleNodeUpsert: (data: Partial<MeshNode> & { node_id: string }) => void;
  handleCoverageUpdate: (data: {
    node_id: string;
    geom: { type: string; coordinates: unknown };
    strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
  }) => void;
  applyInitialViablePairs: (pairs?: [string, string][]) => void;
  applyInitialViableLinks: (links?: ViableLinkSnapshot[]) => void;
  applyLinkUpdate: (update: {
    node_a_id: string;
    node_b_id: string;
    observed_count: number;
    itm_viable: boolean | null;
    itm_path_loss_db?: number | null;
    count_a_to_b?: number;
    count_b_to_a?: number;
  }) => void;
};

export function useAppMessageHandler({
  handleInitialState,
  handlePacket,
  handleNodeUpdate,
  handleNodeUpsert,
  handleCoverageUpdate,
  applyInitialViablePairs,
  applyInitialViableLinks,
  applyLinkUpdate,
}: UseAppMessageHandlerParams) {
  return useCallback((msg: WSMessage) => {
    if (msg.type === 'initial_state') {
      const data = msg.data as Parameters<typeof handleInitialState>[0] & {
        viable_pairs?: [string, string][];
        viable_links?: ViableLinkSnapshot[];
      };
      handleInitialState(data);
      if (data.viable_links && data.viable_links.length > 0) {
        applyInitialViableLinks(data.viable_links);
      } else {
        applyInitialViablePairs(data.viable_pairs);
      }
      return;
    }

    if (msg.type === 'packet') {
      handlePacket(msg.data as LivePacketData);
      return;
    }

    if (msg.type === 'node_update') {
      handleNodeUpdate(msg.data as { nodeId: string; ts: number });
      return;
    }

    if (msg.type === 'node_upsert') {
      handleNodeUpsert(msg.data as Partial<MeshNode> & { node_id: string });
      return;
    }

    if (msg.type === 'coverage_update') {
      handleCoverageUpdate(msg.data as {
        node_id: string;
        geom: { type: string; coordinates: unknown };
        strength_geoms?: Partial<Record<'green' | 'amber' | 'red', { type: string; coordinates: unknown }>>;
      });
      return;
    }

    if (msg.type === 'link_update') {
      applyLinkUpdate(msg.data as {
        node_a_id: string;
        node_b_id: string;
        observed_count: number;
        itm_viable: boolean | null;
        itm_path_loss_db?: number | null;
        count_a_to_b?: number;
        count_b_to_a?: number;
      });
    }
  }, [
    handleInitialState,
    handlePacket,
    handleNodeUpdate,
    handleNodeUpsert,
    handleCoverageUpdate,
    applyInitialViablePairs,
    applyInitialViableLinks,
    applyLinkUpdate,
  ]);
}
