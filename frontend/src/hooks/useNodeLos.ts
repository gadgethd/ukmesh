import { useCallback, useLayoutEffect, useRef } from 'react';
import { useCommittedRef } from './useCommittedRef.js';
import { nodeStore } from '../features/realtime/nodeStore.js';
import { useOverlayStore } from '../store/overlayStore.js';
import { fetchNodeLinks } from '../features/map/nodeLinks.js';
import { nodeLosProfiles } from '../features/map/los.js';
import { scopeKey, type PublicScope } from '../utils/scope.js';
import { TERRAIN_CONFIG } from '../config/terrain.js';
import { LINK_AMBER_THRESHOLD_DB } from '../features/map/linkPolicy.js';

export function useNodeLos(scope: PublicScope) {
  const key = scopeKey(scope);
  const scopeRef = useCommittedRef(scope);
  const jobs = useRef(new Map<string, { controller: AbortController; timer?: ReturnType<typeof setTimeout> }>());
  const clear = useCallback((nodeId: string) => {
    const job = jobs.current.get(nodeId);
    if (job) { job.controller.abort(); clearTimeout(job.timer); jobs.current.delete(nodeId); }
    useOverlayStore.getState().removeLosNode(nodeId);
  }, []);
  useLayoutEffect(() => () => {
    for (const nodeId of [...jobs.current.keys()]) clear(nodeId);
  }, [key, clear]);
  return useCallback(async (nodeId: string) => {
    if (useOverlayStore.getState().losNodeIds.has(nodeId)) { clear(nodeId); return; }
    clear(nodeId);
    const job = { controller: new AbortController(), timer: undefined as ReturnType<typeof setTimeout> | undefined };
    jobs.current.set(nodeId, job);
    useOverlayStore.getState().addLosLoading(nodeId);
    try {
      const links = await fetchNodeLinks(nodeId, scopeRef.current, job.controller.signal);
      if (job.controller.signal.aborted || jobs.current.get(nodeId) !== job) return;
      const nodes = nodeStore.getState().nodes;
      useOverlayStore.getState().setLosProfilesForNode(nodeId,
        nodeLosProfiles(nodes.get(nodeId), links, nodes, TERRAIN_CONFIG.exaggeration, LINK_AMBER_THRESHOLD_DB));
    } catch {
      if (job.controller.signal.aborted || jobs.current.get(nodeId) !== job) return;
      useOverlayStore.getState().setLosProfilesForNode(nodeId, []);
    }
    job.timer = setTimeout(() => clear(nodeId), 15_000);
  }, [clear]);
}
