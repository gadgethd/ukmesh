import type * as maplibre from 'maplibre-gl';
import { nodeStore } from '../../features/realtime/nodeStore.js';
import { linkStateStore } from '../../features/realtime/linkStore.js';
import { useOverlayStore } from '../../store/overlayStore.js';
import { PACKET_OBSERVED_EVENT } from '../../utils/appEvents.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import type { ClashComputation } from '../../features/map/model.js';
import { EMPTY_FC, MAP_ARC_REFRESH_INTERVAL_MS, MAP_REFRESH_INTERVAL_MS } from './mapConfig.js';
import {
  ALL_MAP_SOURCE_DIRTY_FLAGS, buildHiddenMask, buildLinksGeoJSON, buildNodeGeoJSON,
  buildPrivacyRingsGeoJSON, computeClashData, mergeMapSourceDirtyFlags,
  nextNodePresenceTransition, nodeMapNeedsSourceRefresh, type MapSourceDirtyFlags,
} from './geojsonBuilders.js';

type View = {
  showLinks: boolean; showClients: boolean; showClashes: boolean; maxClashHops: number;
  light: boolean; focusedNodeId: string | null; focusedPrefixNodeIds: Set<string> | null;
};
type Options = {
  getMap: () => maplibre.Map | null;
  getView: () => View;
  onSelectedNodeChange: () => void;
  refreshPlannedLinks: () => void;
};

/** Owns live-store subscriptions, native source invalidation and bounded refresh deadlines. */
export function createLiveMapSources(options: Options) {
  let nodes = nodeStore.getState().nodes;
  let storeScope = nodeStore.getState().scopeKey;
  let links = linkStateStore.getState();
  let pathNodeIds = useOverlayStore.getState().pathNodeIds;
  let replayNodeIds = useOverlayStore.getState().replayNodeIds;
  let mask = new Map<string, HiddenMaskGeometry>();
  let dirty = { ...ALL_MAP_SOURCE_DIRTY_FLAGS };
  let refreshTimer: ReturnType<typeof setTimeout> | undefined;
  let presenceTimer: ReturnType<typeof setTimeout> | undefined;
  let lastArcActivity = 0;
  let clash: ClashComputation = { clashOffenderNodeIds: new Set(), clashRelayIds: new Set(), clashPathLines: [], clashModeActive: false };

  const refresh = () => {
    const map = options.getMap();
    if (!map) return;
    const changes = dirty;
    dirty = { nodes: false, privacy: false, links: false, clash: false, plannedLinks: false };
    const view = options.getView();
    if (changes.nodes || changes.privacy || changes.links || changes.clash || changes.plannedLinks) mask = buildHiddenMask(nodes);
    const clashDirty = changes.clash || changes.nodes || changes.links;
    if (clashDirty) clash = computeClashData(nodes, links.viablePairsArr, links.linkMetrics,
      view.showClashes, view.maxClashHops, view.focusedNodeId, view.focusedPrefixNodeIds);
    if (changes.nodes || clashDirty) {
      (map.getSource('nodes') as maplibre.GeoJSONSource | undefined)?.setData(buildNodeGeoJSON(nodes, mask,
        view.showClients, clash.clashOffenderNodeIds, clash.clashRelayIds, clash.clashModeActive,
        clash.clashModeActive ? null : pathNodeIds, replayNodeIds, Date.now()));
      clearTimeout(presenceTimer);
      const transition = nextNodePresenceTransition(nodes);
      presenceTimer = transition === null ? undefined : setTimeout(() => {
        presenceTimer = undefined;
        dirty = { ...ALL_MAP_SOURCE_DIRTY_FLAGS };
        refresh();
      }, Math.max(1, Math.min(2_147_483_647, transition - Date.now())));
    }
    if (changes.privacy || changes.nodes) {
      (map.getSource('privacy-rings') as maplibre.GeoJSONSource | undefined)?.setData(buildPrivacyRingsGeoJSON(nodes, mask));
    }
    if (changes.links || changes.nodes) {
      const data = view.showLinks ? buildLinksGeoJSON(nodes, links.viablePairsArr, links.linkMetrics,
        mask, view.light ? 'light' : 'dark') : EMPTY_FC;
      (map.getSource('viable-links') as maplibre.GeoJSONSource | undefined)?.setData(data);
      map.setLayoutProperty('viable-links-layer', 'visibility', view.showLinks ? 'visible' : 'none');
    }
    if (clashDirty) {
      useOverlayStore.getState().setClashPathLines(clash.clashModeActive ? clash.clashPathLines : []);
      (map.getSource('clash-lines') as maplibre.GeoJSONSource | undefined)?.setData(EMPTY_FC);
      map.setLayoutProperty('clash-lines-layer', 'visibility', 'none');
    }
    if (changes.plannedLinks || changes.nodes || changes.links) options.refreshPlannedLinks();
  };
  const schedule = (flags: Partial<MapSourceDirtyFlags> = ALL_MAP_SOURCE_DIRTY_FLAGS) => {
    dirty = mergeMapSourceDirtyFlags(dirty, flags);
    if (refreshTimer !== undefined) return;
    const interval = Date.now() - lastArcActivity < 5_000 ? MAP_ARC_REFRESH_INTERVAL_MS : MAP_REFRESH_INTERVAL_MS;
    refreshTimer = setTimeout(() => { refreshTimer = undefined; refresh(); }, interval);
  };
  const cancel = () => {
    clearTimeout(refreshTimer); refreshTimer = undefined;
    clearTimeout(presenceTimer); presenceTimer = undefined;
  };
  const reset = () => { dirty = { ...ALL_MAP_SOURCE_DIRTY_FLAGS }; refresh(); };
  const start = () => {
    nodes = nodeStore.getState().nodes;
    storeScope = nodeStore.getState().scopeKey;
    links = linkStateStore.getState();
    pathNodeIds = useOverlayStore.getState().pathNodeIds;
    replayNodeIds = useOverlayStore.getState().replayNodeIds;
    const unsubscribeNodes = nodeStore.subscribe(() => {
      const { nodes: next, nodeChanges, scopeKey } = nodeStore.getState();
      if (next === nodes) return;
      const changed = nodeChanges.full || nodeChanges.previous !== nodes || nodeMapNeedsSourceRefresh(nodes, next, Date.now(), nodeChanges.ids);
      nodes = next;
      if (scopeKey !== storeScope) {
        storeScope = scopeKey;
        reset();
      } else if (changed) schedule({ nodes: true, privacy: true, links: true, clash: true, plannedLinks: true });
      options.onSelectedNodeChange();
    });
    const unsubscribeLinks = linkStateStore.subscribe(() => {
      links = linkStateStore.getState();
      schedule({ links: true, clash: true, nodes: true, plannedLinks: true });
    });
    const unsubscribeOverlay = useOverlayStore.subscribe(state => {
      if (state.pathNodeIds === pathNodeIds && state.replayNodeIds === replayNodeIds) return;
      pathNodeIds = state.pathNodeIds; replayNodeIds = state.replayNodeIds;
      schedule({ nodes: true });
    });
    const packet = () => { lastArcActivity = Date.now(); };
    window.addEventListener(PACKET_OBSERVED_EVENT, packet);
    return () => {
      unsubscribeNodes(); unsubscribeLinks(); unsubscribeOverlay();
      window.removeEventListener(PACKET_OBSERVED_EVENT, packet);
      cancel();
    };
  };
  return { refresh, schedule, reset, cancel, start,
    getNodes: () => nodes, getMask: () => mask, getReplayNodes: () => replayNodeIds };
}
