import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import * as maplibre from './maplibre.js';
import { useOverlayStore } from '../../store/overlayStore.js';
import type { MeshNode } from '../../types/domain.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import { hasCoords, maskNodePoint } from '../../utils/pathing.js';
import type { LosProfile, PlannedRepeater } from '../../features/map/model.js';
import { sampleElevationAt } from '../../utils/terrainSampler.js';
import { TERRAIN_CONFIG } from '../../config/terrain.js';
import { EMPTY_FC } from './mapConfig.js';
import { buildPlannedCoverageGeoJSON, buildPlannedLinksGeoJSON, buildPlannedPinGeoJSON } from './geojsonBuilders.js';
import { PlannedRepeaterPopup } from './PlannedRepeaterPopup.js';
import { useCommittedRef } from '../../hooks/useCommittedRef.js';

type Context = {
  enabled: boolean;
  getMap: () => maplibre.Map | null;
  getNodes: () => ReadonlyMap<string, MeshNode>;
  getMask: () => Map<string, HiddenMaskGeometry>;
  isLight: () => boolean;
  linksVisible: () => boolean;
  onSelectNode: (nodeId: string | null) => void;
};

/** Dormant rollback capability. It never creates a map or starts coverage work. */
export function useLegacyPlanner(context: Context) {
  const current = useCommittedRef(context);
  const plans = useOverlayStore(state => state.plannedRepeaters);
  const requestedCoordinates = useOverlayStore(state => state.requestedPlanCoordinates);
  const applied = useRef(new Set<string>());
  const lifetime = useRef(0);
  const popup = useRef<maplibre.Popup | null>(null);
  const [container] = useState(() => document.createElement('div'));
  const [selection, setSelection] = useState<{ planId: string; lngLat: maplibre.LngLatLike } | null>(null);
  const selectedPlan = context.enabled ? plans.find(plan => plan.id === selection?.planId) ?? null : null;
  const selectedId = selection?.planId;
  const losActive = useOverlayStore(state => selectedId != null && state.losNodeIds.has(selectedId));
  const losLoading = useOverlayStore(state => selectedId != null && state.losLoadingIds.has(selectedId));
  useEffect(() => { lifetime.current++; return () => { lifetime.current++; }; }, []);

  const updateLinks = useCallback(() => {
    const state = current.current;
    const map = state.getMap();
    if (!map) return;
    const visible = state.enabled && state.linksVisible();
    const data = visible ? buildPlannedLinksGeoJSON(useOverlayStore.getState().plannedRepeaters,
      state.getNodes(), state.getMask(), state.isLight() ? 'light' : 'dark') : EMPTY_FC;
    (map.getSource('planned-links') as maplibre.GeoJSONSource | undefined)?.setData(data);
    if (map.getLayer('planned-links-layer')) map.setLayoutProperty('planned-links-layer', 'visibility', visible ? 'visible' : 'none');
  }, []);
  const rehydrate = useCallback((map: maplibre.Map) => {
    const visible = current.current.enabled ? useOverlayStore.getState().plannedRepeaters : [];
    (map.getSource('planned-coverage') as maplibre.GeoJSONSource | undefined)?.setData(buildPlannedCoverageGeoJSON(visible));
    (map.getSource('planned-pins') as maplibre.GeoJSONSource | undefined)?.setData(buildPlannedPinGeoJSON(visible));
    updateLinks();
  }, [updateLinks]);
  const open = useCallback((planId: string, lngLat: maplibre.LngLatLike) => {
    if (!current.current.enabled) return;
    current.current.onSelectNode(null);
    setSelection({ planId, lngLat });
  }, []);
  const place = useCallback(async (lat: number, lon: number) => {
    void lat; void lon;
    // The retired viewshed planner retains its no-op restore/placement contract
    // until its rollback window ends. Live RF coverage uses HopReach instead.
  }, []);
  const canPlace = useCallback(() => useOverlayStore.getState().plannedRepeaters.length < 5, []);
  const remove = useCallback((planId: string) => {
    setSelection(previous => previous?.planId === planId ? null : previous);
    applied.current.delete(planId);
    useOverlayStore.getState().removeLosNode(planId);
    useOverlayStore.getState().removePlannedRepeater(planId);
  }, []);
  const profiles = useCallback(async (plan: PlannedRepeater): Promise<LosProfile[]> => {
    if (!current.current.enabled || !plan.coverage?.predicted_links?.length) return [];
    const elevation = await sampleElevationAt(plan.lon, plan.lat).catch(() => 0);
    const state = current.current;
    const result: LosProfile[] = [];
    for (const link of plan.coverage.predicted_links) {
      const peer = state.getNodes().get(link.peer_id);
      if (!hasCoords(peer)) continue;
      const [lat, lon] = maskNodePoint(peer, state.getMask());
      result.push({ peer_id: link.peer_id, peer_name: link.peer_name, itm_path_loss_db: link.itm_path_loss_db,
        itm_viable: link.itm_viable, profile: [
          [plan.lon, plan.lat, ((elevation ?? 0) + 10) * TERRAIN_CONFIG.exaggeration],
          [lon, lat, ((peer.elevation_m ?? 0) + 10) * TERRAIN_CONFIG.exaggeration],
        ] });
    }
    return result;
  }, []);
  const toggleLos = useCallback(async (planId: string) => {
    if (!current.current.enabled) return;
    if (useOverlayStore.getState().losNodeIds.has(planId)) { useOverlayStore.getState().removeLosNode(planId); return; }
    const plan = useOverlayStore.getState().plannedRepeaters.find(item => item.id === planId);
    if (!plan) return;
    useOverlayStore.getState().addLosLoading(planId);
    const result = await profiles(plan);
    if (current.current.enabled && useOverlayStore.getState().plannedRepeaters.includes(plan)) {
      useOverlayStore.getState().setLosProfilesForNode(planId, result);
    }
  }, [profiles]);
  useEffect(() => {
    if (!context.enabled) return;
    const token = lifetime.current;
    const ids = new Set(plans.map(plan => plan.id));
    for (const id of applied.current) if (!ids.has(id)) {
      applied.current.delete(id); useOverlayStore.getState().removeLosNode(id);
    }
    for (const plan of plans) {
      if (plan.status !== 'ready' || applied.current.has(plan.id)) continue;
      applied.current.add(plan.id);
      useOverlayStore.getState().addLosLoading(plan.id);
      void profiles(plan).then(result => {
        if (lifetime.current === token && current.current.enabled
          && useOverlayStore.getState().plannedRepeaters.some(item => item.id === plan.id)) {
          useOverlayStore.getState().setLosProfilesForNode(plan.id, result);
        }
      });
    }
  }, [context.enabled, plans, profiles]);
  useEffect(() => {
    if (!context.enabled || requestedCoordinates.length === 0) return;
    for (const coordinate of requestedCoordinates.slice(0, Math.max(0, 5 - plans.length))) void place(coordinate.lat, coordinate.lon);
    useOverlayStore.getState().clearPlanRestoreRequest();
  }, [context.enabled, requestedCoordinates, plans.length, place]);
  useEffect(() => {
    const map = current.current.getMap();
    if (map) rehydrate(map);
    if (context.enabled) return;
    useOverlayStore.getState().setPlanRepeaterMode(false);
    setSelection(null);
    popup.current?.remove();
    for (const plan of useOverlayStore.getState().plannedRepeaters) remove(plan.id);
    applied.current.clear();
  }, [context.enabled, plans, rehydrate, remove]);
  useEffect(() => {
    const map = current.current.getMap();
    if (!map) return;
    if (!selection) { popup.current?.remove(); return; }
    if (!popup.current) {
      popup.current = new maplibre.Popup({ maxWidth: '280px', closeOnClick: false }).setDOMContent(container);
      popup.current.on('close', () => setSelection(null));
    }
    popup.current.setLngLat(selection.lngLat).addTo(map);
  }, [selection, container]);
  useEffect(() => { if (selection && !selectedPlan) setSelection(null); }, [selection, selectedPlan]);
  useEffect(() => () => {
    popup.current?.remove();
    for (const id of applied.current) useOverlayStore.getState().removeLosNode(id);
    applied.current.clear();
  }, []);
  const portal = context.enabled && selection && selectedPlan ? createPortal(<PlannedRepeaterPopup
    planId={selectedPlan.id} lat={selectedPlan.lat} lon={selectedPlan.lon} status={selectedPlan.status}
    links={selectedPlan.coverage?.predicted_links} getPeerName={id => current.current.getNodes().get(id)?.name ?? id.slice(0, 8)}
    onRemove={remove} losActive={losActive} losLoading={losLoading} onToggleLos={toggleLos} />, container) : null;
  return useMemo(() => ({ updateLinks, rehydrate, open, place, canPlace, portal }), [updateLinks, rehydrate, open, place, canPlace, portal]);
}
