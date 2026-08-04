import React, { useEffect, useMemo, useRef } from 'react';
import { WebMercatorViewport, type Layer, type PickingInfo } from '@deck.gl/core';
import { ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { MapboxOverlay } from '@deck.gl/mapbox';
import type maplibregl from 'maplibre-gl';
import { TERRAIN_CONFIG } from './mapConfig.js';
import {
  PATH_ARC_BLOOM_WIDTH,
  PATH_ARC_CORE_WIDTH,
  PATH_ARC_HEIGHT,
  PATH_HOP_ANIMATION_MS,
  PATH_LINE_FADE_MS,
  PATH_LINE_TTL_MS,
  PATH_TERRAIN_CLEARANCE_M,
  pathArcColors,
} from './pathArcStyle.js';

export type AerialPathNode = {
  position: [number, number];
  nodeId?: string;
  name?: string;
  isObserver?: boolean;
  confidence?: number | null;
};

export type AerialPath = {
  id: string;
  confidence: number | null;
  nodes: AerialPathNode[];
};

export type AerialPathSegment = {
  id: string;
  source: AerialPathNode;
  target: AerialPathNode;
  confidence: number | null;
};

export type DeckPosition = [number, number, number];

type RenderedSegment = AerialPathSegment & {
  sourcePosition: DeckPosition;
  targetPosition: DeckPosition;
  renderedTarget: DeckPosition;
  opacity: number;
};

type RenderedNode = AerialPathNode & {
  confidence: number | null;
  renderedPosition: DeckPosition;
  opacity: number;
};

type RenderedObserverNode = AerialPathNode & {
  renderedPosition: DeckPosition;
};

type LeadingPulse = {
  position: DeckPosition;
  confidence: number | null;
};

type TerrainElevationMap = Pick<maplibregl.Map, 'queryTerrainElevation'>;

export type PathRegistryEntry = {
  signature: string;
  segment: AerialPathSegment;
  startedAt: number;
};

const EMPTY_OBSERVER_NODES: AerialPathNode[] = [];

function aerialNodeKey(node: AerialPathNode): string {
  return `${node.position[0].toFixed(6)},${node.position[1].toFixed(6)}`;
}

export function aerialSegmentKey(
  pathId: string,
  source: AerialPathNode,
  target: AerialPathNode,
): string {
  return `${pathId}:stable-segment:${aerialNodeKey(source)}>${aerialNodeKey(target)}`;
}

export function buildAerialPathSegments(paths: AerialPath[]): AerialPathSegment[] {
  return paths.flatMap((path) => path.nodes.slice(0, -1).flatMap((source, index) => {
    const target = path.nodes[index + 1];
    if (!target) return [];
    return [{
      id: aerialSegmentKey(path.id, source, target),
      source,
      target,
      confidence: target.confidence ?? path.confidence,
    }];
  }));
}

function strongestConfidence(a: number | null, b: number | null): number | null {
  if (a == null) return b;
  if (b == null) return a;
  return Math.max(a, b);
}

/**
 * Register stable packet-scoped edges. Shared trunk edges are stored once, so
 * adding an observer updates their metadata without restarting animation/TTL;
 * new branch edges and different packet scopes receive independent entries.
 */
export function registerAerialPaths(
  registry: Map<string, PathRegistryEntry>,
  paths: AerialPath[],
  now: number,
): void {
  for (const path of paths) {
    let nextSegmentStart = now;
    for (const segment of buildAerialPathSegments([path])) {
      const existing = registry.get(segment.id);
      const previous = existing?.segment;
      const merged = previous
        ? { ...segment, confidence: strongestConfidence(previous.confidence, segment.confidence) }
        : segment;
      const signature = [
        aerialNodeKey(merged.source),
        aerialNodeKey(merged.target),
        merged.confidence ?? 'unknown',
      ].join(':');
      registry.set(segment.id, {
        signature,
        segment: merged,
        startedAt: existing?.startedAt ?? nextSegmentStart,
      });
      const completionAt = (existing?.startedAt ?? nextSegmentStart) + PATH_HOP_ANIMATION_MS;
      if (completionAt > now) nextSegmentStart = Math.max(nextSegmentStart, completionAt);
    }
  }
}

export function terrainCoordinateKey(position: [number, number]): string {
  return `${position[0].toFixed(6)},${position[1].toFixed(6)}`;
}

export function cachedTerrainElevation(
  map: TerrainElevationMap | null,
  position: [number, number],
  cache: Map<string, number | null>,
  terrainEnabled: boolean,
): number | null {
  if (!terrainEnabled || !map) return null;
  const key = terrainCoordinateKey(position);
  if (cache.has(key)) return cache.get(key) ?? null;

  let elevation: number | null = null;
  try {
    const queried = map.queryTerrainElevation(position);
    elevation = typeof queried === 'number' && Number.isFinite(queried) ? queried : null;
  } catch {
    // A terrain tile may not be ready yet. The idle listener retries null
    // samples; unavailable terrain keeps the pre-terrain z=0 behaviour.
  }
  cache.set(key, elevation);
  return elevation;
}

export function terrainAwarePosition(
  position: [number, number],
  elevation: number | null,
  terrainEnabled: boolean,
  terrainExaggeration = TERRAIN_CONFIG.exaggeration,
): DeckPosition {
  if (!terrainEnabled || elevation == null || !Number.isFinite(elevation)) {
    return [position[0], position[1], 0];
  }
  return [
    position[0],
    position[1],
    (elevation + PATH_TERRAIN_CLEARANCE_M) * terrainExaggeration,
  ];
}

const ARC_PROJECTION = new WebMercatorViewport({
  width: 1,
  height: 1,
  longitude: 0,
  latitude: 0,
  zoom: 0,
});

export function easeArcProgress(progress: number): number {
  const clamped = Math.max(0, Math.min(1, progress));
  return clamped * clamped * (3 - 2 * clamped);
}

/** Sample the same projected paraboloid used by Deck.gl's ArcLayer shader. */
export function interpolateArcPosition(
  source: DeckPosition,
  target: DeckPosition,
  progress: number,
  height = PATH_ARC_HEIGHT,
): DeckPosition {
  const ratio = Math.max(0, Math.min(1, progress));
  if (ratio === 0) return source;
  if (ratio === 1) return target;
  const sourceWorld = ARC_PROJECTION.projectPosition(source);
  const targetWorld = ARC_PROJECTION.projectPosition(target);
  const distance = Math.hypot(
    targetWorld[0] - sourceWorld[0],
    targetWorld[1] - sourceWorld[1],
  );
  const heightDistance = distance * height;
  const deltaZ = targetWorld[2] - sourceWorld[2];
  let z: number;
  if (heightDistance === 0) {
    z = sourceWorld[2] + deltaZ * ratio;
  } else {
    const unitZ = deltaZ / heightDistance;
    const paraboloidWidth = unitZ * unitZ + 1;
    const reversed = deltaZ <= 0;
    const arcRatio = reversed ? 1 - ratio : ratio;
    const baseZ = reversed ? targetWorld[2] : sourceWorld[2];
    z = Math.sqrt(Math.max(0, arcRatio * (paraboloidWidth - arcRatio)))
      * heightDistance + baseZ;
  }
  return ARC_PROJECTION.unprojectPosition([
    sourceWorld[0] + (targetWorld[0] - sourceWorld[0]) * ratio,
    sourceWorld[1] + (targetWorld[1] - sourceWorld[1]) * ratio,
    z,
  ]) as DeckPosition;
}

function renderedPosition(
  node: AerialPathNode,
  map: TerrainElevationMap | null,
  terrainEnabled: boolean,
  elevationCache: Map<string, number | null>,
): DeckPosition {
  return terrainAwarePosition(
    node.position,
    cachedTerrainElevation(map, node.position, elevationCache, terrainEnabled),
    terrainEnabled,
  );
}

function uniqueNodes(segments: RenderedSegment[]): RenderedNode[] {
  const nodesByKey = new Map<string, RenderedNode>();
  for (const segment of segments) {
    for (const [node, position] of [
      [segment.source, segment.sourcePosition],
      [segment.target, segment.targetPosition],
    ] as const) {
      const key = `${node.nodeId ?? ''}:${node.position[0]}:${node.position[1]}`;
      const existing = nodesByKey.get(key);
      if (existing && existing.opacity >= segment.opacity) continue;
      nodesByKey.set(key, {
        ...node,
        confidence: segment.confidence,
        renderedPosition: position,
        opacity: segment.opacity,
      });
    }
  }
  return [...nodesByKey.values()];
}

function layersForFrame(
  segments: RenderedSegment[],
  nodes: RenderedNode[],
  pulses: LeadingPulse[],
  observerNodes: RenderedObserverNode[],
  onNodeClick?: (node: AerialPathNode) => void,
): Layer[] {
  const layers: Layer[] = [];
  if (segments.length > 0) {
    layers.push(
      new ArcLayer<RenderedSegment>({
        id: 'resolved-path-arc-bloom',
        data: segments,
        getSourcePosition: (segment) => segment.sourcePosition,
        getTargetPosition: (segment) => segment.renderedTarget,
        getSourceColor: (segment) => pathArcColors(segment.confidence, segment.opacity).bloomSource,
        getTargetColor: (segment) => pathArcColors(segment.confidence, segment.opacity).bloomTarget,
        getWidth: PATH_ARC_BLOOM_WIDTH,
        getHeight: PATH_ARC_HEIGHT,
        pickable: false,
      }),
      new ArcLayer<RenderedSegment>({
        id: 'resolved-path-arc-core',
        data: segments,
        getSourcePosition: (segment) => segment.sourcePosition,
        getTargetPosition: (segment) => segment.renderedTarget,
        getSourceColor: (segment) => pathArcColors(segment.confidence, segment.opacity).coreSource,
        getTargetColor: (segment) => pathArcColors(segment.confidence, segment.opacity).coreTarget,
        getWidth: PATH_ARC_CORE_WIDTH,
        getHeight: PATH_ARC_HEIGHT,
        pickable: false,
      }),
    );
  }

  if (nodes.length > 0) {
    layers.push(new ScatterplotLayer<RenderedNode>({
      id: 'resolved-path-nodes',
      data: nodes,
      getPosition: (node) => node.renderedPosition,
      getFillColor: (node) => [11, 23, 37, Math.round(255 * node.opacity)],
      getLineColor: (node) => pathArcColors(node.confidence, node.opacity).coreTarget,
      getRadius: 6,
      radiusUnits: 'pixels',
      stroked: true,
      lineWidthUnits: 'pixels',
      getLineWidth: 2.5,
      pickable: Boolean(onNodeClick),
      onClick: ({ object }: PickingInfo<AerialPathNode>) => { if (object) onNodeClick?.(object); },
    }));
  }

  if (observerNodes.length > 0) {
    layers.push(new ScatterplotLayer<RenderedObserverNode>({
      id: 'resolved-path-observers',
      data: observerNodes,
      getPosition: (node) => node.renderedPosition,
      getFillColor: [59, 130, 246, 255],
      getLineColor: [255, 255, 255, 235],
      getRadius: 8,
      radiusUnits: 'pixels',
      stroked: true,
      lineWidthUnits: 'pixels',
      getLineWidth: 2,
      pickable: Boolean(onNodeClick),
      onClick: ({ object }: PickingInfo<AerialPathNode>) => { if (object) onNodeClick?.(object); },
    }));
  }

  if (pulses.length > 0) {
    layers.push(new ScatterplotLayer<LeadingPulse>({
      id: 'resolved-path-arc-rider',
      data: pulses,
      getPosition: (item) => item.position,
      getFillColor: (item) => pathArcColors(item.confidence).coreTarget,
      getLineColor: [255, 255, 255, 220],
      getRadius: 7,
      radiusUnits: 'pixels',
      stroked: true,
      lineWidthUnits: 'pixels',
      getLineWidth: 1.5,
      pickable: false,
    }));
  }
  return layers;
}

export const AnimatedPathOverlay: React.FC<{
  map: maplibregl.Map | null;
  paths: AerialPath[];
  observerNodes?: AerialPathNode[];
  active: boolean;
  terrainEnabled?: boolean;
  onNodeClick?: (node: AerialPathNode) => void;
}> = ({
  map,
  paths,
  observerNodes = EMPTY_OBSERVER_NODES,
  active,
  terrainEnabled = false,
  onNodeClick,
}) => {
  const overlayRef = useRef<MapboxOverlay | null>(null);
  const registryRef = useRef(new Map<string, PathRegistryEntry>());
  const elevationCacheRef = useRef(new Map<string, number | null>());
  const terrainCoordinatesRef = useRef(new Map<string, [number, number]>());
  const frameRef = useRef<number | null>(null);
  const wakeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const renderRef = useRef<(now: number) => void>(() => {});
  const scheduleRef = useRef<() => void>(() => {});
  const activeRef = useRef(active);
  const terrainEnabledRef = useRef(terrainEnabled);
  const onNodeClickRef = useRef(onNodeClick);
  const observerNodesRef = useRef(observerNodes);
  activeRef.current = active;
  terrainEnabledRef.current = terrainEnabled;
  onNodeClickRef.current = onNodeClick;
  observerNodesRef.current = observerNodes;
  const signature = useMemo(() => paths.map((path) => [
    path.id,
    path.confidence ?? 'unknown',
    ...path.nodes.map((node) => `${node.position[0].toFixed(6)},${node.position[1].toFixed(6)}`),
  ].join(':')).join('|'), [paths]);
  const featureSignature = useMemo(() => [
    signature,
    ...observerNodes
      .map((node) => `${node.nodeId ?? ''}:${aerialNodeKey(node)}`)
      .sort(),
  ].join('|'), [observerNodes, signature]);

  useEffect(() => {
    if (!map) return undefined;
    const overlay = new MapboxOverlay({ interleaved: false, layers: [] });
    map.addControl(overlay as unknown as maplibregl.IControl);
    overlayRef.current = overlay;
    return () => {
      if (frameRef.current != null) cancelAnimationFrame(frameRef.current);
      if (wakeTimerRef.current) clearTimeout(wakeTimerRef.current);
      frameRef.current = null;
      wakeTimerRef.current = null;
      registryRef.current.clear();
      elevationCacheRef.current.clear();
      terrainCoordinatesRef.current.clear();
      map.removeControl(overlay as unknown as maplibregl.IControl);
      overlayRef.current = null;
    };
  }, [map]);

  scheduleRef.current = () => {
    if (frameRef.current != null) cancelAnimationFrame(frameRef.current);
    if (wakeTimerRef.current) clearTimeout(wakeTimerRef.current);
    wakeTimerRef.current = null;
    frameRef.current = requestAnimationFrame((now) => {
      frameRef.current = null;
      renderRef.current(now);
    });
  };

  renderRef.current = (now: number) => {
    const overlay = overlayRef.current;
    if (!overlay || !activeRef.current) return;
    const rendered: RenderedSegment[] = [];
    const pulses: LeadingPulse[] = [];
    const terrainEnabledNow = terrainEnabledRef.current;
    const terrainMap = map;
    let needsAnimationFrame = false;
    let nextFadeAt = Number.POSITIVE_INFINITY;

    for (const [segmentId, entry] of registryRef.current) {
      const animationDuration = PATH_HOP_ANIMATION_MS;
      if (now < entry.startedAt) {
        needsAnimationFrame = true;
        continue;
      }
      const elapsed = now - entry.startedAt;
      if (elapsed < animationDuration) {
        needsAnimationFrame = true;
        const activeSegment = entry.segment;
        const hopProgress = easeArcProgress(elapsed / PATH_HOP_ANIMATION_MS);
        const sourcePosition = renderedPosition(
          activeSegment.source,
          terrainMap,
          terrainEnabledNow,
          elevationCacheRef.current,
        );
        const targetPosition = renderedPosition(
          activeSegment.target,
          terrainMap,
          terrainEnabledNow,
          elevationCacheRef.current,
        );
        const position = interpolateArcPosition(sourcePosition, targetPosition, hopProgress);
        rendered.push({
          ...activeSegment,
          sourcePosition,
          targetPosition,
          renderedTarget: targetPosition,
          opacity: 1,
        });
        pulses.push({ position, confidence: activeSegment.confidence });
        continue;
      }

      const ageSinceCompletion = elapsed - animationDuration;
      if (ageSinceCompletion >= PATH_LINE_TTL_MS + PATH_LINE_FADE_MS) {
        registryRef.current.delete(segmentId);
        continue;
      }
      const opacity = ageSinceCompletion <= PATH_LINE_TTL_MS
        ? 1
        : Math.max(0, 1 - (ageSinceCompletion - PATH_LINE_TTL_MS) / PATH_LINE_FADE_MS);
      if (ageSinceCompletion > PATH_LINE_TTL_MS) needsAnimationFrame = true;
      else nextFadeAt = Math.min(nextFadeAt, entry.startedAt + animationDuration + PATH_LINE_TTL_MS);
      const segment = entry.segment;
      const sourcePosition = renderedPosition(
        segment.source,
        terrainMap,
        terrainEnabledNow,
        elevationCacheRef.current,
      );
      const targetPosition = renderedPosition(
        segment.target,
        terrainMap,
        terrainEnabledNow,
        elevationCacheRef.current,
      );
      rendered.push({
        ...segment,
        sourcePosition,
        targetPosition,
        renderedTarget: targetPosition,
        opacity,
      });
    }

    const renderedObserverNodes: RenderedObserverNode[] = observerNodesRef.current.map((node) => ({
      ...node,
      renderedPosition: renderedPosition(
        node,
        terrainMap,
        terrainEnabledNow,
        elevationCacheRef.current,
      ),
    }));

    overlay.setProps({
      layers: layersForFrame(
        rendered,
        uniqueNodes(rendered),
        pulses,
        renderedObserverNodes,
        onNodeClickRef.current,
      ),
    });

    if (needsAnimationFrame) {
      frameRef.current = requestAnimationFrame((nextNow) => {
        frameRef.current = null;
        renderRef.current(nextNow);
      });
    } else if (Number.isFinite(nextFadeAt)) {
      wakeTimerRef.current = setTimeout(
        () => scheduleRef.current(),
        Math.max(0, nextFadeAt - performance.now()),
      );
    }
  };

  useEffect(() => {
    const overlay = overlayRef.current;
    const coordinates = new Map<string, [number, number]>();
    const rememberCoordinate = (position: [number, number]) => {
      coordinates.set(terrainCoordinateKey(position), position);
    };
    for (const path of paths) {
      for (const node of path.nodes) rememberCoordinate(node.position);
    }
    for (const node of observerNodes) rememberCoordinate(node.position);
    terrainCoordinatesRef.current = coordinates;
    elevationCacheRef.current.clear();
    if (terrainEnabled && map) {
      for (const position of coordinates.values()) {
        cachedTerrainElevation(map, position, elevationCacheRef.current, true);
      }
    }

    const retryUnavailableTerrain = () => {
      if (!map || !terrainEnabledRef.current) return;
      let updated = false;
      for (const [key, position] of terrainCoordinatesRef.current) {
        if (elevationCacheRef.current.get(key) !== null) continue;
        elevationCacheRef.current.delete(key);
        const elevation = cachedTerrainElevation(map, position, elevationCacheRef.current, true);
        if (elevation !== null) updated = true;
      }
      if (updated) scheduleRef.current();
    };
    if (map && terrainEnabled) map.on('idle', retryUnavailableTerrain);

    if (!overlay) {
      return () => {
        if (map && terrainEnabled) map.off('idle', retryUnavailableTerrain);
      };
    }
    if (!active) {
      registryRef.current.clear();
      if (frameRef.current != null) cancelAnimationFrame(frameRef.current);
      if (wakeTimerRef.current) clearTimeout(wakeTimerRef.current);
      frameRef.current = null;
      wakeTimerRef.current = null;
      overlay.setProps({ layers: [] });
      return () => {
        if (map && terrainEnabled) map.off('idle', retryUnavailableTerrain);
      };
    }

    registerAerialPaths(registryRef.current, paths, performance.now());
    scheduleRef.current();
    return () => {
      if (map && terrainEnabled) map.off('idle', retryUnavailableTerrain);
    };
  // `signature` is the stable semantic dependency for the path collection.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, featureSignature, map, paths, terrainEnabled]);

  return null;
};
