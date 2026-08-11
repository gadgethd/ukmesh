import React, { useEffect, useMemo, useRef } from 'react';
import { WebMercatorViewport, type Layer, type PickingInfo } from '@deck.gl/core';
import { ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { MapboxOverlay } from '@deck.gl/mapbox';
import type maplibregl from 'maplibre-gl';
import { TERRAIN_CONFIG } from './mapConfig.js';
import {
  PATH_ARC_BLOOM_WIDTH,
  PATH_ARC_CORE_WIDTH,
  PATH_ARC_HEIGHT_M,
  PATH_ARC_SEGMENTS,
  PATH_HOP_ANIMATION_MS,
  PATH_LINE_FADE_MS,
  PATH_LINE_TTL_MS,
  PATH_TERRAIN_CLEARANCE_M,
  packetPathColors,
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
  packetHash?: string | null;
  confidence: number | null;
  nodes: AerialPathNode[];
};

export type AerialPathSegment = {
  id: string;
  packetHash: string;
  source: AerialPathNode;
  target: AerialPathNode;
  confidence: number | null;
};

export type DeckPosition = [number, number, number];

type RenderedSegment = AerialPathSegment & {
  sourcePosition: DeckPosition;
  targetPosition: DeckPosition;
  renderedTarget: DeckPosition;
  arcHeight: number;
  opacity: number;
};

type RenderedNode = AerialPathNode & {
  packetHash: string;
  confidence: number | null;
  renderedPosition: DeckPosition;
  opacity: number;
};

type RenderedObserverNode = AerialPathNode & {
  renderedPosition: DeckPosition;
};

type LeadingPulse = {
  position: DeckPosition;
  packetHash: string;
  confidence: number | null;
};

export type TerrainProfileSample = {
  progress: number;
  position: DeckPosition;
};

export type TerrainElevationMap = Pick<maplibregl.Map, 'queryTerrainElevation'> & {
  isSourceLoaded?: (sourceId: string) => boolean;
  // MapLibre exposes the terrain-relative query publicly but keeps the
  // exaggerated map-centre elevation on its internal transform.
  transform?: { elevation?: number };
};

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
      packetHash: path.packetHash ?? path.id,
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
    // MapLibre returns a flat/zero DEM while the source is still loading. Do
    // not turn that transient value into a cached z=0 endpoint: idle retries
    // will revisit the coordinate after the terrain tile is ready.
    if (map.isSourceLoaded && !map.isSourceLoaded('terrain-dem')) {
      cache.set(key, null);
      return null;
    }
    const queried = map.queryTerrainElevation(position);
    if (typeof queried === 'number' && Number.isFinite(queried)) {
      const mapCentreElevation = map.transform?.elevation;
      elevation = queried + (
        typeof mapCentreElevation === 'number' && Number.isFinite(mapCentreElevation)
          ? mapCentreElevation
          : 0
      );
    }
  } catch {
    // A terrain tile may not be ready yet. The idle listener retries null
    // samples; terrain-enabled paths stay hidden until their anchors resolve.
  }
  cache.set(key, elevation);
  return elevation;
}

export function terrainAwarePosition(
  position: [number, number],
  elevation: number | null,
  terrainEnabled: boolean,
  terrainExaggeration = TERRAIN_CONFIG.exaggeration,
): DeckPosition | null {
  if (!terrainEnabled) {
    return [position[0], position[1], 0];
  }
  if (elevation == null || !Number.isFinite(elevation)) return null;
  return [
    position[0],
    position[1],
    elevation + PATH_TERRAIN_CLEARANCE_M * terrainExaggeration,
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

function arcWorldElevation(
  sourceWorld: DeckPosition,
  targetWorld: DeckPosition,
  ratio: number,
  height: number,
): number {
  const distance = Math.hypot(
    targetWorld[0] - sourceWorld[0],
    targetWorld[1] - sourceWorld[1],
  );
  const heightDistance = distance * height;
  const deltaZ = targetWorld[2] - sourceWorld[2];
  if (heightDistance === 0) return sourceWorld[2] + deltaZ * ratio;
  const unitZ = deltaZ / heightDistance;
  const paraboloidWidth = unitZ * unitZ + 1;
  const reversed = deltaZ <= 0;
  const arcRatio = reversed ? 1 - ratio : ratio;
  const baseZ = reversed ? targetWorld[2] : sourceWorld[2];
  return Math.sqrt(Math.max(0, arcRatio * (paraboloidWidth - arcRatio)))
    * heightDistance + baseZ;
}

/**
 * Convert a fixed physical lift and the terrain profile into ArcLayer's
 * length-relative getHeight multiplier. The binary search raises only the
 * segments whose sampled DEM crest would otherwise intersect the paraboloid.
 */
export function arcHeightMultiplier(
  source: DeckPosition,
  target: DeckPosition,
  terrainSamples: readonly TerrainProfileSample[] = [],
): number {
  const sourceWorld = ARC_PROJECTION.projectPosition(source) as DeckPosition;
  const targetWorld = ARC_PROJECTION.projectPosition(target) as DeckPosition;
  const distance = Math.hypot(
    targetWorld[0] - sourceWorld[0],
    targetWorld[1] - sourceWorld[1],
  );
  if (distance === 0) return 0;

  const midpointLatitude = (source[1] + target[1]) / 2;
  const baseHeightWorld = ARC_PROJECTION.projectPosition([
    source[0],
    midpointLatitude,
    PATH_ARC_HEIGHT_M,
  ])[2];
  let height = Math.max(0, baseHeightWorld / distance);

  for (const sample of terrainSamples) {
    const ratio = Math.max(0, Math.min(1, sample.progress));
    if (ratio === 0 || ratio === 1) continue;
    const requiredWorld = ARC_PROJECTION.projectPosition(sample.position)[2];
    if (arcWorldElevation(sourceWorld, targetWorld, ratio, height) >= requiredWorld) continue;

    let low = height;
    let high = Math.max(height, 0.001);
    while (arcWorldElevation(sourceWorld, targetWorld, ratio, high) < requiredWorld) {
      high *= 2;
    }
    for (let iteration = 0; iteration < 24; iteration += 1) {
      const middle = (low + high) / 2;
      if (arcWorldElevation(sourceWorld, targetWorld, ratio, middle) < requiredWorld) low = middle;
      else high = middle;
    }
    height = high;
  }
  return height;
}

/** Sample the same projected paraboloid used by Deck.gl's ArcLayer shader. */
export function interpolateArcPosition(
  source: DeckPosition,
  target: DeckPosition,
  progress: number,
  height = arcHeightMultiplier(source, target),
): DeckPosition {
  const ratio = Math.max(0, Math.min(1, progress));
  if (ratio === 0) return source;
  if (ratio === 1) return target;
  const sourceWorld = ARC_PROJECTION.projectPosition(source);
  const targetWorld = ARC_PROJECTION.projectPosition(target);
  return ARC_PROJECTION.unprojectPosition([
    sourceWorld[0] + (targetWorld[0] - sourceWorld[0]) * ratio,
    sourceWorld[1] + (targetWorld[1] - sourceWorld[1]) * ratio,
    arcWorldElevation(sourceWorld, targetWorld, ratio, height),
  ]) as DeckPosition;
}

/** Return the ground coordinate at the same projected XY ratio as ArcLayer. */
export function arcGroundPosition(
  source: [number, number],
  target: [number, number],
  progress: number,
): [number, number] {
  const ratio = Math.max(0, Math.min(1, progress));
  const sourceWorld = ARC_PROJECTION.projectPosition([...source, 0]);
  const targetWorld = ARC_PROJECTION.projectPosition([...target, 0]);
  return ARC_PROJECTION.unprojectPosition([
    sourceWorld[0] + (targetWorld[0] - sourceWorld[0]) * ratio,
    sourceWorld[1] + (targetWorld[1] - sourceWorld[1]) * ratio,
    0,
  ]).slice(0, 2) as [number, number];
}

function arcTerrainCoordinates(
  source: [number, number],
  target: [number, number],
): [number, number][] {
  return Array.from({ length: PATH_ARC_SEGMENTS }, (_, index) => arcGroundPosition(
    source,
    target,
    easeArcProgress(index / (PATH_ARC_SEGMENTS - 1)),
  ));
}

function terrainProfileForSegment(
  source: AerialPathNode,
  target: AerialPathNode,
  map: TerrainElevationMap | null,
  terrainEnabled: boolean,
  elevationCache: Map<string, number | null>,
): TerrainProfileSample[] | null {
  if (!terrainEnabled) return [];
  const samples: TerrainProfileSample[] = [];
  for (let index = 1; index < PATH_ARC_SEGMENTS - 1; index += 1) {
    const progress = easeArcProgress(index / (PATH_ARC_SEGMENTS - 1));
    const position = arcGroundPosition(source.position, target.position, progress);
    const elevation = cachedTerrainElevation(map, position, elevationCache, true);
    const terrainPosition = terrainAwarePosition(position, elevation, true);
    if (!terrainPosition) return null;
    samples.push({ progress, position: terrainPosition });
  }
  return samples;
}

export function renderedPosition(
  node: AerialPathNode,
  map: TerrainElevationMap | null,
  terrainEnabled: boolean,
  elevationCache: Map<string, number | null>,
  anchorCache: Map<string, DeckPosition | null>,
): DeckPosition | null {
  const key = terrainCoordinateKey(node.position);
  if (anchorCache.has(key)) return anchorCache.get(key) ?? null;
  const rendered = terrainAwarePosition(
    node.position,
    cachedTerrainElevation(map, node.position, elevationCache, terrainEnabled),
    terrainEnabled,
  );
  // Arc endpoints, path node dots, observer dots, and the hop marker all use
  // this one coordinate anchor. This prevents a line from ending at a second
  // terrain query that is numerically close but not the icon's actual anchor.
  anchorCache.set(key, rendered);
  return rendered;
}

type SegmentGeometry = {
  sourcePosition: DeckPosition;
  targetPosition: DeckPosition;
  renderedTarget: DeckPosition;
  arcHeight: number;
};

function segmentGeometry(
  segment: AerialPathSegment,
  map: TerrainElevationMap | null,
  terrainEnabled: boolean,
  elevationCache: Map<string, number | null>,
  anchorCache: Map<string, DeckPosition | null>,
): SegmentGeometry | null {
  const sourcePosition = renderedPosition(
    segment.source,
    map,
    terrainEnabled,
    elevationCache,
    anchorCache,
  );
  const targetPosition = renderedPosition(
    segment.target,
    map,
    terrainEnabled,
    elevationCache,
    anchorCache,
  );
  if (!sourcePosition || !targetPosition) return null;
  const terrainSamples = terrainProfileForSegment(
    segment.source,
    segment.target,
    map,
    terrainEnabled,
    elevationCache,
  );
  if (!terrainSamples) return null;
  const arcHeight = arcHeightMultiplier(sourcePosition, targetPosition, terrainSamples);
  return {
    sourcePosition,
    targetPosition,
    renderedTarget: targetPosition,
    arcHeight,
  };
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
        packetHash: segment.packetHash,
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
        id: 'resolved-path-arc-bloom-terrain-clearance-v2',
        data: segments,
        getSourcePosition: (segment) => segment.sourcePosition,
        getTargetPosition: (segment) => segment.renderedTarget,
        getSourceColor: (segment) => packetPathColors(segment.packetHash, segment.opacity).bloomSource,
        getTargetColor: (segment) => packetPathColors(segment.packetHash, segment.opacity).bloomTarget,
        getWidth: PATH_ARC_BLOOM_WIDTH,
        getHeight: (segment) => segment.arcHeight,
        numSegments: PATH_ARC_SEGMENTS,
        pickable: false,
      }),
      new ArcLayer<RenderedSegment>({
        id: 'resolved-path-arc-core-terrain-clearance-v2',
        data: segments,
        getSourcePosition: (segment) => segment.sourcePosition,
        getTargetPosition: (segment) => segment.renderedTarget,
        getSourceColor: (segment) => packetPathColors(segment.packetHash, segment.opacity).coreSource,
        getTargetColor: (segment) => packetPathColors(segment.packetHash, segment.opacity).coreTarget,
        getWidth: PATH_ARC_CORE_WIDTH,
        getHeight: (segment) => segment.arcHeight,
        numSegments: PATH_ARC_SEGMENTS,
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
      getLineColor: (node) => packetPathColors(node.packetHash, node.opacity).coreTarget,
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
      getFillColor: (item) => packetPathColors(item.packetHash).coreTarget,
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
    path.packetHash ?? path.id,
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
    const terrainMap = map as TerrainElevationMap | null;
    const anchorCache = new Map<string, DeckPosition | null>();
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
        const geometry = segmentGeometry(
          activeSegment,
          terrainMap,
          terrainEnabledNow,
          elevationCacheRef.current,
          anchorCache,
        );
        if (!geometry) continue;
        const position = interpolateArcPosition(
          geometry.sourcePosition,
          geometry.targetPosition,
          hopProgress,
          geometry.arcHeight,
        );
        rendered.push({
          ...activeSegment,
          ...geometry,
          opacity: 1,
        });
        pulses.push({
          position,
          packetHash: activeSegment.packetHash,
          confidence: activeSegment.confidence,
        });
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
      const geometry = segmentGeometry(
        segment,
        terrainMap,
        terrainEnabledNow,
        elevationCacheRef.current,
        anchorCache,
      );
      if (!geometry) continue;
      rendered.push({
        ...segment,
        ...geometry,
        opacity,
      });
    }

    const renderedObserverNodes: RenderedObserverNode[] = observerNodesRef.current.flatMap((node) => {
      const renderedPositionForNode = renderedPosition(
        node,
        terrainMap,
        terrainEnabledNow,
        elevationCacheRef.current,
        anchorCache,
      );
      return renderedPositionForNode ? [{ ...node, renderedPosition: renderedPositionForNode }] : [];
    });

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
      for (let index = 0; index < path.nodes.length - 1; index += 1) {
        const source = path.nodes[index];
        const target = path.nodes[index + 1];
        if (!source || !target) continue;
        for (const position of arcTerrainCoordinates(source.position, target.position)) {
          rememberCoordinate(position);
        }
      }
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
        if (elevationCacheRef.current.has(key) && elevationCacheRef.current.get(key) !== null) continue;
        elevationCacheRef.current.delete(key);
        const elevation = cachedTerrainElevation(
          map as TerrainElevationMap,
          position,
          elevationCacheRef.current,
          true,
        );
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
