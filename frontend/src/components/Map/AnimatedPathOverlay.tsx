import React, { useEffect, useMemo, useRef } from 'react';
import type { Layer, PickingInfo } from '@deck.gl/core';
import { ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { MapboxOverlay } from '@deck.gl/mapbox';
import type maplibregl from 'maplibre-gl';
import {
  PATH_ARC_BLOOM_WIDTH,
  PATH_ARC_CORE_WIDTH,
  PATH_ARC_HEIGHT,
  PATH_HOP_ANIMATION_MS,
  PATH_LINE_FADE_MS,
  PATH_LINE_TTL_MS,
  pathArcColors,
} from './pathArcStyle.js';

export type AerialPathNode = {
  position: [number, number];
  nodeId?: string;
  name?: string;
  isObserver?: boolean;
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

type RenderedSegment = AerialPathSegment & {
  renderedTarget: [number, number];
  opacity: number;
};

type RenderedNode = AerialPathNode & {
  confidence: number | null;
  opacity: number;
};

type LeadingPulse = {
  position: [number, number];
  confidence: number | null;
};

export type PathRegistryEntry = {
  signature: string;
  segments: AerialPathSegment[];
  startedAt: number;
};

export function buildAerialPathSegments(paths: AerialPath[]): AerialPathSegment[] {
  return paths.flatMap((path) => path.nodes.slice(0, -1).flatMap((source, index) => {
    const target = path.nodes[index + 1];
    if (!target) return [];
    return [{ id: `${path.id}:${index}`, source, target, confidence: path.confidence }];
  }));
}

function aerialPathSignature(path: AerialPath): string {
  return [
    path.confidence ?? 'unknown',
    ...path.nodes.map((node) => `${node.position[0].toFixed(6)},${node.position[1].toFixed(6)}`),
  ].join(':');
}

/** Add or update incoming paths without removing paths from earlier packets. */
export function registerAerialPaths(
  registry: Map<string, PathRegistryEntry>,
  paths: AerialPath[],
  now: number,
): void {
  for (const path of paths) {
    const signature = aerialPathSignature(path);
    const existing = registry.get(path.id);
    if (existing?.signature === signature) continue;
    const segments = buildAerialPathSegments([path]);
    if (segments.length > 0) registry.set(path.id, { signature, segments, startedAt: now });
  }
}

function interpolate(
  source: [number, number],
  target: [number, number],
  progress: number,
): [number, number] {
  return [
    source[0] + (target[0] - source[0]) * progress,
    source[1] + (target[1] - source[1]) * progress,
  ];
}

function uniqueNodes(segments: RenderedSegment[]): RenderedNode[] {
  const nodesByKey = new Map<string, RenderedNode>();
  for (const segment of segments) {
    for (const node of [segment.source, segment.target]) {
      const key = `${node.nodeId ?? ''}:${node.position[0]}:${node.position[1]}`;
      const existing = nodesByKey.get(key);
      if (existing && existing.opacity >= segment.opacity) continue;
      nodesByKey.set(key, {
        ...node,
        confidence: segment.confidence,
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
  onNodeClick?: (node: AerialPathNode) => void,
): Layer[] {
  const layers: Layer[] = [];
  if (segments.length > 0) {
    layers.push(
      new ArcLayer<RenderedSegment>({
        id: 'resolved-path-arc-bloom',
        data: segments,
        getSourcePosition: (segment) => segment.source.position,
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
        getSourcePosition: (segment) => segment.source.position,
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
      getPosition: (node) => node.position,
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

  if (pulses.length > 0) {
    layers.push(new ScatterplotLayer<LeadingPulse>({
      id: 'resolved-path-leading-pulse',
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
  active: boolean;
  onNodeClick?: (node: AerialPathNode) => void;
}> = ({ map, paths, active, onNodeClick }) => {
  const overlayRef = useRef<MapboxOverlay | null>(null);
  const registryRef = useRef(new Map<string, PathRegistryEntry>());
  const frameRef = useRef<number | null>(null);
  const wakeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const renderRef = useRef<(now: number) => void>(() => {});
  const scheduleRef = useRef<() => void>(() => {});
  const activeRef = useRef(active);
  const onNodeClickRef = useRef(onNodeClick);
  activeRef.current = active;
  onNodeClickRef.current = onNodeClick;
  const signature = useMemo(() => paths.map((path) => [
    path.id,
    path.confidence ?? 'unknown',
    ...path.nodes.map((node) => `${node.position[0].toFixed(6)},${node.position[1].toFixed(6)}`),
  ].join(':')).join('|'), [paths]);

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
    let needsAnimationFrame = false;
    let nextFadeAt = Number.POSITIVE_INFINITY;

    for (const [pathId, entry] of registryRef.current) {
      const animationDuration = entry.segments.length * PATH_HOP_ANIMATION_MS;
      const elapsed = Math.max(0, now - entry.startedAt);
      if (elapsed < animationDuration) {
        needsAnimationFrame = true;
        const completedCount = Math.floor(elapsed / PATH_HOP_ANIMATION_MS);
        const activeSegment = entry.segments[completedCount];
        for (const segment of entry.segments.slice(0, completedCount)) {
          rendered.push({ ...segment, renderedTarget: segment.target.position, opacity: 1 });
        }
        if (activeSegment) {
          const hopProgress = Math.min(
            1,
            (elapsed - completedCount * PATH_HOP_ANIMATION_MS) / PATH_HOP_ANIMATION_MS,
          );
          const position = interpolate(activeSegment.source.position, activeSegment.target.position, hopProgress);
          rendered.push({ ...activeSegment, renderedTarget: position, opacity: 1 });
          pulses.push({ position, confidence: activeSegment.confidence });
        }
        continue;
      }

      const ageSinceCompletion = elapsed - animationDuration;
      if (ageSinceCompletion >= PATH_LINE_TTL_MS + PATH_LINE_FADE_MS) {
        registryRef.current.delete(pathId);
        continue;
      }
      const opacity = ageSinceCompletion <= PATH_LINE_TTL_MS
        ? 1
        : Math.max(0, 1 - (ageSinceCompletion - PATH_LINE_TTL_MS) / PATH_LINE_FADE_MS);
      if (ageSinceCompletion > PATH_LINE_TTL_MS) needsAnimationFrame = true;
      else nextFadeAt = Math.min(nextFadeAt, entry.startedAt + animationDuration + PATH_LINE_TTL_MS);
      for (const segment of entry.segments) {
        rendered.push({ ...segment, renderedTarget: segment.target.position, opacity });
      }
    }

    overlay.setProps({
      layers: layersForFrame(rendered, uniqueNodes(rendered), pulses, onNodeClickRef.current),
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
    if (!overlay) return;
    if (!active) {
      registryRef.current.clear();
      if (frameRef.current != null) cancelAnimationFrame(frameRef.current);
      if (wakeTimerRef.current) clearTimeout(wakeTimerRef.current);
      frameRef.current = null;
      wakeTimerRef.current = null;
      overlay.setProps({ layers: [] });
      return;
    }

    registerAerialPaths(registryRef.current, paths, performance.now());
    scheduleRef.current();
  // `signature` is the stable semantic dependency for the path collection.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, map, signature]);

  return null;
};
