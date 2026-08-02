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

type RenderedSegment = AerialPathSegment & { renderedTarget: [number, number] };

export function buildAerialPathSegments(paths: AerialPath[]): AerialPathSegment[] {
  return paths.flatMap((path) => path.nodes.slice(0, -1).flatMap((source, index) => {
    const target = path.nodes[index + 1];
    if (!target) return [];
    return [{ id: `${path.id}:${index}`, source, target, confidence: path.confidence }];
  }));
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

function uniqueNodes(segments: AerialPathSegment[]): AerialPathNode[] {
  const seen = new Set<string>();
  const nodes: AerialPathNode[] = [];
  for (const segment of segments) {
    for (const node of [segment.source, segment.target]) {
      const key = `${node.nodeId ?? ''}:${node.position[0]}:${node.position[1]}`;
      if (seen.has(key)) continue;
      seen.add(key);
      nodes.push(node);
    }
  }
  return nodes;
}

function layersForFrame(
  segments: RenderedSegment[],
  nodes: AerialPathNode[],
  pulse: { position: [number, number]; confidence: number | null } | null,
  opacity: number,
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
        getSourceColor: (segment) => pathArcColors(segment.confidence, opacity).bloomSource,
        getTargetColor: (segment) => pathArcColors(segment.confidence, opacity).bloomTarget,
        getWidth: PATH_ARC_BLOOM_WIDTH,
        getHeight: PATH_ARC_HEIGHT,
        pickable: false,
        updateTriggers: { getSourceColor: opacity, getTargetColor: opacity },
      }),
      new ArcLayer<RenderedSegment>({
        id: 'resolved-path-arc-core',
        data: segments,
        getSourcePosition: (segment) => segment.source.position,
        getTargetPosition: (segment) => segment.renderedTarget,
        getSourceColor: (segment) => pathArcColors(segment.confidence, opacity).coreSource,
        getTargetColor: (segment) => pathArcColors(segment.confidence, opacity).coreTarget,
        getWidth: PATH_ARC_CORE_WIDTH,
        getHeight: PATH_ARC_HEIGHT,
        pickable: false,
        updateTriggers: { getSourceColor: opacity, getTargetColor: opacity },
      }),
    );
  }

  if (nodes.length > 0) {
    const segmentByPosition = new Map<string, AerialPathSegment>();
    for (const segment of segments) {
      segmentByPosition.set(segment.source.position.join(','), segment);
      segmentByPosition.set(segment.target.position.join(','), segment);
    }
    layers.push(new ScatterplotLayer<AerialPathNode>({
      id: 'resolved-path-nodes',
      data: nodes,
      getPosition: (node) => node.position,
      getFillColor: [11, 23, 37, Math.round(255 * opacity)],
      getLineColor: (node) => {
        const segment = segmentByPosition.get(node.position.join(','));
        return pathArcColors(segment?.confidence ?? null, opacity).coreTarget;
      },
      getRadius: 6,
      radiusUnits: 'pixels',
      stroked: true,
      lineWidthUnits: 'pixels',
      getLineWidth: 2.5,
      pickable: Boolean(onNodeClick),
      onClick: ({ object }: PickingInfo<AerialPathNode>) => { if (object) onNodeClick?.(object); },
      updateTriggers: { getFillColor: opacity, getLineColor: opacity },
    }));
  }

  if (pulse) {
    layers.push(new ScatterplotLayer<typeof pulse>({
      id: 'resolved-path-leading-pulse',
      data: [pulse],
      getPosition: (item) => item.position,
      getFillColor: (item) => pathArcColors(item.confidence, opacity).coreTarget,
      getLineColor: [255, 255, 255, Math.round(220 * opacity)],
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
  const pathsRef = useRef(paths);
  pathsRef.current = paths;
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
      map.removeControl(overlay as unknown as maplibregl.IControl);
      overlayRef.current = null;
    };
  }, [map]);

  useEffect(() => {
    const overlay = overlayRef.current;
    if (!overlay) return undefined;
    let frameId: number | null = null;
    let ttlTimer: ReturnType<typeof setTimeout> | null = null;
    let cancelled = false;
    const segments = buildAerialPathSegments(pathsRef.current);
    if (!active || segments.length === 0) {
      overlay.setProps({ layers: [] });
      return undefined;
    }

    const startedAt = performance.now();
    const render = (now: number) => {
      if (cancelled) return;
      const elapsed = Math.max(0, now - startedAt);
      const completedCount = Math.min(segments.length, Math.floor(elapsed / PATH_HOP_ANIMATION_MS));
      const activeSegment = segments[completedCount];
      const hopProgress = activeSegment
        ? Math.min(1, (elapsed - completedCount * PATH_HOP_ANIMATION_MS) / PATH_HOP_ANIMATION_MS)
        : 1;
      const rendered: RenderedSegment[] = segments.slice(0, completedCount).map((segment) => ({
        ...segment,
        renderedTarget: segment.target.position,
      }));
      if (activeSegment) {
        rendered.push({
          ...activeSegment,
          renderedTarget: interpolate(activeSegment.source.position, activeSegment.target.position, hopProgress),
        });
      }
      const revealedSegments = segments.slice(0, Math.min(segments.length, completedCount + 1));
      const pulse = activeSegment
        ? { position: interpolate(activeSegment.source.position, activeSegment.target.position, hopProgress), confidence: activeSegment.confidence }
        : null;
      overlay.setProps({ layers: layersForFrame(rendered, uniqueNodes(revealedSegments), pulse, 1, onNodeClick) });

      if (completedCount < segments.length) {
        frameId = requestAnimationFrame(render);
        return;
      }

      const complete = segments.map((segment) => ({ ...segment, renderedTarget: segment.target.position }));
      const allNodes = uniqueNodes(segments);
      overlay.setProps({ layers: layersForFrame(complete, allNodes, null, 1, onNodeClick) });
      ttlTimer = setTimeout(() => {
        const fadeStartedAt = performance.now();
        const fade = (fadeNow: number) => {
          if (cancelled) return;
          const opacity = Math.max(0, 1 - (fadeNow - fadeStartedAt) / PATH_LINE_FADE_MS);
          overlay.setProps({ layers: layersForFrame(complete, allNodes, null, opacity, onNodeClick) });
          if (opacity > 0) frameId = requestAnimationFrame(fade);
          else overlay.setProps({ layers: [] });
        };
        frameId = requestAnimationFrame(fade);
      }, PATH_LINE_TTL_MS);
    };
    frameId = requestAnimationFrame(render);

    return () => {
      cancelled = true;
      if (frameId != null) cancelAnimationFrame(frameId);
      if (ttlTimer) clearTimeout(ttlTimer);
      overlay.setProps({ layers: [] });
    };
  }, [active, map, onNodeClick, signature]);

  return null;
};
