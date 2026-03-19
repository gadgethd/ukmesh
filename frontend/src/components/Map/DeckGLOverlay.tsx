/**
 * DeckGLOverlay — all GPU-rendered map overlays via @deck.gl/mapbox.
 *
 * Uses MapboxOverlay (works with MapLibre GL) to integrate deck.gl layers
 * directly into the MapLibre map. No separate WebGL canvas or viewport sync
 * needed — deck.gl automatically follows the MapLibre viewport.
 */
import React, { useEffect, useRef, useMemo } from 'react';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { ArcLayer, LineLayer, PathLayer } from '@deck.gl/layers';
import { PathStyleExtension } from '@deck.gl/extensions';
import type { PathStyleExtensionProps } from '@deck.gl/extensions';
import type { Layer } from '@deck.gl/core';
import type maplibregl from 'maplibre-gl';
import type { PacketArc } from '../../hooks/useNodes.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import { maskPoint } from '../../utils/pathing.js';

const ARC_TTL_MS = 5_000;
const FADE_DURATION_MS = 1_000;

type HistorySegment = {
  positions: [[number, number], [number, number]];
  count: number;
};

type HistorySegmentWithColor = HistorySegment & {
  color: [number, number, number, number];
  width: number;
};

interface Props {
  map: maplibregl.Map | null;

  // Live arc trails
  arcs: PacketArc[];
  showArcs: boolean;

  // Packet path history (link-segment heat map)
  packetHistorySegments: HistorySegment[];
  showPacketHistory: boolean;

  // Beta path overlays
  betaPaths: [number, number][][];
  betaLowSegments: [[number, number], [number, number]][];
  betaCompletionPaths: [number, number][][];
  showBetaPaths: boolean;
  pathFadingOut: boolean;

  hiddenCoordMask: Map<string, HiddenMaskGeometry>;
}

// Lat/lon [lat, lon] (Leaflet convention) → deck.gl [lon, lat] (GeoJSON convention)
function toXY(
  pt: [number, number],
  mask: Map<string, HiddenMaskGeometry>,
): [number, number] {
  const [lat, lon] = maskPoint(pt, mask);
  return [lon, lat];
}

// Shared PathStyleExtension instance for dashed paths — created once outside the component.
const DASH_EXT = [new PathStyleExtension({ dash: true, highPrecisionDash: true })];

function buildLayers(
  arcs: PacketArc[],
  showArcs: boolean,
  packetHistorySegments: HistorySegment[],
  showPacketHistory: boolean,
  betaPaths: [number, number][][],
  betaLowSegments: [[number, number], [number, number]][],
  betaCompletionPaths: [number, number][][],
  showBetaPaths: boolean,
  pathFadingOut: boolean,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
): Layer[] {
  const now = Date.now();
  const layers: Layer[] = [];

  // ── Arc trails ─────────────────────────────────────────────────────────────
  if (showArcs && arcs.length > 0) {
    const visible = arcs.filter((a) => now - a.ts < ARC_TTL_MS);
    if (visible.length > 0) {
      const fade = (ts: number) => Math.max(0, 1 - (now - ts) / ARC_TTL_MS);
      layers.push(
        new ArcLayer<PacketArc>({
          id: 'arc-bloom',
          data: visible,
          getSourcePosition: (d) => d.from,
          getTargetPosition: (d) => d.to,
          getSourceColor: (d) => [0, 196, 255, Math.round(35 * fade(d.ts))],
          getTargetColor: (d) => [0, 196, 255, Math.round(70 * fade(d.ts))],
          getWidth: 10,
          getHeight: 0.15,
        }),
        new ArcLayer<PacketArc>({
          id: 'arc-core',
          data: visible,
          getSourcePosition: (d) => d.from,
          getTargetPosition: (d) => d.to,
          getSourceColor: (d) => [120, 220, 255, Math.round(200 * fade(d.ts))],
          getTargetColor: (d) => [200, 245, 255, Math.round(255 * fade(d.ts))],
          getWidth: 2,
          getHeight: 0.15,
        }),
      );
    }
  }

  // ── Packet history heat map ────────────────────────────────────────────────
  if (showPacketHistory && packetHistorySegments.length > 0) {
    const historyWithColors: HistorySegmentWithColor[] = packetHistorySegments.map((d) => {
      const s = Math.max(1, d.count);
      const alpha = Math.min(0.82, 0.12 + Math.log10(s + 1) * 0.32);
      return {
        ...d,
        color: [168, 85, 247, Math.round(alpha * 255)] as [number, number, number, number],
        width: Math.min(6, 1.2 + Math.log2(s + 1) * 1.05),
      };
    });
    layers.push(
      new LineLayer<HistorySegmentWithColor>({
        id: 'packet-history',
        data: historyWithColors,
        getSourcePosition: (d) => toXY(d.positions[0], hiddenCoordMask),
        getTargetPosition: (d) => toXY(d.positions[1], hiddenCoordMask),
        getColor: (d) => d.color,
        getWidth: (d) => d.width,
        widthUnits: 'pixels',
        widthMinPixels: 1,
        pickable: false,
        updateTriggers: {
          getSourcePosition: hiddenCoordMask,
          getTargetPosition: hiddenCoordMask,
        },
      }),
    );
  }

  // ── Beta path overlays ─────────────────────────────────────────────────────
  if (showBetaPaths) {
    const targetOpacity = pathFadingOut ? 0 : 1;
    const opacityTransition = { duration: pathFadingOut ? FADE_DURATION_MS : 0 };

    if (betaLowSegments.length > 0) {
      layers.push(
        new PathLayer<[[number, number], [number, number]], PathStyleExtensionProps>({
          id: 'beta-low-segs',
          data: betaLowSegments,
          getPath: (d) => [toXY(d[0], hiddenCoordMask), toXY(d[1], hiddenCoordMask)],
          getColor: [239, 68, 68, 230],
          getWidth: 2.6,
          widthUnits: 'pixels',
          getDashArray: [6, 9],
          opacity: targetOpacity * 0.9,
          transitions: { opacity: opacityTransition },
          extensions: DASH_EXT,
          pickable: false,
          updateTriggers: { getPath: hiddenCoordMask },
        }),
      );
    }

    if (betaPaths.length > 0) {
      layers.push(
        new PathLayer<[number, number][], PathStyleExtensionProps>({
          id: 'beta-purple',
          data: betaPaths,
          getPath: (d) => d.map((pt) => toXY(pt, hiddenCoordMask)),
          getColor: [168, 85, 247, 255],
          getWidth: 2.8,
          widthUnits: 'pixels',
          getDashArray: [6, 9],
          opacity: targetOpacity * 0.75,
          transitions: { opacity: opacityTransition },
          extensions: DASH_EXT,
          pickable: false,
          updateTriggers: { getPath: hiddenCoordMask },
        }),
      );
    }

    if (betaCompletionPaths.length > 0) {
      layers.push(
        new PathLayer<[number, number][], PathStyleExtensionProps>({
          id: 'beta-completion',
          data: betaCompletionPaths,
          getPath: (d) => d.map((pt) => toXY(pt, hiddenCoordMask)),
          getColor: [239, 68, 68, 255],
          getWidth: 1.8,
          widthUnits: 'pixels',
          getDashArray: [4, 7],
          opacity: targetOpacity * 0.74,
          transitions: { opacity: opacityTransition },
          extensions: DASH_EXT,
          pickable: false,
          updateTriggers: { getPath: hiddenCoordMask },
        }),
      );
    }
  }

  return layers;
}

export const DeckGLOverlay: React.FC<Props> = ({
  map,
  arcs, showArcs,
  packetHistorySegments, showPacketHistory,
  betaPaths, betaLowSegments, betaCompletionPaths,
  showBetaPaths, pathFadingOut,
  hiddenCoordMask,
}) => {
  const overlayRef = useRef<MapboxOverlay | null>(null);

  // Create/destroy the MapboxOverlay when the map instance changes
  useEffect(() => {
    if (!map) return;

    const overlay = new MapboxOverlay({ interleaved: false, layers: [] });
    // MapboxOverlay implements IControl — addControl works with MapLibre GL
    map.addControl(overlay as unknown as maplibregl.IControl);
    overlayRef.current = overlay;

    return () => {
      map.removeControl(overlay as unknown as maplibregl.IControl);
      overlayRef.current = null;
    };
  }, [map]);

  // Recompute layers (useMemo keeps this off the render hot path)
  const layers = useMemo(
    () => buildLayers(
      arcs, showArcs,
      packetHistorySegments, showPacketHistory,
      betaPaths, betaLowSegments, betaCompletionPaths,
      showBetaPaths, pathFadingOut,
      hiddenCoordMask,
    ),
    [arcs, showArcs, packetHistorySegments, showPacketHistory,
      betaPaths, betaLowSegments, betaCompletionPaths,
      showBetaPaths, pathFadingOut, hiddenCoordMask],
  );

  // Push updated layers to the overlay imperatively
  useEffect(() => {
    overlayRef.current?.setProps({ layers });
  }, [layers]);

  // No DOM output — everything is rendered inside the MapLibre canvas
  return null;
};

// Keep DeckViewState export for backward compat (no longer used by App)
export type { Props as DeckGLOverlayProps };
