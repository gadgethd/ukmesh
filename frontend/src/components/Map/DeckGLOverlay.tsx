/**
 * DeckGLOverlay — all GPU-rendered map overlays via @deck.gl/mapbox.
 *
 * Uses MapboxOverlay (works with MapLibre GL) to integrate deck.gl layers
 * directly into the MapLibre map. No separate WebGL canvas or viewport sync
 * needed — deck.gl automatically follows the MapLibre viewport.
 */
import React, { useEffect, useRef, useMemo } from 'react';
import { MapboxOverlay } from '@deck.gl/mapbox';
import { ArcLayer, LineLayer, PathLayer, ScatterplotLayer } from '@deck.gl/layers';
import { PathStyleExtension } from '@deck.gl/extensions';
import type { PathStyleExtensionProps } from '@deck.gl/extensions';
import type { Layer } from '@deck.gl/core';
import type maplibregl from 'maplibre-gl';
import type { PacketArc } from '../../hooks/useNodes.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import { maskPoint } from '../../utils/pathing.js';
import type { ClashPathLine, CustomLosPoint, CustomLosSegment, LosProfile } from './types.js';
import { TERRAIN_CONFIG } from './mapConfig.js';

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
  clashPathLines: ClashPathLine[];
  showBetaPaths: boolean;
  pathFadingOut: boolean;

  hiddenCoordMask: Map<string, HiddenMaskGeometry>;
  positionElevations: Map<string, number>;
  useTerrainElevation: boolean;
  losProfiles: LosProfile[] | null;
  customLosSegments: CustomLosSegment[];
  customLosStart: CustomLosPoint | null;
}

// Lat/lon [lat, lon] (Leaflet convention) → deck.gl [lon, lat] (GeoJSON convention)
const ANTENNA_H = 10;

function positionKey(lat: number, lon: number): string {
  return `${Math.round(lat * 1_000_000) / 1_000_000},${Math.round(lon * 1_000_000) / 1_000_000}`;
}

function toXYZ(
  pt: [number, number],
  mask: Map<string, HiddenMaskGeometry>,
  positionElevations: Map<string, number>,
  useTerrainElevation: boolean,
): [number, number, number] {
  const [rawLat, rawLon] = pt;
  const [lat, lon] = maskPoint(pt, mask);
  const elevation = useTerrainElevation
    ? (positionElevations.get(positionKey(rawLat, rawLon)) ?? positionElevations.get(positionKey(lat, lon)) ?? 0)
    : 0;
  const z = useTerrainElevation ? (elevation + ANTENNA_H) * TERRAIN_CONFIG.exaggeration : 0;
  return [lon, lat, z];
}

// Shared PathStyleExtension instance for dashed paths — created once outside the component.
const DASH_EXT = [new PathStyleExtension({ dash: true, highPrecisionDash: true })];

function observedPathColor(count: number, maxCount: number, alpha: number): [number, number, number, number] {
  if (maxCount <= 1) return [239, 68, 68, alpha];
  const normalized = Math.log(Math.max(1, count)) / Math.log(maxCount);
  if (normalized >= 2 / 3) return [34, 197, 94, alpha];
  if (normalized >= 1 / 3) return [251, 191, 36, alpha];
  return [239, 68, 68, alpha];
}

function buildLayers(
  arcs: PacketArc[],
  showArcs: boolean,
  packetHistorySegments: HistorySegment[],
  showPacketHistory: boolean,
  betaPaths: [number, number][][],
  betaLowSegments: [[number, number], [number, number]][],
  betaCompletionPaths: [number, number][][],
  clashPathLines: ClashPathLine[],
  positionElevations: Map<string, number>,
  useTerrainElevation: boolean,
  showBetaPaths: boolean,
  pathFadingOut: boolean,
  hiddenCoordMask: Map<string, HiddenMaskGeometry>,
  losProfiles: LosProfile[] | null,
  customLosSegments: CustomLosSegment[],
  customLosStart: CustomLosPoint | null,
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
    const maxCount = packetHistorySegments.reduce((max, segment) => Math.max(max, segment.count), 0);
    const historyWithColors: HistorySegmentWithColor[] = packetHistorySegments.map((d) => {
      const s = Math.max(1, d.count);
      const alpha = Math.min(0.55, 0.10 + Math.log10(s + 1) * 0.20);
      return {
        ...d,
        color: observedPathColor(s, maxCount, Math.round(alpha * 255)),
        width: Math.min(2.2, 0.8 + Math.log2(s + 1) * 0.45),
      };
    });
    layers.push(
      new LineLayer<HistorySegmentWithColor>({
        id: 'packet-history',
        data: historyWithColors,
        getSourcePosition: (d) => toXYZ(d.positions[0], hiddenCoordMask, positionElevations, useTerrainElevation),
        getTargetPosition: (d) => toXYZ(d.positions[1], hiddenCoordMask, positionElevations, useTerrainElevation),
        getColor: (d) => d.color,
        getWidth: (d) => d.width,
        widthUnits: 'pixels',
        widthMinPixels: 1,
        pickable: false,
        updateTriggers: {
          getSourcePosition: [hiddenCoordMask, positionElevations, useTerrainElevation],
          getTargetPosition: [hiddenCoordMask, positionElevations, useTerrainElevation],
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
          getPath: (d) => [toXYZ(d[0], hiddenCoordMask, positionElevations, useTerrainElevation), toXYZ(d[1], hiddenCoordMask, positionElevations, useTerrainElevation)],
          getColor: [239, 68, 68, 230],
          getWidth: 2.6,
          widthUnits: 'pixels',
          getDashArray: [6, 9],
          opacity: targetOpacity * 0.9,
          transitions: { opacity: opacityTransition },
          extensions: DASH_EXT,
          pickable: false,
          updateTriggers: { getPath: [hiddenCoordMask, positionElevations, useTerrainElevation] },
        }),
      );
    }

    if (betaPaths.length > 0) {
      layers.push(
        new PathLayer<[number, number][], PathStyleExtensionProps>({
          id: 'beta-purple',
          data: betaPaths,
          getPath: (d) => d.map((pt) => toXYZ(pt, hiddenCoordMask, positionElevations, useTerrainElevation)),
          getColor: [168, 85, 247, 255],
          getWidth: 2.8,
          widthUnits: 'pixels',
          getDashArray: [6, 9],
          opacity: targetOpacity * 0.75,
          transitions: { opacity: opacityTransition },
          extensions: DASH_EXT,
          pickable: false,
          updateTriggers: { getPath: [hiddenCoordMask, positionElevations, useTerrainElevation] },
        }),
      );
    }

    if (betaCompletionPaths.length > 0) {
      layers.push(
        new PathLayer<[number, number][], PathStyleExtensionProps>({
          id: 'beta-completion',
          data: betaCompletionPaths,
          getPath: (d) => d.map((pt) => toXYZ(pt, hiddenCoordMask, positionElevations, useTerrainElevation)),
          getColor: [239, 68, 68, 255],
          getWidth: 1.8,
          widthUnits: 'pixels',
          getDashArray: [4, 7],
          opacity: targetOpacity * 0.74,
          transitions: { opacity: opacityTransition },
          extensions: DASH_EXT,
          pickable: false,
          updateTriggers: { getPath: [hiddenCoordMask, positionElevations, useTerrainElevation] },
        }),
      );
    }
  }

  // ── Hex clash paths ───────────────────────────────────────────────────────
  if (clashPathLines.length > 0) {
    layers.push(
      new PathLayer<ClashPathLine>({
        id: 'hex-clash-lines',
        data: clashPathLines,
        getPath: (d) => d.positions.map((pt) => toXYZ(pt, hiddenCoordMask, positionElevations, useTerrainElevation)),
        getColor: [249, 115, 22, 230],
        getWidth: 2.2,
        widthUnits: 'pixels',
        pickable: false,
        updateTriggers: { getPath: [hiddenCoordMask, positionElevations, useTerrainElevation] },
      }),
    );
  }

  // ── LOS terrain profiles ───────────────────────────────────────────────────
  if (losProfiles && losProfiles.length > 0) {
    type LosEndpoint = { position: [number, number, number]; viable: boolean };
    const losEndpoints: LosEndpoint[] = losProfiles.flatMap((p) => [
      { position: p.profile[0] as [number, number, number], viable: p.itm_viable },
      { position: p.profile[p.profile.length - 1] as [number, number, number], viable: p.itm_viable },
    ]);

    layers.push(
      // Outer glow — wide + transparent for tube feel
      new PathLayer<LosProfile>({
        id: 'los-bloom',
        data: losProfiles,
        getPath: (d) => d.profile,
        getColor: (d) => d.itm_viable ? [34, 197, 94, 55] : [239, 68, 68, 55],
        getWidth: 14,
        widthUnits: 'pixels',
        pickable: false,
      }),
      // Bright core line
      new PathLayer<LosProfile>({
        id: 'los-core',
        data: losProfiles,
        getPath: (d) => d.profile,
        getColor: (d) => d.itm_viable ? [34, 197, 94, 230] : [239, 68, 68, 230],
        getWidth: 4,
        widthUnits: 'pixels',
        pickable: false,
      }),
      // Endpoint markers — rendered last so they appear above the lines
      new ScatterplotLayer<LosEndpoint>({
        id: 'los-endpoints',
        data: losEndpoints,
        getPosition: (d) => d.position,
        getFillColor: (d) => d.viable ? [34, 197, 94, 255] : [239, 68, 68, 255],
        getLineColor: [255, 255, 255, 200],
        getRadius: 7,
        radiusUnits: 'pixels',
        stroked: true,
        lineWidthUnits: 'pixels',
        getLineWidth: 2,
        pickable: false,
      }),
    );
  }

  // ── Custom LOS segments ────────────────────────────────────────────────────
  if (customLosSegments.length > 0) {
    layers.push(
      // Glow layer
      new PathLayer<CustomLosSegment>({
        id: 'custom-los-bloom',
        data: customLosSegments,
        getPath: (d) => d.path,
        getColor: (d) => d.obstructed ? [239, 68, 68, 50] : [34, 197, 94, 50],
        getWidth: 14,
        widthUnits: 'pixels',
        pickable: false,
      }),
      // Core line
      new PathLayer<CustomLosSegment>({
        id: 'custom-los-core',
        data: customLosSegments,
        getPath: (d) => d.path,
        getColor: (d) => d.obstructed ? [239, 68, 68, 230] : [34, 197, 94, 230],
        getWidth: 4,
        widthUnits: 'pixels',
        pickable: false,
      }),
    );
  }

  // ── Custom LOS start marker (waiting for second point) ─────────────────────
  if (customLosStart) {
    const EXAG = TERRAIN_CONFIG.exaggeration;
    layers.push(
      new ScatterplotLayer<CustomLosPoint>({
        id: 'custom-los-start',
        data: [customLosStart],
        getPosition: (d) => [d.lon, d.lat, (d.elevation_m + ANTENNA_H) * EXAG],
        getFillColor: [59, 130, 246, 255],
        getLineColor: [255, 255, 255, 200],
        getRadius: 9,
        radiusUnits: 'pixels',
        stroked: true,
        lineWidthUnits: 'pixels',
        getLineWidth: 2,
        pickable: false,
      }),
    );
  }

  return layers;
}

export const DeckGLOverlay: React.FC<Props> = ({
  map,
  arcs, showArcs,
  packetHistorySegments, showPacketHistory,
  betaPaths, betaLowSegments, betaCompletionPaths, clashPathLines,
  showBetaPaths, pathFadingOut,
  hiddenCoordMask,
  positionElevations,
  useTerrainElevation,
  losProfiles,
  customLosSegments,
  customLosStart,
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
      betaPaths, betaLowSegments, betaCompletionPaths, clashPathLines,
      positionElevations, useTerrainElevation,
      showBetaPaths, pathFadingOut,
      hiddenCoordMask,
      losProfiles,
      customLosSegments,
      customLosStart,
    ),
    [arcs, showArcs, packetHistorySegments, showPacketHistory,
      betaPaths, betaLowSegments, betaCompletionPaths, clashPathLines,
      showBetaPaths, pathFadingOut, hiddenCoordMask, positionElevations, useTerrainElevation, losProfiles,
      customLosSegments, customLosStart],
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
