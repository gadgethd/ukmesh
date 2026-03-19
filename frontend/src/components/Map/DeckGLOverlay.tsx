/**
 * DeckGLOverlay — single WebGL canvas for all GPU-rendered map overlays.
 *
 * Consolidates packet arc trails, packet history link-segments, and beta path
 * overlays into one DeckGL instance to avoid multiple WebGL contexts and to
 * keep all rendering off the SVG/DOM layer.
 *
 * Replaces PacketArcLayer.tsx and the Leaflet Pane/Polyline overlays that
 * previously lived in MapView (packet history, beta paths).
 */
import React, { useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { ArcLayer, LineLayer, PathLayer } from '@deck.gl/layers';
import { PathStyleExtension } from '@deck.gl/extensions';
import type { PathStyleExtensionProps } from '@deck.gl/extensions';
import type { PacketArc } from '../../hooks/useNodes.js';
import type { HiddenMaskGeometry } from '../../utils/pathing.js';
import { maskPoint } from '../../utils/pathing.js';

const ARC_TTL_MS      = 5_000;
const FADE_DURATION_MS = 1_000;

export interface DeckViewState {
  longitude: number;
  latitude:  number;
  zoom:      number;
  pitch:     number;
  bearing:   number;
}

type HistorySegment = {
  positions: [[number, number], [number, number]];
  count:     number;
};

type HistorySegmentWithColor = HistorySegment & {
  color: [number, number, number, number];
  width: number;
};

interface Props {
  // Live arc trails
  arcs:             PacketArc[];
  showArcs:         boolean;

  // Packet path history (link-segment heat map)
  packetHistorySegments: HistorySegment[];
  showPacketHistory:     boolean;

  // Beta path overlays
  betaPaths:           [number, number][][];
  betaLowSegments:     [[number, number], [number, number]][];
  betaCompletionPaths: [number, number][][];
  showBetaPaths:       boolean;
  /** When true, opacity transitions to 0 (deck.gl handles the interpolation). */
  pathFadingOut:       boolean;

  viewState:       DeckViewState;
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

function useDeckLayers(
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
) {
  return useMemo(() => {
    const now    = Date.now();
    const layers = [];

    // ── Arc trails ───────────────────────────────────────────────────────────
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
            getSourceColor: (d) => [0,   196, 255, Math.round(35  * fade(d.ts))],
            getTargetColor: (d) => [0,   196, 255, Math.round(70  * fade(d.ts))],
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

    // ── Packet history heat map (replaces up to 700 SVG Polylines) ───────────
    if (showPacketHistory && packetHistorySegments.length > 0) {
      // Pre-compute colour and width once per useMemo update instead of per segment per frame
      const historyWithColors: HistorySegmentWithColor[] = packetHistorySegments.map((d) => {
        const s     = Math.max(1, d.count);
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

    // ── Beta path overlays (replaces Leaflet Pane with SVG Polylines) ────────
    if (showBetaPaths) {
      // Opacity fades smoothly to 0 when pathFadingOut; deck.gl interpolates the
      // uniform between renders so we only need two React state changes (not 60/s rAF).
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
  }, [
    arcs, showArcs,
    packetHistorySegments, showPacketHistory,
    betaPaths, betaLowSegments, betaCompletionPaths,
    showBetaPaths, pathFadingOut,
    hiddenCoordMask,
  ]);
}

export const DeckGLOverlay: React.FC<Props> = React.memo(({
  arcs, showArcs,
  packetHistorySegments, showPacketHistory,
  betaPaths, betaLowSegments, betaCompletionPaths,
  showBetaPaths, pathFadingOut,
  viewState, hiddenCoordMask,
}) => {
  const layers = useDeckLayers(
    arcs, showArcs,
    packetHistorySegments, showPacketHistory,
    betaPaths, betaLowSegments, betaCompletionPaths,
    showBetaPaths, pathFadingOut,
    hiddenCoordMask,
  );

  if (layers.length === 0) return null;

  return (
    <DeckGL
      viewState={viewState}
      controller={false}
      layers={layers}
      style={{
        position: 'absolute',
        top: '0', left: '0', right: '0', bottom: '0',
        pointerEvents: 'none',
        zIndex: '400',
      }}
    />
  );
});
