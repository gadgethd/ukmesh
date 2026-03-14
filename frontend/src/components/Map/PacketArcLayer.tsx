import React, { useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { ArcLayer } from '@deck.gl/layers';
import type { PacketArc } from '../../hooks/useNodes.js';

const ARC_TTL = 5000;

interface DeckViewState {
  longitude: number;
  latitude:  number;
  zoom:      number;
  pitch:     number;
  bearing:   number;
}

interface Props {
  arcs:     PacketArc[];
  showArcs: boolean;
  viewState: DeckViewState;
}

// Memoize layer creation to avoid rebuilding every frame
function useArcLayers(arcs: PacketArc[], showArcs: boolean): ArcLayer<PacketArc>[] {
  return useMemo(() => {
    if (!showArcs || arcs.length === 0) return [];
    
    const now = Date.now();
    const visible = arcs.filter((a) => now - a.ts < ARC_TTL);
    if (visible.length === 0) return [];

    const fade = (ts: number) => Math.max(0, 1 - (now - ts) / ARC_TTL);

    return [
      new ArcLayer<PacketArc>({
        id: 'arc-bloom',
        data: visible,
        getSourcePosition: (d) => d.from,
        getTargetPosition: (d) => d.to,
        getSourceColor:    (d) => [0, 196, 255, Math.round(35  * fade(d.ts))],
        getTargetColor:    (d) => [0, 196, 255, Math.round(70  * fade(d.ts))],
        getWidth: 10,
        getHeight: 0.15,
      }),
      new ArcLayer<PacketArc>({
        id: 'arc-core',
        data: visible,
        getSourcePosition: (d) => d.from,
        getTargetPosition: (d) => d.to,
        getSourceColor:    (d) => [120, 220, 255, Math.round(200 * fade(d.ts))],
        getTargetColor:    (d) => [200, 245, 255, Math.round(255 * fade(d.ts))],
        getWidth: 2,
        getHeight: 0.15,
      }),
    ];
  }, [arcs, showArcs]);
}

export const PacketArcLayer: React.FC<Props> = React.memo(({ arcs, showArcs, viewState }) => {
  const layers = useArcLayers(arcs, showArcs);

  if (layers.length === 0) return null;

  return (
    <DeckGL
      viewState={viewState}
      controller={false}
      layers={layers}
      style={{ position: 'absolute', top: '0', left: '0', right: '0', bottom: '0', pointerEvents: 'none', zIndex: '400' }}
    />
  );
});
