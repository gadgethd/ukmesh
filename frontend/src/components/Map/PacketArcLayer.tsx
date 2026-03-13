import React, { useEffect, useRef, useState } from 'react';
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

function buildArcLayers(arcs: PacketArc[], now: number, show: boolean) {
  if (!show) return [];
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
}

export const PacketArcLayer: React.FC<Props> = ({ arcs, showArcs, viewState }) => {
  const [now, setNow] = useState<number>(() => Date.now());
  const [isPageVisible, setIsPageVisible] = useState(
    () => (typeof document === 'undefined' ? true : document.visibilityState === 'visible'),
  );
  const rafRef = useRef<ReturnType<typeof requestAnimationFrame>>(0);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const updateVisibility = () => setIsPageVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', updateVisibility);
    return () => document.removeEventListener('visibilitychange', updateVisibility);
  }, []);

  useEffect(() => {
    const hasVisibleArcs = showArcs && arcs.some((arc) => Date.now() - arc.ts < ARC_TTL);
    if (!isPageVisible || !hasVisibleArcs) {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      return undefined;
    }
    const tick = () => {
      setNow(Date.now());
      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
  }, [arcs, isPageVisible, showArcs]);

  const layers = buildArcLayers(arcs, now, showArcs);

  return (
    <DeckGL
      viewState={viewState}
      controller={false}
      layers={layers}
      style={{ position: 'absolute', top: '0', left: '0', right: '0', bottom: '0', pointerEvents: 'none', zIndex: '400' }}
    />
  );
};
