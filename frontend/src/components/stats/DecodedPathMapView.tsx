import React, { useEffect } from 'react';
import type * as maplibregl from 'maplibre-gl';
import { AnimatedPathOverlay, type AerialPath } from '../Map/AnimatedPathOverlay.js';
import { OPENFREEMAP_STYLE_DARK } from '../Map/mapConfig.js';
import * as maplibre from '../Map/maplibre.js';
import 'maplibre-gl/dist/maplibre-gl.css';
import type { DecodedPathNode } from './StatsPrimitives.js';

const DecodedPathMapView: React.FC<{ nodes: DecodedPathNode[] }> = ({ nodes }) => {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const [mapInstance, setMapInstance] = React.useState<maplibregl.Map | null>(null);
  const paths = React.useMemo<AerialPath[]>(() => [{
    id: 'decoded-path',
    confidence: 1,
    nodes: nodes.map((node) => ({
      position: [Number(node.lon), Number(node.lat)],
      nodeId: node.node_id,
      name: node.name ?? undefined,
    })),
  }], [nodes]);
  const [failed, setFailed] = React.useState(false);
  useEffect(() => {
    if (!containerRef.current || nodes.length < 2) return undefined;
    let cancelled = false;
    setFailed(false);
    const map = new maplibre.Map({
      container: containerRef.current,
      style: OPENFREEMAP_STYLE_DARK,
      center: [Number(nodes[0]!.lon), Number(nodes[0]!.lat)],
      zoom: 8,
    });
    map.on('load', () => {
      if (cancelled) return;
      const bounds = new maplibre.LngLatBounds();
      for (const node of nodes) bounds.extend([Number(node.lon), Number(node.lat)]);
      map.fitBounds(bounds, { padding: 24, animate: false });
      map.setPitch(50);
      map.setBearing(-8);
      setMapInstance(map);
    });
    map.on('error', () => { if (!cancelled && !map.getStyle()) setFailed(true); });
    return () => { cancelled = true; map.remove(); };
  }, [nodes]);
  return (
    <div style={{ position: 'relative', height: '100%', width: '100%' }}>
      <div ref={containerRef} style={{ height: '100%', width: '100%' }} />
      {failed ? <p role="alert">The path map could not be loaded. Close it and try again.</p>
        : <AnimatedPathOverlay map={mapInstance} paths={paths} active />}
    </div>
  );
};

export default DecodedPathMapView;
