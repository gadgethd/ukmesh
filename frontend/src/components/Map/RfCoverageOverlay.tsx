import { useEffect, useMemo } from 'react';
import type maplibregl from 'maplibre-gl';
import {
  isValidRfCoverageTile,
  type RfCoverageMeta,
  type RfCoverageTierName,
} from '../../hooks/useRfCoverage.js';

const SOURCE_PREFIX = 'hopreach-rf-source-';
const LAYER_PREFIX = 'hopreach-rf-layer-';

function removeRfLayers(map: maplibregl.Map): void {
  const style = map.getStyle();
  for (const layer of [...(style.layers ?? [])].reverse()) {
    if (layer.id.startsWith(LAYER_PREFIX) && map.getLayer(layer.id)) map.removeLayer(layer.id);
  }
  for (const sourceId of Object.keys(style.sources ?? {})) {
    if (sourceId.startsWith(SOURCE_PREFIX) && map.getSource(sourceId)) map.removeSource(sourceId);
  }
}

export function rfCoverageTileUrl(image: string, revision: string): string {
  const encodedPath = image.split('/').map(encodeURIComponent).join('/');
  return `/rf-coverage/${encodedPath}?revision=${encodeURIComponent(revision)}`;
}

export function RfCoverageOverlay({
  map,
  meta,
  tier,
  visible,
}: {
  map: maplibregl.Map | null;
  meta: RfCoverageMeta | null;
  tier: RfCoverageTierName;
  visible: boolean;
}) {
  const product = meta?.coverage?.[tier];
  const tiles = useMemo(
    () => (product?.tiles ?? []).filter(isValidRfCoverageTile),
    [product?.tiles],
  );
  const revision = `${meta?.run?.id ?? meta?.generated_at ?? 'unknown'}-${meta?.run?.tiers?.[tier]?.completed_tiles ?? tiles.length}`;
  const signature = `${visible}|${tier}|${revision}|${tiles.map((tile) => `${tile.image}:${JSON.stringify(tile.bounds)}`).join('|')}`;

  useEffect(() => {
    if (!map) return undefined;
    let active = true;
    let retryFrame: number | null = null;

    const render = () => {
      if (!active || !map.isStyleLoaded()) return;
      removeRfLayers(map);
      if (!visible || tiles.length === 0) return;

      const beforeId = map.getLayer('map-labels-water')
        ? 'map-labels-water'
        : map.getLayer('privacy-rings-layer')
          ? 'privacy-rings-layer'
          : undefined;

      tiles.forEach((tile, index) => {
        const sourceId = `${SOURCE_PREFIX}${index}`;
        const layerId = `${LAYER_PREFIX}${index}`;
        map.addSource(sourceId, {
          type: 'image',
          url: rfCoverageTileUrl(tile.image, revision),
          coordinates: [
            [tile.bounds.West, tile.bounds.North],
            [tile.bounds.East, tile.bounds.North],
            [tile.bounds.East, tile.bounds.South],
            [tile.bounds.West, tile.bounds.South],
          ],
        });
        map.addLayer({
          id: layerId,
          type: 'raster',
          source: sourceId,
          paint: {
            'raster-opacity': 0.72,
            'raster-resampling': 'nearest',
            'raster-fade-duration': 0,
          },
        }, beforeId);
      });
    };

    // MapLibre can publish the map reference from inside its initial `load`
    // callback while isStyleLoaded() still reports false. Subscribing to
    // `load` at that point is too late: the current event will not invoke the
    // newly-added listener, leaving an already-enabled RF layer absent until
    // the user toggles it. Retry on the next frame, with `idle` as a fallback,
    // and continue to handle later theme/style replacements.
    const renderOnIdle = () => render();
    map.on('style.load', render);
    if (map.isStyleLoaded()) render();
    else {
      map.once('load', render);
      retryFrame = window.requestAnimationFrame(() => {
        retryFrame = null;
        if (map.isStyleLoaded()) render();
        else map.once('idle', renderOnIdle);
      });
    }

    return () => {
      active = false;
      if (retryFrame !== null) window.cancelAnimationFrame(retryFrame);
      map.off('load', render);
      map.off('idle', renderOnIdle);
      map.off('style.load', render);
      if (map.isStyleLoaded()) removeRfLayers(map);
    };
  // signature deliberately captures content changes without depending on
  // unstable array/object identities returned by each metadata poll.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [map, signature]);

  return null;
}
