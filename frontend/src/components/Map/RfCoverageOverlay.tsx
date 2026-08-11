import { useEffect, useMemo } from 'react';
import type * as maplibregl from 'maplibre-gl';
import {
  isValidRfCoverageTile,
  type RfCoverageMeta,
  type RfCoverageTierName,
} from '../../hooks/useRfCoverage.js';
import {
  maxRfRasterZoom,
  registerRfRasterDataset,
} from './rfCoverageRasterProtocol.js';

const SOURCE_ID = 'hopreach-rf-source';
const LAYER_ID = 'hopreach-rf-layer';

function removeRfLayers(map: maplibregl.Map): void {
  if (map.getLayer(LAYER_ID)) map.removeLayer(LAYER_ID);
  if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID);
}

export function rfCoverageTileUrl(image: string, revision: string): string {
  const encodedPath = image.split('/').map(encodeURIComponent).join('/');
  return `/rf-coverage/${encodedPath}?revision=${encodeURIComponent(revision)}`;
}

export function RfCoverageOverlay({
  map,
  meta,
  tier,
  nodePublicKey,
  visible,
}: {
  map: maplibregl.Map | null;
  meta: RfCoverageMeta | null;
  tier: RfCoverageTierName;
  nodePublicKey?: string | null;
  visible: boolean;
}) {
  const nodeEntry = nodePublicKey ? meta?.node_coverage?.[nodePublicKey.toLowerCase()] : undefined;
  const product = nodePublicKey ? nodeEntry?.standard : meta?.coverage?.[tier];
  const tiles = useMemo(
    () => (product?.tiles ?? []).filter(isValidRfCoverageTile),
    [product?.tiles],
  );
  const revision = nodeEntry
    ? `${nodeEntry.dataset_id}-${nodeEntry.updated_at}-${nodeEntry.completed_tiles ?? tiles.length}`
    : `${meta?.run?.id ?? meta?.generated_at ?? 'unknown'}-${meta?.run?.tiers?.[tier]?.completed_tiles ?? tiles.length}`;
  const datasetKind = nodePublicKey ? `node:${nodePublicKey.toLowerCase()}` : tier;
  const signature = `${visible}|${datasetKind}|${revision}|${tiles.map((tile) => `${tile.image}:${JSON.stringify(tile.bounds)}`).join('|')}`;

  useEffect(() => {
    if (!map) return undefined;
    let active = true;
    let retryFrame: number | null = null;
    let releaseDataset: (() => void) | null = null;

    const render = () => {
      if (!active || !map.isStyleLoaded()) return;
      removeRfLayers(map);
      releaseDataset?.();
      releaseDataset = null;
      if (!visible || tiles.length === 0) return;

      const beforeId = map.getLayer('map-labels-water')
        ? 'map-labels-water'
        : map.getLayer('privacy-rings-layer')
          ? 'privacy-rings-layer'
          : undefined;

      const dataset = registerRfRasterDataset(
        tiles.map((tile) => ({
          url: rfCoverageTileUrl(tile.image, revision),
          bounds: tile.bounds,
        })),
        maxRfRasterZoom(nodePublicKey ? 'standard' : tier),
      );
      releaseDataset = dataset.release;
      map.addSource(SOURCE_ID, {
        type: 'raster',
        tiles: [dataset.tileTemplate],
        tileSize: 256,
        minzoom: 0,
        maxzoom: maxRfRasterZoom(nodePublicKey ? 'standard' : tier),
        bounds: dataset.bounds,
      });
      map.addLayer({
        id: LAYER_ID,
        type: 'raster',
        source: SOURCE_ID,
        paint: {
          'raster-opacity': 0.72,
          'raster-resampling': 'nearest',
          'raster-fade-duration': 0,
        },
      }, beforeId);
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
      releaseDataset?.();
    };
  // signature deliberately captures content changes without depending on
  // unstable array/object identities returned by each metadata poll.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [map, signature]);

  return null;
}
