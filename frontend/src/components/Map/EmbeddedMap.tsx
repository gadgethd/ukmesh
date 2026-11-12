import { useEffect, useRef, useState } from 'react';
import * as maplibre from './maplibre.js';
import {
  basemapWatchdogDelay, createBasemapHealth, decideBasemapAction, isSuccessfulBasemapTile,
  noteFallback, noteRetryStarted, noteSourceData, noteStyleLoaded, noteTileError,
  refreshBasemapMetadataUrl, BASEMAP_VECTOR_SOURCE_ID,
} from './basemapHealth.js';
import { buildFallbackStyle, installFallbackMarkers, loadFallbackCities } from './basemapFallback.js';
import { MAP_INITIAL_LOAD_TIMEOUT_MS, OPENFREEMAP_STYLE_DARK } from './mapConfig.js';
import 'maplibre-gl/dist/maplibre-gl.css';
import './embedded-map.css';
import './basemap-labels.css';

type Props = {
  initialView: { lat: number; lon: number; zoom: number };
  scopeKey: string;
  onMapReady: (map: maplibre.Map | null) => void;
};

/** Native basemap lifecycle for public overlays. Data and selection belong to
 * each overlay; this base has no dashboard stores, layers or recurring refresh. */
export function EmbeddedMap({ initialView, scopeKey, onMapReady }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibre.Map | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [notice, setNotice] = useState<string | null>(null);
  const [blocked, setBlocked] = useState(false);
  useEffect(() => {
    if (!container.current) return;
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let resizeFrame: number | undefined;
    let markers: (() => void) | undefined;
    let styleReady = false;
    let revision = Date.now();
    let health = noteStyleLoaded(createBasemapHealth(), Date.now());
    setNotice(null);
    setBlocked(false);
    let map: maplibre.Map;
    try {
      map = new maplibre.Map({ container: container.current, style: OPENFREEMAP_STYLE_DARK,
        center: [initialView.lon, initialView.lat], zoom: initialView.zoom, minZoom: 6, maxPitch: 0,
        transformRequest: url => ({ url: refreshBasemapMetadataUrl(url, revision) }) });
    } catch {
      setBlocked(true);
      setNotice('The map could not start. Check that WebGL is enabled, then retry.');
      return;
    }
    mapRef.current = map;
    // Style JSON may arrive while a sprite or worker remains stalled. Keep an
    // independent, bounded full-load deadline as well as basemap tile recovery.
    const loadTimer = setTimeout(() => {
      if (alive && !map.loaded()) {
        setBlocked(true);
        setNotice('The map is taking too long to load. Check your connection, then retry.');
      }
    }, MAP_INITIAL_LOAD_TIMEOUT_MS);
    const idle = () => {
      if (!map.loaded()) return;
      clearTimeout(loadTimer);
      if (health.phase === 'healthy') { setBlocked(false); setNotice(null); }
    };
    map.on('idle', idle);
    const arm = () => {
      clearTimeout(timer);
      const delay = basemapWatchdogDelay(health, Date.now());
      if (delay !== null) timer = setTimeout(recover, delay);
    };
    const setStyle = (style: string | maplibre.StyleSpecification) => {
      // A failed initial style has no valid style to diff against.
      map.setStyle(style, { diff: false });
    };
    function recover() {
      if (!alive) return;
      const decision = decideBasemapAction(health, Date.now());
      if (decision === 'none') { arm(); return; }
      if (decision === 'retry') {
        if (!styleReady) setBlocked(true);
        health = noteRetryStarted(health, Date.now());
        revision = Date.now();
        setNotice('Retrying the basemap…');
        setStyle(refreshBasemapMetadataUrl(OPENFREEMAP_STYLE_DARK, revision));
        arm();
      } else {
        health = noteFallback(health);
        clearTimeout(timer);
        setNotice('The vector basemap is unavailable. Showing the fallback map.');
        setStyle(buildFallbackStyle('dark'));
        void loadFallbackCities().then(cities => {
          if (mapRef.current === map && health.phase === 'fallback') markers = installFallbackMarkers(map, cities);
        }).catch(() => { /* Land and raster remain usable without city labels. */ });
      }
    }
    const styleLoaded = () => {
      if (!alive) return;
      styleReady = true;
      onMapReady(map);
      health = noteStyleLoaded(health, Date.now());
      const style = map.getStyle();
      if (health.phase !== 'fallback' && !style.layers.some(layer => 'source' in layer && layer.source === BASEMAP_VECTOR_SOURCE_ID)) {
        health = noteSourceData(health, BASEMAP_VECTOR_SOURCE_ID, true, Date.now());
        setNotice(null);
        setBlocked(false);
      }
      arm();
    };
    const sourceData = (event: maplibre.MapSourceDataEvent) => {
      if (!isSuccessfulBasemapTile(event)) return;
      health = noteSourceData(health, event.sourceId, true, Date.now());
      if (health.phase === 'healthy') { clearTimeout(timer); setNotice(null); }
    };
    const sourceLoading = (event: maplibre.MapSourceDataEvent) => {
      if (event.sourceId === BASEMAP_VECTOR_SOURCE_ID && event.tile && health.phase === 'healthy') {
        health = noteStyleLoaded(health, Date.now());
        arm();
      }
    };
    const error = (event: maplibre.ErrorEvent & { sourceId?: string }) => {
      if (event.sourceId !== BASEMAP_VECTOR_SOURCE_ID && styleReady) return;
      if (!styleReady) {
        setBlocked(true);
        setNotice('The map could not load. Retry to try again.');
      }
      health = noteTileError(health, Date.now(), event.error.message);
      recover();
    };
    const contextLost = () => {
      setBlocked(true);
      setNotice('The browser lost the map graphics connection. Retry to reload the map.');
    };
    map.on('style.load', styleLoaded);
    map.on('sourcedata', sourceData);
    map.on('sourcedataloading', sourceLoading);
    map.on('error', error);
    map.on('webglcontextlost', contextLost);
    const resize = new ResizeObserver(() => {
      if (resizeFrame !== undefined) cancelAnimationFrame(resizeFrame);
      resizeFrame = requestAnimationFrame(() => { resizeFrame = undefined; if (alive) map.resize(); });
    });
    resize.observe(container.current);
    arm();
    return () => {
      alive = false;
      mapRef.current = null;
      clearTimeout(timer);
      clearTimeout(loadTimer);
      if (resizeFrame !== undefined) cancelAnimationFrame(resizeFrame);
      resize.disconnect();
      markers?.();
      onMapReady(null);
      map.off('style.load', styleLoaded);
      map.off('sourcedata', sourceData);
      map.off('sourcedataloading', sourceLoading);
      map.off('error', error);
      map.off('webglcontextlost', contextLost);
      map.off('idle', idle);
      map.remove();
    };
  }, [initialView, scopeKey, attempt, onMapReady]);
  return <div className="embedded-map">
    <div className="embedded-map__canvas" ref={container} />
    {notice && <div className="embedded-map__notice" role={blocked ? 'alert' : 'status'}>
      <span>{notice}</span><button type="button" onClick={() => setAttempt(value => value + 1)}>{blocked ? 'Retry map' : 'Retry basemap'}</button>
    </div>}
  </div>;
}
