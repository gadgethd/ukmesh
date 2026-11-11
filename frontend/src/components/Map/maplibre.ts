import { setWorkerUrl } from 'maplibre-gl';
import workerUrl from 'virtual:maplibre-worker-url';

// The worker shares the same source graph and content-hashed chunks as the renderer.
// Fixed worker filenames can remain in browser/SW caches across library upgrades,
// pairing the current renderer with an incompatible worker indefinitely.
setWorkerUrl(workerUrl);

// Keep dynamic imports from retaining every optional MapLibre control.
export { Map, LngLatBounds, Popup, Marker, NavigationControl, addProtocol } from 'maplibre-gl';
export type * from 'maplibre-gl';
