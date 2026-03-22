import React, { useEffect, useRef, useMemo, useCallback } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import type { MeshNode } from '../../hooks/useNodes.js';
import type { FeedPacket } from './UKFeedPage.js';

// ── Types ─────────────────────────────────────────────────────────────────────

type PacketDetail = {
  packetHash: string;
  time: string;
  rxNodeId: string | null;
  srcNodeId: string | null;
  topic: string;
  packetType: number | null;
  routeType: number | null;
  hopCount: number | null;
  rssi: number | null;
  snr: number | null;
  payload: Record<string, unknown> | null;
  pathHashes: string[] | null;
  pathHashSizeBytes: number | null;
  rawHex: string | null;
  observations: Array<{ rxNodeId: string | null; time: string; rssi: number | null; snr: number | null; hopCount: number | null }>;
};

export type ResolvedPath = {
  ok: boolean;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  purplePath: [number, number][] | null;
  redPath: [number, number][] | null;
  redSegments?: [[number, number], [number, number]][];
};

type RadioState = {
  frequency?: number;
  sf?: number;
  bw?: number;
  cr?: number;
  channel?: string;
};

export type LazyPathNode = {
  position: number;
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  appearances: number;
  totalObservations: number;
  ambiguous: boolean;
  isObserver: boolean;
};

export type LazyPath = {
  canonicalPath: LazyPathNode[];
  coordinates: Array<[number, number]>;
  matchedHops: number;
  totalHops: number;
  observerIds: string[];
};

export type LazyPathResult = {
  packetHash: string;
  observerCount: number;
  paths: LazyPath[];
};

// ── Constants ─────────────────────────────────────────────────────────────────

const PAYLOAD_NAMES: Record<number, string> = {
  0: 'Request', 1: 'Response', 2: 'TextMessage', 3: 'Ack',
  4: 'Advertisement', 5: 'GroupMessage', 6: 'Data', 7: 'Anon',
  8: 'Path', 9: 'Trace', 11: 'Control',
};

const ROUTE_NAMES: Record<number, string> = {
  0: 'Flood', 1: 'Direct', 2: 'Flood+Codes', 3: 'Direct+Codes',
};

const CARTO_TILES = [
  'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
  'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
];

const C_PURPLE = '#ce93d8';
const C_RED = '#ff5252';
const C_CYAN = '#00c4ff';

// ── Hex parsing ───────────────────────────────────────────────────────────────

function hexToBytes(hex: string): Uint8Array {
  const clean = hex.replace(/\s/g, '');
  const bytes = new Uint8Array(Math.floor(clean.length / 2));
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function hexSlice(hex: string, byteStart: number, byteEnd: number): string {
  return hex.slice(byteStart * 2, byteEnd * 2).toUpperCase();
}

function byteRangeName(start: number, end: number): string {
  return start === end - 1 ? `Byte ${start}` : `Bytes ${start}–${end - 1}`;
}

type ParsedPacket = {
  totalBytes: number;
  headerByte: number;
  rawRouteType: number;   // bits 0-1
  rawPayloadType: number; // bits 2-5
  version: number;        // bits 6-7
  hasTransportCodes: boolean;
  transportCodesHex: string | null;
  pathLengthByte: number;
  pathHashCount: number;
  pathHashSizeBytes: number;
  pathDataStart: number;
  pathDataEnd: number;
  payloadStart: number;
};

function parsePacketHex(hex: string): ParsedPacket | null {
  const clean = hex.replace(/\s/g, '');
  if (clean.length < 4) return null;
  const bytes = hexToBytes(clean);
  if (bytes.length < 2) return null;

  const headerByte = bytes[0]!;
  const rawRouteType = headerByte & 0x03;
  const rawPayloadType = (headerByte >> 2) & 0x0F;
  const version = (headerByte >> 6) & 0x03;

  // routeType 0 or 3 have 4 transport code bytes before path length
  const hasTransportCodes = rawRouteType === 0 || rawRouteType === 3;
  const lengthByteOffset = hasTransportCodes ? 5 : 1;

  if (bytes.length <= lengthByteOffset) return null;
  const pathLengthByte = bytes[lengthByteOffset]!;
  const pathHashCount = pathLengthByte & 0x3f;
  const pathHashSizeBytes = (pathLengthByte >> 6) + 1;

  const pathDataStart = lengthByteOffset + 1;
  const pathDataEnd = pathDataStart + pathHashCount * pathHashSizeBytes;
  const payloadStart = pathDataEnd;

  return {
    totalBytes: bytes.length,
    headerByte,
    rawRouteType,
    rawPayloadType,
    version,
    hasTransportCodes,
    transportCodesHex: hasTransportCodes ? hexSlice(clean, 1, 5) : null,
    pathLengthByte,
    pathHashCount,
    pathHashSizeBytes,
    pathDataStart,
    pathDataEnd,
    payloadStart,
  };
}

// ── Path map ─────────────────────────────────────────────────────────────────

// Distinct colours for multiple lazy paths
const LAZY_PATH_COLORS = ['#26c6a2', '#00b4d8', '#f59e0b', '#a78bfa', '#f87171'];

export const PathMap: React.FC<{
  results: ResolvedPath[];
  observerPositions?: [number, number][];
  lazyPaths?: LazyPath[];
  nodeMap?: Map<string, MeshNode>;
}> = ({ results, observerPositions = [], lazyPaths = [], nodeMap }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const nodeMapRef = useRef(nodeMap);
  nodeMapRef.current = nodeMap;

  const toLngLat = ([lat, lon]: [number, number]): [number, number] => [lon, lat];

  const allPoints = useMemo((): [number, number][] => {
    const pts: [number, number][] = [];
    for (const r of results) {
      if (r.purplePath) pts.push(...r.purplePath);
      if (r.redPath) pts.push(...r.redPath);
      if (r.redSegments) r.redSegments.forEach(([a, b]) => { pts.push(a); pts.push(b); });
    }
    pts.push(...observerPositions);
    for (const path of lazyPaths) pts.push(...path.coordinates);
    return pts.filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
  }, [results, observerPositions, lazyPaths]);

  // Stable string key — prevents map recreation when array refs change but coordinate values are identical
  const mapKey = useMemo(
    () => allPoints.map(([lat, lon]) => `${lat.toFixed(5)},${lon.toFixed(5)}`).join('|'),
    [allPoints],
  );

  useEffect(() => {
    if (!containerRef.current || allPoints.length === 0) return;

    const firstPt = toLngLat(allPoints[0]!);
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          tiles: { type: 'raster', tiles: CARTO_TILES, tileSize: 256, maxzoom: 19, attribution: '© OpenStreetMap © CARTO' },
        },
        layers: [{ id: 'bg', type: 'raster', source: 'tiles' }],
      },
      center: firstPt,
      zoom: 8,
      attributionControl: false,
    });

    map.on('load', () => {
      const bounds = new maplibregl.LngLatBounds();
      const allPurpleCoords: [number, number][][] = [];
      const allRedCoords: [number, number][][] = [];
      const allNodeCoords: [number, number][] = [];

      for (const r of results) {
        if (r.purplePath && r.purplePath.length >= 2) {
          const coords = r.purplePath.map(toLngLat);
          allPurpleCoords.push(coords);
          allNodeCoords.push(...coords);
          coords.forEach((c) => bounds.extend(c));
        }
        if (r.redPath && r.redPath.length >= 2) {
          allRedCoords.push(r.redPath.map(toLngLat));
        }
        if (r.redSegments && r.redSegments.length > 0) {
          r.redSegments.forEach(([a, b]) => allRedCoords.push([toLngLat(a), toLngLat(b)]));
        }
        if (r.redPath) r.redPath.forEach((pt) => bounds.extend(toLngLat(pt)));
        if (r.redSegments) r.redSegments.forEach(([a, b]) => { bounds.extend(toLngLat(a)); bounds.extend(toLngLat(b)); });
      }

      if (allPurpleCoords.length > 0) {
        map.addSource('purple-lines', {
          type: 'geojson',
          data: {
            type: 'FeatureCollection',
            features: allPurpleCoords.map((coords) => ({ type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: {} })),
          },
        });
        map.addLayer({ id: 'purple-lines-layer', type: 'line', source: 'purple-lines', paint: { 'line-color': C_PURPLE, 'line-width': 2.5, 'line-opacity': 0.85 } });
        map.addSource('purple-nodes', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: allNodeCoords.map((c) => ({ type: 'Feature', geometry: { type: 'Point', coordinates: c }, properties: {} })) },
        });
        map.addLayer({ id: 'purple-node-circles', type: 'circle', source: 'purple-nodes', paint: { 'circle-radius': 5, 'circle-color': '#0b1725', 'circle-stroke-color': C_CYAN, 'circle-stroke-width': 2 } });
      }

      if (allRedCoords.length > 0) {
        map.addSource('red-lines', {
          type: 'geojson',
          data: {
            type: 'FeatureCollection',
            features: allRedCoords.map((coords) => ({ type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: {} })),
          },
        });
        map.addLayer({ id: 'red-lines-layer', type: 'line', source: 'red-lines', paint: { 'line-color': C_RED, 'line-width': 1.5, 'line-opacity': 0.65, 'line-dasharray': [4, 4] } });
      }

      lazyPaths.forEach((lazyPath, pi) => {
        const color = LAZY_PATH_COLORS[pi % LAZY_PATH_COLORS.length]!;
        const validNodes = lazyPath.canonicalPath.filter(
          (n) => n.lat != null && n.lon != null && Number.isFinite(n.lat) && Number.isFinite(n.lon),
        );
        if (validNodes.length < 2) return;
        const lngLat = validNodes.map((n) => [n.lon!, n.lat!] as [number, number]);
        lngLat.forEach((c) => bounds.extend(c));
        const lineId = `lazy-line-${pi}`;
        const nodeLayerId = `lazy-nodes-${pi}`;
        map.addSource(lineId, { type: 'geojson', data: { type: 'Feature', geometry: { type: 'LineString', coordinates: lngLat }, properties: {} } });
        map.addLayer({ id: `${lineId}-layer`, type: 'line', source: lineId, paint: { 'line-color': color, 'line-width': 3, 'line-opacity': 0.9 } });
        map.addSource(nodeLayerId, {
          type: 'geojson',
          data: {
            type: 'FeatureCollection',
            features: validNodes.map((n, ni) => ({
              type: 'Feature' as const,
              geometry: { type: 'Point' as const, coordinates: lngLat[ni]! },
              properties: { nodeId: n.nodeId ?? '', name: n.name ?? '', isObserver: n.isObserver },
            })),
          },
        });
        map.addLayer({ id: `${nodeLayerId}-layer`, type: 'circle', source: nodeLayerId, paint: { 'circle-radius': 6, 'circle-color': '#0b1725', 'circle-stroke-color': color, 'circle-stroke-width': 2.5 } });
        map.on('click', `${nodeLayerId}-layer`, (e) => {
          const feat = e.features?.[0];
          if (!feat) return;
          const props = feat.properties as { nodeId: string; name: string; isObserver: boolean };
          const fullNode = props.nodeId ? nodeMapRef.current?.get(props.nodeId) : undefined;
          const displayName = props.name || props.nodeId.slice(0, 12) || '—';
          const pubKey = fullNode?.public_key ?? props.nodeId ?? '—';
          new maplibregl.Popup({ closeButton: true, maxWidth: '320px' })
            .setLngLat(e.lngLat)
            .setHTML(
              `<div style="font-family:monospace;font-size:12px;line-height:1.6">` +
              `<strong style="font-size:13px;font-family:sans-serif">${displayName}${props.isObserver ? ' <span style="color:#ffb300">[observer]</span>' : ''}</strong><br>` +
              `<span style="color:#aaa">Public key</span><br><span style="word-break:break-all">${pubKey}</span>` +
              `</div>`,
            )
            .addTo(map);
        });
        map.on('mouseenter', `${nodeLayerId}-layer`, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', `${nodeLayerId}-layer`, () => { map.getCanvas().style.cursor = ''; });
      });

      const validObservers = observerPositions.filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
      if (validObservers.length > 0) {
        const obsFeatures = validObservers.map(([lat, lon]) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [lon, lat] as [number, number] },
          properties: {},
        }));
        map.addSource('observer-pos', { type: 'geojson', data: { type: 'FeatureCollection', features: obsFeatures } });
        map.addLayer({ id: 'observer-pos-layer', type: 'circle', source: 'observer-pos', paint: { 'circle-radius': 8, 'circle-color': '#ffb300', 'circle-stroke-color': '#ffffff', 'circle-stroke-width': 2 } });
        obsFeatures.forEach((f) => bounds.extend(f.geometry.coordinates));
      }

      if (!bounds.isEmpty()) map.fitBounds(bounds, { padding: 24, animate: false });
    });

    return () => { map.remove(); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mapKey]);

  if (allPoints.length === 0) {
    return <div className="feed-detail__no-map">Path could not be resolved</div>;
  }
  return <div ref={containerRef} style={{ height: '100%', width: '100%' }} />;
};

// ── Byte breakdown section ────────────────────────────────────────────────────

const ByteSection: React.FC<{
  label: string;
  byteRange: string;
  hexValue: string;
  description?: string;
  children?: React.ReactNode;
}> = ({ label, byteRange, hexValue, description, children }) => (
  <div className="feed-detail__byte-section">
    <div className="feed-detail__byte-section-header">
      <span className="feed-detail__byte-label">{label}</span>
      <span className="feed-detail__byte-name">{byteRange}</span>
      <code className="feed-detail__byte-label" style={{ background: 'transparent', color: 'var(--text-secondary)' }}>{hexValue}</code>
    </div>
    {(description || children) && (
      <div className="feed-detail__byte-body">
        {description && <p className="feed-detail__byte-note">{description}</p>}
        {children}
      </div>
    )}
  </div>
);

const BitTable: React.FC<{
  rows: Array<{ bits: string; field: string; value: string; binary: string }>;
}> = ({ rows }) => (
  <table className="feed-detail__bit-table">
    <thead>
      <tr><th>Bits</th><th>Field</th><th>Value</th><th>Binary</th></tr>
    </thead>
    <tbody>
      {rows.map((row, i) => (
        <tr key={i}>
          <td>{row.bits}</td><td>{row.field}</td><td>{row.value}</td><td style={{ fontFamily: 'var(--font-mono)', fontSize: '10px', color: 'var(--text-muted)' }}>{row.binary}</td>
        </tr>
      ))}
    </tbody>
  </table>
);

// ── Radio formatting ──────────────────────────────────────────────────────────

function formatRadio(radio: RadioState): string {
  const parts: string[] = [];
  if (radio.frequency != null) parts.push(`${(radio.frequency / 1_000_000).toFixed(3)} MHz`);
  if (radio.sf != null) parts.push(`SF${radio.sf}`);
  if (radio.bw != null) parts.push(`BW${radio.bw >= 1000 ? (radio.bw / 1000).toFixed(1) : radio.bw}`);
  if (radio.cr != null) parts.push(`CR${radio.cr}`);
  return parts.join(' / ') || '—';
}

// ── Main panel ────────────────────────────────────────────────────────────────

const TYPE_LABELS: Record<number, string> = {
  0: 'REQ', 1: 'RSP', 2: 'DM', 3: 'ACK', 4: 'ADV', 5: 'GRP',
  6: 'DAT', 7: 'ANON', 8: 'PATH', 9: 'TRC', 11: 'CTL',
};


export const PacketDetailPanel: React.FC<{
  packet: FeedPacket;
  nodeMap: Map<string, MeshNode>;
  network: string;
  onClose: () => void;
  cachedLazyPath?: LazyPathResult | null;
}> = ({ packet, nodeMap, network, onClose, cachedLazyPath }) => {
  const [detail, setDetail] = React.useState<PacketDetail | null>(null);
  const [resolvedPaths, setResolvedPaths] = React.useState<ResolvedPath[]>([]);
  const [radio, setRadio] = React.useState<RadioState | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [pathLoading, setPathLoading] = React.useState(false);
  const [lazyPath, setLazyPath] = React.useState<LazyPathResult | null>(null);
  const [lazyStatus, setLazyStatus] = React.useState<'idle' | 'settling' | 'loading' | 'done' | 'notfound' | 'error'>('idle');
  const [lazyCountdown, setLazyCountdown] = React.useState(0);
  const lazyTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lazyTickRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch static detail + radio once per packet hash
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setDetail(null);

    Promise.all([
      fetch(`/api/packets/${packet.packet_hash}?network=${encodeURIComponent(network)}`, { cache: 'no-store' })
        .then((r) => r.ok ? r.json() as Promise<PacketDetail> : null)
        .catch(() => null),
      fetch('/api/radio-stats', { cache: 'no-store' })
        .then((r) => r.ok ? r.json() as Promise<RadioState> : null)
        .catch(() => null),
    ]).then(([d, r]) => {
      if (cancelled) return;
      setDetail(d);
      setRadio(r);
      setLoading(false);
    });

    return () => { cancelled = true; };
  }, [packet.packet_hash, network]);

  // Lazy path fetch — called once settling is done
  const fetchLazyPath = useCallback(async () => {
    setLazyStatus('loading');
    try {
      const netParam = network ? `&network=${encodeURIComponent(network)}` : '';
      const r = await fetch(`/api/path-lazy/resolve?hash=${packet.packet_hash}${netParam}`, { cache: 'no-store' });
      if (r.status === 404) { setLazyStatus('notfound'); return; }
      if (!r.ok) { setLazyStatus('error'); return; }
      const data = await r.json() as LazyPathResult;
      setLazyPath(data);
      setLazyStatus('done');
    } catch {
      setLazyStatus('error');
    }
  }, [packet.packet_hash, network]);

  // Re-fetch resolved paths whenever the observer list changes (new MQTT observation)
  const observerKey = (packet.observer_node_ids ?? []).slice().sort().join(',');
  useEffect(() => {
    let cancelled = false;
    setPathLoading(true);

    const netParam = network ? `&network=${encodeURIComponent(network)}` : '';
    fetch(`/api/path-beta/resolve-multi?hash=${packet.packet_hash}${netParam}`, { cache: 'no-store' })
      .then((r): Promise<ResolvedPath[]> => {
        if (!r.ok) return Promise.resolve([]);
        return (r.json() as Promise<{ results?: ResolvedPath[] }>).then((data) => data.results ?? []);
      })
      .catch(() => [])
      .then((paths) => {
        if (cancelled) return;
        setResolvedPaths(paths);
        setPathLoading(false);
      });

    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [packet.packet_hash, network, observerKey]);

  // Lazy path: use background-cached result if available, otherwise settle-and-fetch.
  const SETTLE_MS = 10_000;
  useEffect(() => {
    if (lazyTimerRef.current) clearTimeout(lazyTimerRef.current);
    if (lazyTickRef.current) clearInterval(lazyTickRef.current);

    // If the background cache in UKFeedPage already has a result, use it immediately.
    if (cachedLazyPath) {
      setLazyPath(cachedLazyPath);
      setLazyStatus('done');
      setLazyCountdown(0);
      return;
    }

    // Don't attempt lazy resolution for packets with no path hashes
    if (!packet.path_hashes?.length) {
      setLazyStatus('notfound');
      return;
    }

    // Otherwise run the in-panel settle timer as a fallback.
    setLazyPath(null);
    setLazyStatus('settling');
    setLazyCountdown(SETTLE_MS / 1000);

    lazyTickRef.current = setInterval(() => {
      setLazyCountdown((c) => Math.max(0, c - 1));
    }, 1000);

    lazyTimerRef.current = setTimeout(() => {
      if (lazyTickRef.current) clearInterval(lazyTickRef.current);
      setLazyCountdown(0);
      void fetchLazyPath();
    }, SETTLE_MS);

    return () => {
      if (lazyTimerRef.current) clearTimeout(lazyTimerRef.current);
      if (lazyTickRef.current) clearInterval(lazyTickRef.current);
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [packet.packet_hash, network, observerKey, cachedLazyPath]);

  // Observer info
  const rxNodeId = detail?.rxNodeId ?? packet.rx_node_id ?? null;
  const rxNode = rxNodeId ? nodeMap.get(rxNodeId) : undefined;
  const observerName = rxNode?.name ?? rxNodeId?.slice(0, 8) ?? '—';
  const observerIata = rxNode?.iata?.trim().toUpperCase() ?? '—';

  // Regions heard — combine live observer_node_ids, rx_node_id fallback, and DB observations
  const regionsHeard = useMemo(() => {
    const iatas = new Set<string>();
    const ids: (string | null | undefined)[] = [
      ...(packet.observer_node_ids?.length ? packet.observer_node_ids : [packet.rx_node_id]),
      ...(detail?.observations?.map((o) => o.rxNodeId) ?? []),
    ];
    for (const id of ids) {
      if (!id) continue;
      const iata = nodeMap.get(id)?.iata;
      if (iata) iatas.add(iata.trim().toUpperCase());
    }
    return Array.from(iatas).join(' · ') || '—';
  }, [packet.observer_node_ids, packet.rx_node_id, detail?.observations, nodeMap]);

  // Propagation time — span from first observer to last observer receiving this packet
  const propagationTime = useMemo(() => {
    if (!detail?.observations || detail.observations.length < 2) return null;
    const times = detail.observations.map((o) => Date.parse(o.time)).filter(Number.isFinite);
    if (times.length < 2) return null;
    const diffMs = Math.max(...times) - Math.min(...times);
    if (diffMs === 0) return null; // all observers at same ms
    if (diffMs > 300_000) return null; // >5min likely clock skew, not propagation
    return diffMs < 1000 ? `${diffMs}ms` : `${(diffMs / 1000).toFixed(2)}s`;
  }, [detail]);

  // Path text
  const pathText = useMemo(() => {
    const hashes = packet.path_hashes ?? detail?.pathHashes;
    if (!hashes?.length) return null;
    return hashes.map((h) => h.toUpperCase()).join('→');
  }, [packet.path_hashes, detail?.pathHashes]);

  // Byte breakdown
  const parsed = useMemo(() => {
    if (!detail?.rawHex) return null;
    return parsePacketHex(detail.rawHex);
  }, [detail?.rawHex]);

  const routeLabel = detail?.routeType != null ? (ROUTE_NAMES[detail.routeType] ?? `Type${detail.routeType}`) : '—';
  const typeLabel = packet.packet_type != null ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—';
  const heardAt = new Date(packet.time).toLocaleString();
  const observerCount = detail?.observations?.length ?? packet.rx_count ?? 1;

  const resolvedHopCount = useMemo(() => {
    const allNodes = new Set<string>();
    for (const r of resolvedPaths) {
      r.purplePath?.forEach(([lat, lon]) => allNodes.add(`${lat},${lon}`));
    }
    return allNodes.size;
  }, [resolvedPaths]);

  const totalHops = packet.path_hashes?.length ?? packet.hop_count ?? null;

  // Observer GPS positions for map markers
  const observerPositions = useMemo((): [number, number][] => {
    const allIds = new Set<string>();
    const candidates: string[] = [
      ...(packet.observer_node_ids?.length ? packet.observer_node_ids : [packet.rx_node_id].filter(Boolean) as string[]),
      ...(detail?.observations?.map((o) => o.rxNodeId).filter(Boolean) as string[] ?? []),
    ];
    const positions: [number, number][] = [];
    for (const id of candidates) {
      if (!id || allIds.has(id)) continue;
      allIds.add(id);
      const node = nodeMap.get(id);
      if (node?.lat != null && node?.lon != null) {
        positions.push([node.lat, node.lon]);
      }
    }
    return positions;
  }, [packet.observer_node_ids, packet.rx_node_id, detail?.observations, nodeMap]);

  return (
    <div className="feed-detail-panel">
      {/* Header */}
      <div className="feed-detail__header">
        <code className="feed-detail__hash">{packet.packet_hash}</code>
        <span className="feed-detail__badge">{typeLabel}</span>
        {totalHops != null && <span className="feed-detail__badge feed-detail__badge--muted">{totalHops} hop{totalHops !== 1 ? 's' : ''}</span>}
        <button type="button" className="feed-detail__close" onClick={onClose}>✕</button>
      </div>

      {loading && <p className="feed-detail__loading">Loading…</p>}

      {/* Info grid */}
      <div className="feed-detail__section">
        <div className="feed-detail__info-grid">
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Observer</span>
            <span className="feed-detail__info-value">{observerName}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Route</span>
            <span className="feed-detail__info-value">{routeLabel}</span>
          </div>
          {radio && (
            <div className="feed-detail__info-item">
              <span className="feed-detail__info-label">Radio</span>
              <span className="feed-detail__info-value">{formatRadio(radio)}</span>
            </div>
          )}
          {propagationTime && (
            <div className="feed-detail__info-item">
              <span className="feed-detail__info-label">Propagation</span>
              <span className="feed-detail__info-value">{propagationTime}s</span>
            </div>
          )}
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Heard at</span>
            <span className="feed-detail__info-value">{heardAt}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Heard by</span>
            <span className="feed-detail__info-value">{observerCount} observer{observerCount !== 1 ? 's' : ''}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Observer region</span>
            <span className="feed-detail__info-value">{observerIata}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Regions heard</span>
            <span className="feed-detail__info-value">{regionsHeard}</span>
          </div>
        </div>
      </div>

      {/* Observer table */}
      {detail?.observations && detail.observations.length > 0 && (
        <div className="feed-detail__section">
          <div className="feed-detail__section-title">Observers ({detail.observations.length})</div>
          <table className="feed-detail__observer-table">
            <thead>
              <tr><th>Node</th><th>Region</th><th>Hops</th><th>RSSI</th><th>SNR</th><th>Time</th></tr>
            </thead>
            <tbody>
              {detail.observations.map((obs, i) => {
                const node = obs.rxNodeId ? nodeMap.get(obs.rxNodeId) : undefined;
                const iata = node?.iata?.trim().toUpperCase() ?? '—';
                const name = node?.name ?? (obs.rxNodeId ? `${obs.rxNodeId.slice(0, 8)}…` : '—');
                return (
                  <tr key={i}>
                    <td>{name}</td>
                    <td>{iata}</td>
                    <td>{obs.hopCount ?? '—'}</td>
                    <td>{obs.rssi ?? '—'}</td>
                    <td>{obs.snr != null ? obs.snr.toFixed(1) : '—'}</td>
                    <td>{new Date(obs.time).toLocaleTimeString()}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Lazy path status + hop table */}
      <div className="feed-detail__section">
        <div className="feed-detail__section-title">
          Hash-traced path
          {lazyStatus === 'settling' && (
            <span className="feed-detail__section-note"> — settling ({lazyCountdown}s)</span>
          )}
          {lazyStatus === 'loading' && (
            <span className="feed-detail__section-note"> — resolving…</span>
          )}
          {lazyStatus === 'done' && lazyPath && (() => {
            const totalMatched = lazyPath.paths.reduce((s, p) => s + p.matchedHops, 0);
            const totalHopsAll = lazyPath.paths.reduce((s, p) => s + p.totalHops, 0);
            return (
              <span className="feed-detail__section-note">
                {' '}— {totalMatched} of {totalHopsAll} hops matched
                {lazyPath.paths.length > 1 && ` (${lazyPath.paths.length} paths)`}
                {totalMatched > 0 && <span className="feed-detail__lazy-dot" />}
              </span>
            );
          })()}
          {lazyStatus === 'notfound' && (
            <span className="feed-detail__section-note"> — no path hashes</span>
          )}
          {lazyStatus === 'error' && (
            <span className="feed-detail__section-note feed-detail__section-note--error"> — fetch failed</span>
          )}
        </div>
        {lazyPath && lazyPath.paths.map((lp, pi) => (
          lp.canonicalPath.length > 0 && (
            <React.Fragment key={pi}>
              {lazyPath.paths.length > 1 && (
                <div style={{ fontSize: '11px', color: LAZY_PATH_COLORS[pi % LAZY_PATH_COLORS.length], marginBottom: '4px', marginTop: pi > 0 ? '8px' : undefined }}>
                  Path {pi + 1} — {lp.observerIds.length} observer{lp.observerIds.length !== 1 ? 's' : ''}
                </div>
              )}
              <table className="feed-detail__observer-table">
                <thead>
                  <tr><th>Hop</th><th>Hash</th><th>Node</th><th>Region</th><th>Seen by</th></tr>
                </thead>
                <tbody>
                  {lp.canonicalPath.map((step, si) => {
                    const iata = step.nodeId ? nodeMap.get(step.nodeId)?.iata?.trim().toUpperCase() ?? '—' : '—';
                    const matched = step.nodeId !== null && !step.ambiguous;
                    const rowClass = step.isObserver
                      ? 'feed-detail__lazy-row--observer'
                      : matched ? 'feed-detail__lazy-row--matched' : 'feed-detail__lazy-row--unmatched';
                    return (
                      <tr key={`${step.position}-${si}`} className={rowClass}>
                        <td>{step.isObserver ? '▶' : step.position + 1}</td>
                        <td><code style={{ fontSize: '10px', color: 'var(--text-muted)' }}>{step.isObserver ? 'observer' : step.hash}</code></td>
                        <td>{step.isObserver ? `[${step.name ?? step.nodeId?.slice(0, 10) ?? '?'}]` : step.ambiguous ? `${step.name ?? step.nodeId?.slice(0, 10)} (+amb)` : (step.name ?? step.nodeId?.slice(0, 10) ?? '—')}</td>
                        <td>{iata}</td>
                        <td style={{ color: 'var(--text-muted)' }}>{step.isObserver ? '—' : `${step.appearances}/${step.totalObservations}`}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </React.Fragment>
          )
        ))}
        {lazyStatus === 'settling' && (
          <div className="feed-detail__lazy-hint">
            Waiting for propagation to settle before tracing route from packet hashes…
          </div>
        )}
      </div>

      {/* Path text */}
      {pathText && (
        <div className="feed-detail__section">
          <div className="feed-detail__section-title">Path ({totalHops ?? '?'} hops)</div>
          <div className="feed-detail__path-text">{pathText}</div>
        </div>
      )}

      {/* Map */}
      <div className="feed-detail__section">
        <div className="feed-detail__section-title">
          Resolved path
          {pathLoading && <span className="feed-detail__section-note"> — updating…</span>}
          {!pathLoading && totalHops != null && resolvedHopCount > 0 && (
            <span className="feed-detail__section-note"> — {resolvedHopCount} of {totalHops} hops located</span>
          )}
        </div>
        <div className="feed-detail__map">
          {!loading && (
            <PathMap
              results={resolvedPaths}
              observerPositions={observerPositions}
              lazyPaths={lazyPath?.paths ?? []}
              nodeMap={nodeMap}
            />
          )}
        </div>
      </div>

      {/* Raw hex */}
      {detail?.rawHex && (
        <div className="feed-detail__section">
          <div className="feed-detail__section-title">
            Packet Byte Breakdown ({Math.floor(detail.rawHex.replace(/\s/g, '').length / 2)} bytes)
          </div>
          <code className="feed-detail__hex">{detail.rawHex.toUpperCase()}</code>

          {parsed && (
            <div className="feed-detail__breakdown">
              {/* Header byte */}
              <ByteSection
                label="Header"
                byteRange={byteRangeName(0, 1)}
                hexValue={`0x${detail.rawHex.slice(0, 2).toUpperCase()}`}
              >
                <BitTable rows={[
                  { bits: '0–1', field: 'Route Type', value: ROUTE_NAMES[parsed.rawRouteType] ?? `${parsed.rawRouteType}`, binary: parsed.rawRouteType.toString(2).padStart(2, '0') },
                  { bits: '2–5', field: 'Payload Type', value: PAYLOAD_NAMES[parsed.rawPayloadType] ?? `${parsed.rawPayloadType}`, binary: parsed.rawPayloadType.toString(2).padStart(4, '0') },
                  { bits: '6–7', field: 'Version', value: String(parsed.version), binary: parsed.version.toString(2).padStart(2, '0') },
                ]} />
              </ByteSection>

              {/* Transport codes (if present) */}
              {parsed.hasTransportCodes && parsed.transportCodesHex && (
                <ByteSection
                  label="Transport Codes"
                  byteRange={byteRangeName(1, 5)}
                  hexValue={parsed.transportCodesHex}
                  description="4-byte transport codes"
                />
              )}

              {/* Path length */}
              <ByteSection
                label="Path Length"
                byteRange={byteRangeName(parsed.hasTransportCodes ? 5 : 1, (parsed.hasTransportCodes ? 5 : 1) + 1)}
                hexValue={`0x${detail.rawHex.slice((parsed.hasTransportCodes ? 5 : 1) * 2, (parsed.hasTransportCodes ? 5 : 1) * 2 + 2).toUpperCase()}`}
                description={`${parsed.pathHashCount} × ${parsed.pathHashSizeBytes}-byte hash${parsed.pathHashCount !== 1 ? 'es' : ''} showing route taken`}
              />

              {/* Path data */}
              {parsed.pathHashCount > 0 && (
                <ByteSection
                  label="Path Data"
                  byteRange={byteRangeName(parsed.pathDataStart, parsed.pathDataEnd)}
                  hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.pathDataStart, parsed.pathDataEnd)}
                  description="Historical route taken (bytes added as packet floods)"
                />
              )}

              {/* Payload */}
              {parsed.payloadStart < parsed.totalBytes && (
                <ByteSection
                  label={`Payload — ${PAYLOAD_NAMES[parsed.rawPayloadType] ?? `Type ${parsed.rawPayloadType}`}`}
                  byteRange={byteRangeName(parsed.payloadStart, parsed.totalBytes)}
                  hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart, parsed.totalBytes)}
                  description={`${PAYLOAD_NAMES[parsed.rawPayloadType] ?? 'Unknown'} payload data`}
                >
                  {/* Advertisement decode */}
                  {parsed.rawPayloadType === 4 && detail?.payload && (
                    <div className="feed-detail__adv-fields">
                      {(() => {
                        const app = (detail.payload['appData'] ?? detail.payload) as Record<string, unknown>;
                        const rows: Array<[string, string]> = [];
                        if (typeof app['name'] === 'string') rows.push(['Name', app['name']]);
                        if (typeof app['role'] === 'number') rows.push(['Role', ['?', 'ChatNode', 'Repeater', 'RoomServer', 'Sensor'][app['role'] as number] ?? `${app['role']}`]);
                        if (typeof app['lat'] === 'number' && typeof app['lon'] === 'number') rows.push(['Location', `${(app['lat'] as number).toFixed(5)}, ${(app['lon'] as number).toFixed(5)}`]);
                        if (typeof app['freq'] === 'number') rows.push(['Frequency', `${((app['freq'] as number) / 1_000_000).toFixed(3)} MHz`]);
                        if (typeof app['sf'] === 'number') rows.push(['Spreading Factor', `SF${app['sf']}`]);
                        if (rows.length === 0) rows.push(['Payload', 'No decoded fields available']);
                        return rows.map(([k, v]) => (
                          <div key={k} className="feed-detail__adv-row">
                            <span className="feed-detail__info-label">{k}</span>
                            <span className="feed-detail__info-value">{v}</span>
                          </div>
                        ));
                      })()}
                    </div>
                  )}

                  {/* TextMessage sub-breakdown */}
                  {parsed.rawPayloadType === 2 && parsed.totalBytes - parsed.payloadStart >= 4 && (
                    <div className="feed-detail__breakdown" style={{ marginTop: '8px' }}>
                      <ByteSection
                        label="Destination Hash"
                        byteRange={byteRangeName(parsed.payloadStart, parsed.payloadStart + 1)}
                        hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart, parsed.payloadStart + 1)}
                        description="First byte of destination node public key"
                      />
                      <ByteSection
                        label="Source Hash"
                        byteRange={byteRangeName(parsed.payloadStart + 1, parsed.payloadStart + 2)}
                        hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart + 1, parsed.payloadStart + 2)}
                        description="First byte of source node public key"
                      />
                      {parsed.totalBytes - parsed.payloadStart >= 4 && (
                        <ByteSection
                          label="Cipher MAC"
                          byteRange={byteRangeName(parsed.payloadStart + 2, parsed.payloadStart + 4)}
                          hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart + 2, parsed.payloadStart + 4)}
                          description="MAC for encrypted data"
                        />
                      )}
                      {parsed.totalBytes - parsed.payloadStart > 4 && (
                        <ByteSection
                          label="Ciphertext"
                          byteRange={byteRangeName(parsed.payloadStart + 4, parsed.totalBytes)}
                          hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart + 4, parsed.totalBytes)}
                          description="Encrypted message data (timestamp + message text)"
                        />
                      )}
                    </div>
                  )}
                </ByteSection>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};
