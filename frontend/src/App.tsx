import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import type { Map as LeafletMap } from 'leaflet';
import { MapView } from './components/Map/MapView.js';
import { NodeSearch } from './components/Map/NodeSearch.js';
import { FilterPanel, FILTER_ROWS, type Filters } from './components/FilterPanel/FilterPanel.js';
import { StatsPanel } from './components/StatsPanel/StatsPanel.js';
import { PacketFeed } from './components/PacketFeed.js';
import { useWebSocket, type WSMessage, type WSReadyState } from './hooks/useWebSocket.js';
import { useNodes, type LivePacketData, type MeshNode } from './hooks/useNodes.js';
import { useCoverage, type NodeCoverage } from './hooks/useCoverage.js';

const DEFAULT_FILTERS: Filters = {
  livePackets:       true,
  coverage:          false,
  clientNodes:       false,
  packetPaths:       false,
  betaPaths:         false,
  betaPathThreshold: 0.5,
};

// Connectivity indicator
const ConnIndicator: React.FC<{ state: WSReadyState }> = ({ state }) => (
  <div className="conn-indicator">
    <span className={`conn-dot ${state === 'connected' ? 'conn-dot--connected' : ''}`} />
    <span style={{ color: state === 'connected' ? 'var(--online)' : 'var(--text-muted)' }}>
      {state === 'connected' ? 'LIVE' : state === 'connecting' ? 'CONNECTING' : 'OFFLINE'}
    </span>
  </div>
);

// SVG logo icon
const MeshIcon: React.FC = () => (
  <svg viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
    <circle cx="10" cy="4"  r="2" fill="currentColor" />
    <circle cx="3"  cy="16" r="2" fill="currentColor" />
    <circle cx="17" cy="16" r="2" fill="currentColor" />
    <line x1="10" y1="6" x2="3"  y2="14" stroke="currentColor" strokeWidth="1.2" />
    <line x1="10" y1="6" x2="17" y2="14" stroke="currentColor" strokeWidth="1.2" />
    <line x1="3"  y1="16" x2="17" y2="16" stroke="currentColor" strokeWidth="1.2" />
    <circle cx="10" cy="10" r="1.5" fill="currentColor" opacity="0.6" />
  </svg>
);

const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;

const PATH_TTL = 5_000; // ms to display packet path before auto-clearing

/**
 * Given a list of 2-char relay hop prefixes (from decoded.path), resolve each to
 * the best-matching node and build a full waypoint array: src → relay... → rx.
 * When multiple nodes share the same 2-char hex prefix, pick the one closest to
 * the linearly-interpolated expected position along the src→rx line.
 */
function resolvePathWaypoints(
  pathHashes: string[],
  src: MeshNode | null,   // null when sender unknown (e.g. GroupText)
  rx:  MeshNode,
  allNodes: Map<string, MeshNode>,
): [number, number][] {
  const waypoints: [number, number][] = src ? [[src.lat!, src.lon!]] : [];
  const N = pathHashes.length;

  for (let i = 0; i < N; i++) {
    const prefix = pathHashes[i]!.toUpperCase();
    const candidates = Array.from(allNodes.values()).filter(
      (n) => n.lat && n.lon && !n.name?.includes('🚫') && n.node_id.toUpperCase().startsWith(prefix),
    );
    if (candidates.length === 0) continue;

    let best = candidates[0]!;
    if (candidates.length > 1) {
      if (src) {
        // Interpolate expected position along src→rx line
        const t      = (i + 1) / (N + 1);
        const expLat = src.lat! + t * (rx.lat! - src.lat!);
        const expLon = src.lon! + t * (rx.lon! - src.lon!);
        best = candidates.reduce((a, b) => {
          const da = Math.hypot(a.lat! - expLat, a.lon! - expLon);
          const db = Math.hypot(b.lat! - expLat, b.lon! - expLon);
          return da <= db ? a : b;
        });
      } else {
        // No src — pick candidate closest to last placed waypoint, falling back to rx
        const [anchorLat, anchorLon] = waypoints.length > 0
          ? waypoints[waypoints.length - 1]!
          : [rx.lat!, rx.lon!];
        best = candidates.reduce((a, b) => {
          const da = Math.hypot(a.lat! - anchorLat, a.lon! - anchorLon);
          const db = Math.hypot(b.lat! - anchorLat, b.lon! - anchorLon);
          return da <= db ? a : b;
        });
      }
    }
    waypoints.push([best.lat!, best.lon!]);
  }

  waypoints.push([rx.lat!, rx.lon!]);
  return waypoints;
}

// ── Beta path helpers ─────────────────────────────────────────────────────────
// Effective range for a node: uses the stored radio-horizon radius from the
// viewshed worker (computed from actual SRTM terrain elevation), clamped 50–80 km.
function nodeRange(nodeId: string, coverage: NodeCoverage[]): number {
  const cov = coverage.find((c) => c.node_id === nodeId);
  if (!cov?.radius_m) return 50;
  return Math.min(80, Math.max(50, cov.radius_m / 1000));
}

// Returns true if two nodes are within range of each other.
// Threshold is the max of each node's elevation-derived range (50–80 km).
function canReach(a: MeshNode, b: MeshNode, coverage: NodeCoverage[]): boolean {
  const threshold = Math.max(nodeRange(a.node_id, coverage), nodeRange(b.node_id, coverage));
  const midLat    = ((a.lat! + b.lat!) / 2) * (Math.PI / 180);
  const dlat      = (a.lat! - b.lat!) * 111;
  const dlon      = (a.lon! - b.lon!) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon) < threshold;
}

function pickByInterpolation(
  candidates: MeshNode[],
  src: MeshNode | null,
  rx: MeshNode,
  hopIndex: number,
  totalHops: number,
): MeshNode {
  if (candidates.length === 1) return candidates[0]!;
  if (!src?.lat || !src?.lon) return candidates[0]!;
  const t      = (hopIndex + 1) / (totalHops + 1);
  const expLat = src.lat + t * (rx.lat! - src.lat);
  const expLon = src.lon + t * (rx.lon! - src.lon);
  return candidates.reduce((a, b) =>
    Math.hypot(a.lat! - expLat, a.lon! - expLon) <= Math.hypot(b.lat! - expLat, b.lon! - expLon) ? a : b,
  );
}

/**
 * Scores a beta path using per-hop confidence:
 *   - ambiguity factor:  1 / n_candidates  (fewer matches → more confident)
 *   - distance factor:   1.0 if chosen hop is within 50 km of prev node
 *                        0.3 if out of range
 *                        0.7 if prev node unknown (first hop, src unseen)
 * Last relay → rx is also distance-checked and penalised if out of range.
 * Returns null if any hop has zero known candidates.
 */
function resolveBetaPath(
  pathHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  allNodes: Map<string, MeshNode>,
  coverage: NodeCoverage[],
): { path: [number, number][]; confidence: number } | null {
  if (!rx.lat || !rx.lon || pathHashes.length === 0) return null;

  const resolvedNodes: MeshNode[] = [];
  const hopConfidences: number[]  = [];
  let prevNode: MeshNode | null   = src?.lat && src?.lon ? src : null;

  for (let i = 0; i < pathHashes.length; i++) {
    const prefix     = pathHashes[i]!.slice(0, 2).toUpperCase();
    const candidates = Array.from(allNodes.values()).filter(
      (n) => n.lat && n.lon && (n.role === undefined || n.role === 2)
        && !n.name?.includes('🚫') && n.node_id.toUpperCase().startsWith(prefix),
    );
    if (candidates.length === 0) continue;

    const ambiguityFactor = 1.0 / candidates.length;
    let chosen: MeshNode;
    let coverageFactor: number;

    if (prevNode) {
      const reachable = candidates.filter((c) => canReach(prevNode!, c, coverage));
      if (reachable.length > 0) {
        chosen        = pickByInterpolation(reachable, src, rx, i, pathHashes.length);
        coverageFactor = 1.0;
      } else {
        chosen        = pickByInterpolation(candidates, src, rx, i, pathHashes.length);
        coverageFactor = 0.3;
      }
    } else {
      // First hop, source unknown — can't check inbound coverage
      chosen        = pickByInterpolation(candidates, src, rx, i, pathHashes.length);
      coverageFactor = 0.7;
    }

    hopConfidences.push(coverageFactor * ambiguityFactor);
    resolvedNodes.push(chosen);
    prevNode = chosen;
  }

  // Penalise if the last relay can't reach rx
  if (resolvedNodes.length > 0 && !canReach(resolvedNodes[resolvedNodes.length - 1]!, rx, coverage)) {
    hopConfidences[hopConfidences.length - 1]! *= 0.3;
  }

  const confidence = hopConfidences.reduce((a, b) => a + b, 0) / hopConfidences.length;

  const pathNodes: MeshNode[] = [
    ...(src?.lat && src?.lon ? [src] : []),
    ...resolvedNodes,
    rx,
  ];
  if (pathNodes.length < 2) return null;

  return { path: pathNodes.map((n) => [n.lat!, n.lon!]), confidence };
}

const DISCLAIMER_KEY = 'meshcore-disclaimer-dismissed';

const DisclaimerModal: React.FC<{ onClose: () => void }> = ({ onClose }) => (
  <div className="disclaimer-overlay" role="dialog" aria-modal="true" aria-label="Data disclaimer">
    <div className="disclaimer-modal">
      <h2 className="disclaimer-modal__title">Data disclaimer</h2>
      <div className="disclaimer-modal__body">
        <section>
          <h3>Packet paths</h3>
          <p>
            The relay paths shown on this dashboard are a best estimate. MeshCore packets include
            only the first 2 hex characters of each relay node's ID, so when resolving a path we
            match those 2 characters against known nodes. If multiple nodes share the same prefix
            the closest candidate is chosen, but the actual path the packet took may have been
            different.
          </p>
        </section>
        <section>
          <h3>Coverage map</h3>
          <p>
            The green coverage layer is a radio horizon estimate computed from SRTM terrain data.
            It assumes each repeater antenna is mounted <strong>5 metres above ground level</strong>.
            Actual coverage will vary with antenna height, local obstacles, foliage, and radio
            conditions. Treat it as a rough guide, not a guarantee of connectivity.
          </p>
        </section>
      </div>
      <button className="disclaimer-modal__close" onClick={onClose}>Got it</button>
    </div>
  </div>
);

export const App: React.FC = () => {
  const [filters, setFilters]       = useState<Filters>(DEFAULT_FILTERS);
  const [stats, setStats]           = useState({ mqttNodes: 0, staleNodes: 0, packetsDay: 0 });
  const [map, setMap]               = useState<LeafletMap | null>(null);
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));
  const [packetPath, setPacketPath]         = useState<[number, number][] | null>(null);
  const [betaPacketPath, setBetaPacketPath] = useState<[number, number][] | null>(null);

  const dismissDisclaimer = useCallback(() => {
    localStorage.setItem(DISCLAIMER_KEY, '1');
    setShowDisclaimer(false);
  }, []);
  const [pathOpacity, setPathOpacity] = useState(0.75);
  const pathTimerRef                = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathFadeRef                 = useRef<number | null>(null);

  const {
    nodes, packets, arcs, activeNodes,
    handleInitialState, handlePacket, handleNodeUpdate, handleNodeUpsert,
  } = useNodes();

  const { coverage, handleCoverageUpdate } = useCoverage();

  const mapNodes = useMemo(() => Array.from(nodes.values()).filter(
    (n) => n.lat && n.lon
      && Date.now() - new Date(n.last_seen).getTime() < FOURTEEN_DAYS_MS
      && !n.name?.includes('🚫')
      && (n.role === undefined || n.role === 2)
  ).length, [nodes]); // eslint-disable-line react-hooks/exhaustive-deps

  // All non-sensor, non-hidden nodes ever seen (no recency filter)
  const totalDevices = useMemo(() => Array.from(nodes.values()).filter(
    (n) => !n.name?.includes('🚫') && n.role !== 4
  ).length, [nodes]);

  // Register tile-caching service worker
  useEffect(() => {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }
  }, []);

  // Poll stats every 30s
  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch('/api/stats');
        if (res.ok) setStats(await res.json() as typeof stats);
      } catch { /* ignore */ }
    };
    fetchStats();
    const t = setInterval(fetchStats, 30_000);
    return () => clearInterval(t);
  }, []);

  // Compute dotted path lines from most-recent packet's source → observer.
  // Clears after PATH_TTL ms (with a 1s fade), or immediately when the next
  // distinct packet arrives.
  const latestId = packets[0]?.id;
  useEffect(() => {
    if (pathTimerRef.current) clearTimeout(pathTimerRef.current);
    if (pathFadeRef.current !== null) { cancelAnimationFrame(pathFadeRef.current); pathFadeRef.current = null; }

    const latest = packets[0];
    const rx = latest?.rxNodeId ? nodes.get(latest.rxNodeId) : undefined;

    // ── Regular packet path ────────────────────────────────────────────────────
    if (filters.packetPaths && latest?.rxNodeId && (latest.path?.length || latest.srcNodeId) && rx?.lat && rx?.lon) {
      const src = latest.srcNodeId ? (nodes.get(latest.srcNodeId) ?? null) : null;
      const srcWithPos = src?.lat && src?.lon ? src : null;
      const waypoints = latest.path?.length
        ? resolvePathWaypoints(latest.path, srcWithPos, rx, nodes)
        : [[srcWithPos!.lat!, srcWithPos!.lon!], [rx.lat, rx.lon]] as [number, number][];
      setPacketPath(waypoints.length >= 2 ? waypoints : null);
    } else {
      setPacketPath(null);
    }

    // ── Beta path (unambiguous hops + coverage validation) ────────────────────
    if (filters.betaPaths && latest?.rxNodeId && latest.path?.length && rx?.lat && rx?.lon) {
      const src    = latest.srcNodeId ? (nodes.get(latest.srcNodeId) ?? null) : null;
      const result = resolveBetaPath(
        latest.path,
        src?.lat && src?.lon ? src : null,
        rx, nodes, coverage,
      );
      setBetaPacketPath(result && result.confidence >= filters.betaPathThreshold ? result.path : null);
    } else {
      setBetaPacketPath(null);
    }

    if (!filters.packetPaths && !filters.betaPaths) { setPathOpacity(0.75); return; }
    if (!latest) { setPathOpacity(0.75); return; }

    setPathOpacity(0.75);
    pathTimerRef.current = setTimeout(() => {
      const FADE_MS = 1_000;
      const startTime = performance.now();
      const animate = (now: number) => {
        const t = Math.min(1, (now - startTime) / FADE_MS);
        setPathOpacity(0.75 * (1 - t));
        if (t < 1) {
          pathFadeRef.current = requestAnimationFrame(animate);
        } else {
          pathFadeRef.current = null;
          setPacketPath(null);
          setBetaPacketPath(null);
          setPathOpacity(0.75);
        }
      };
      pathFadeRef.current = requestAnimationFrame(animate);
    }, PATH_TTL - 1_000);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestId, filters.packetPaths, filters.betaPaths]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleMessage = useCallback((msg: WSMessage) => {
    if (msg.type === 'initial_state') {
      handleInitialState(msg.data as Parameters<typeof handleInitialState>[0]);
    } else if (msg.type === 'packet') {
      handlePacket(msg.data as LivePacketData);
    } else if (msg.type === 'node_update') {
      handleNodeUpdate(msg.data as { nodeId: string; ts: number });
    } else if (msg.type === 'node_upsert') {
      handleNodeUpsert(msg.data as Partial<MeshNode> & { node_id: string });
    } else if (msg.type === 'coverage_update') {
      handleCoverageUpdate(msg.data as { node_id: string; geom: { type: string; coordinates: unknown } });
    }
  }, [handleInitialState, handlePacket, handleNodeUpdate, handleNodeUpsert, handleCoverageUpdate]);

  const wsState = useWebSocket(handleMessage);

  return (
    <div className="app-shell">
      {/* ── Topbar ─────────────────────────────────────────────────────── */}
      <header className="topbar">
        <a href="https://www.teessidemesh.com" className="topbar__home-btn" title="Home">← Home</a>
        <div className="topbar__logo">
          <MeshIcon />
          MeshCore Analytics
        </div>
        <div className="topbar__divider" />
        <ConnIndicator state={wsState} />
        <button
          className="topbar__info-btn"
          onClick={() => setShowDisclaimer(true)}
          title="Data disclaimer"
          aria-label="Data disclaimer"
        >
          i
        </button>
        <StatsPanel
          mqttNodes={stats.mqttNodes}
          mapNodes={mapNodes}
          totalDevices={totalDevices}
          staleNodes={stats.staleNodes}
          packetsDay={stats.packetsDay}
        />
      </header>

      {/* ── Mobile controls: 2x2 filter grid + search (in grid flow, above map) ── */}
      <div className="mobile-controls">
        <div className="mobile-filter-grid">
          {FILTER_ROWS.map(({ key, label, color, hollow }) => (
            <div
              key={key}
              className={`filter-row${filters[key] ? ' filter-row--on' : ''}`}
              onClick={() => setFilters({ ...filters, [key]: !filters[key] })}
              role="button"
              aria-pressed={!!filters[key]}
            >
              <span className="filter-row__label">
                {hollow ? (
                  <span className="filter-dot filter-dot--hollow" style={{ borderColor: color, opacity: filters[key] ? 1 : 0.4 }} />
                ) : (
                  <span className="filter-dot" style={{ background: color, opacity: filters[key] ? 1 : 0.3 }} />
                )}
                {label}
              </span>
              <span
                className={`filter-toggle${filters[key] ? ' filter-toggle--on' : ''}`}
                style={filters[key] ? { background: `${color}22`, borderColor: color } : {}}
              />
            </div>
          ))}
        </div>
        <div className="mobile-search">
          <NodeSearch map={map} nodes={nodes} />
        </div>
      </div>

      {/* ── Map + Overlays ─────────────────────────────────────────────── */}
      <MapView
        nodes={nodes}
        arcs={arcs}
        activeNodes={activeNodes}
        coverage={coverage}
        showPackets={filters.livePackets}
        showCoverage={filters.coverage}
        showClientNodes={filters.clientNodes}
        packetPath={packetPath}
        betaPath={betaPacketPath}
        showBetaPaths={filters.betaPaths}
        pathOpacity={pathOpacity}
        onMapReady={setMap}
      />

      {/* ── Filter Panel (desktop only — absolute overlay) ──────────────── */}
      <FilterPanel filters={filters} onChange={setFilters} />

      {/* ── Live Packet Feed ───────────────────────────────────────────── */}
      {filters.livePackets && <PacketFeed packets={packets} nodes={nodes} />}

      {/* ── Disclaimer modal ───────────────────────────────────────────── */}
      {showDisclaimer && <DisclaimerModal onClose={dismissDisclaimer} />}
    </div>
  );
};
