import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import type { Map as LeafletMap } from 'leaflet';
import { MapView } from './components/Map/MapView.js';
import { NodeSearch } from './components/Map/NodeSearch.js';
import { FilterPanel, FILTER_ROWS, type Filters } from './components/FilterPanel/FilterPanel.js';
import { StatsPanel } from './components/StatsPanel/StatsPanel.js';
import { PacketFeed } from './components/PacketFeed.js';
import { useWebSocket, type WSMessage, type WSReadyState } from './hooks/useWebSocket.js';
import { useNodes, type LivePacketData, type MeshNode, type AggregatedPacket } from './hooks/useNodes.js';
import { useCoverage, type NodeCoverage } from './hooks/useCoverage.js';

const DEFAULT_FILTERS: Filters = {
  livePackets:       true,
  coverage:          false,
  clientNodes:       false,
  packetPaths:       false,
  betaPaths:         false,
  betaPathThreshold: 0.5,
  links:             false,
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

const MAX_BETA_HOPS = 15;
const MIN_LINK_OBSERVATIONS = 5; // must match backend db/index.ts

/** Canonical lookup key for a link between two nodes (order-independent). */
function linkKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}

function distKm(a: MeshNode, b: MeshNode): number {
  const midLat = ((a.lat! + b.lat!) / 2) * (Math.PI / 180);
  const dlat   = (a.lat! - b.lat!) * 111;
  const dlon   = (a.lon! - b.lon!) * 111 * Math.cos(midLat);
  return Math.hypot(dlat, dlon);
}

// ── Line-of-sight check (smooth Earth, k=0.25 refraction) ────────────────────
const R_EFF_M = 6_371_000 / (1 - 0.25); // ~8,495,000 m effective Earth radius

/**
 * Smooth-Earth LOS check with atmospheric refraction k=0.25 (equivalent to
 * the standard 4/3-Earth model). Uses each node's elevation_m (terrain ASL)
 * plus 5 m antenna height. Samples 20 evenly-spaced points along the path
 * and rejects the hop if Earth curvature alone would block the ray at any
 * point. Does not model terrain between the nodes — confirmed links already
 * have full ITM terrain analysis; this guards unconfirmed candidates.
 */
function hasLoS(a: MeshNode, b: MeshNode): boolean {
  const hA = (a.elevation_m ?? 0) + 5; // metres ASL at antenna
  const hB = (b.elevation_m ?? 0) + 5;
  const d  = distKm(a, b) * 1000;      // metres
  if (d < 1) return true;
  for (let i = 1; i < 20; i++) {
    const t     = i / 20;
    const x     = t * d;
    const los   = hA + (hB - hA) * t;
    const bulge = x * (d - x) / (2 * R_EFF_M);
    if (los < bulge) return false;
  }
  return true;
}

// Fallback range check (used when no ITM data exists for a pair).
function nodeRange(nodeId: string, coverage: NodeCoverage[]): number {
  const cov = coverage.find((c) => c.node_id === nodeId);
  if (!cov?.radius_m) return 50;
  return Math.min(80, Math.max(50, cov.radius_m / 1000));
}
function canReach(a: MeshNode, b: MeshNode, coverage: NodeCoverage[]): boolean {
  const threshold = Math.max(nodeRange(a.node_id, coverage), nodeRange(b.node_id, coverage));
  return distKm(a, b) < threshold;
}

/**
 * Beta path resolver — backtracking DFS working backwards from the receiver.
 *
 * For each relay prefix (reversed, so we start anchored at a known position):
 *   1. Try confirmed neighbours of prevNode first (real observed link data).
 *   2. If none, try candidates within mutual radio range (coverage-radius based).
 *   3. If a chosen candidate leads to dead ends at subsequent hops, backtrack
 *      and try the next candidate at this hop.
 *   4. If no candidate leads to a valid continuation, skip this hop (up to
 *      maxSkips total). Skips are bounded so the path can't just drop all hops.
 */
function resolveBetaPath(
  pathHashes: string[],
  src: MeshNode | null,
  rx: MeshNode,
  allNodes: Map<string, MeshNode>,
  coverage: NodeCoverage[],
  linkPairs: Set<string>,
): { path: [number, number][]; confidence: number } | null {
  if (!rx.lat || !rx.lon || pathHashes.length === 0) return null;
  if (pathHashes.length >= MAX_BETA_HOPS) return null;

  type HopResult = { node: MeshNode; conf: number } | null; // null = skipped

  /** Ordered candidate list for a single hop: confirmed neighbours first, then reachable,
   *  then a last-resort closest match within 500 km. A hop is only skipped when no node
   *  in the DB has the matching 2-char prefix at all. */
  function getCandidates(prefix: string, prevNode: MeshNode): Array<{ node: MeshNode; conf: number }> {
    const all = Array.from(allNodes.values()).filter(
      (n) => n.lat && n.lon && (n.role === undefined || n.role === 2)
        && !n.name?.includes('🚫') && n.node_id.toUpperCase().startsWith(prefix),
    );
    if (all.length === 0) return [];

    const usedIds = new Set<string>();

    const confirmed = all
      .filter((c) => linkPairs.has(linkKey(c.node_id, prevNode.node_id)))
      .sort((a, b) => distKm(a, prevNode) - distKm(b, prevNode))
      .map((c) => { usedIds.add(c.node_id); return { node: c, conf: 0.9 }; });

    const reachable = all
      .filter((c) => !usedIds.has(c.node_id) && canReach(c, prevNode, coverage) && hasLoS(c, prevNode))
      .sort((a, b) => distKm(a, prevNode) - distKm(b, prevNode))
      .slice(0, 2)
      .map((c) => { usedIds.add(c.node_id); return { node: c, conf: 0.3 / Math.max(1, all.length) }; });

    // Last resort: closest prefix match within 50 km with LOS clearance.
    // Prevents hops being silently dropped just because link data is sparse.
    const fallback = all
      .filter((c) => !usedIds.has(c.node_id) && distKm(c, prevNode) < 50 && hasLoS(c, prevNode))
      .sort((a, b) => distKm(a, prevNode) - distKm(b, prevNode))
      .slice(0, 1)
      .map((c) => ({ node: c, conf: 0.05 / Math.max(1, all.length) }));

    return [...confirmed, ...reachable, ...fallback];
  }

  // Only skip a hop when there is genuinely no node in the DB matching the prefix.
  const maxSkips = pathHashes.length;
  // Hard limit on total DFS calls to prevent exponential blowup on ambiguous prefixes.
  let budget = 300;

  /**
   * Recursive DFS. Returns the hop results (rx-to-src order) or null if budget
   * was exhausted before a valid path could be found.
   */
  function solve(hopIdx: number, prevNode: MeshNode, skipsLeft: number, visited: Set<string>): HopResult[] | null {
    if (hopIdx < 0) return [];
    if (--budget <= 0) return null;

    const prefix = pathHashes[hopIdx]!.slice(0, 2).toUpperCase();
    // Exclude nodes already used in this path — MeshCore nodes only relay a packet once.
    const options = getCandidates(prefix, prevNode).filter((o) => !visited.has(o.node.node_id));

    // Try each candidate. If it leads to a dead end, backtrack and try the next.
    for (const opt of options) {
      const nextVisited = new Set(visited);
      nextVisited.add(opt.node.node_id);
      const rest = solve(hopIdx - 1, opt.node, skipsLeft, nextVisited);
      if (rest !== null) return [opt, ...rest];
    }

    // No candidate produced a valid continuation — try skipping this hop.
    if (skipsLeft > 0) {
      const rest = solve(hopIdx - 1, prevNode, skipsLeft - 1, visited);
      if (rest !== null) return [null, ...rest];
    }

    return null; // truly stuck — caller will try its next candidate
  }

  const raw = solve(pathHashes.length - 1, rx, maxSkips, new Set([rx.node_id]));
  if (!raw) return null;

  // raw is in rx→src order; reverse to get src→rx order for rendering.
  const hops = [...raw].reverse().filter((r): r is { node: MeshNode; conf: number } => r !== null);
  if (hops.length === 0) return null;

  const confidence = hops.reduce((sum, h) => sum + h.conf, 0) / hops.length;

  const pathNodes: MeshNode[] = [
    ...(src?.lat && src?.lon ? [src] : []),
    ...hops.map((h) => h.node),
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
  const [linkPairs, setLinkPairs]       = useState<Set<string>>(new Set());
  const [viablePairsArr, setViablePairsArr] = useState<[string, string][]>([]);
  const [showDisclaimer, setShowDisclaimer] = useState(() => !localStorage.getItem(DISCLAIMER_KEY));
  const [packetPath, setPacketPath]         = useState<[number, number][] | null>(null);
  const [betaPacketPath, setBetaPacketPath] = useState<[number, number][] | null>(null);
  const [pinnedPacketId, setPinnedPacketId] = useState<string | null>(null);
  const pinnedTimerRef                      = useRef<ReturnType<typeof setTimeout> | null>(null);

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
  // distinct packet arrives. Skipped while a packet is pinned by the user.
  const latestId = packets[0]?.id;
  useEffect(() => {
    if (pinnedPacketId !== null) return;
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
      const hops   = latest.hopCount != null ? latest.path.slice(0, latest.hopCount) : latest.path;
      const result = resolveBetaPath(
        hops,
        src?.lat && src?.lon ? src : null,
        rx, nodes, coverage, linkPairs,
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
  }, [latestId, filters.packetPaths, filters.betaPaths, pinnedPacketId]); // eslint-disable-line react-hooks/exhaustive-deps

  const handlePacketPin = useCallback((packet: AggregatedPacket) => {
    // Toggle: clicking the already-pinned packet unpins it
    if (pinnedPacketId === packet.id) {
      setPinnedPacketId(null);
      if (pinnedTimerRef.current) { clearTimeout(pinnedTimerRef.current); pinnedTimerRef.current = null; }
      if (pathTimerRef.current)   { clearTimeout(pathTimerRef.current);   pathTimerRef.current   = null; }
      if (pathFadeRef.current !== null) { cancelAnimationFrame(pathFadeRef.current); pathFadeRef.current = null; }
      setPacketPath(null);
      setBetaPacketPath(null);
      setPathOpacity(0.75);
      return;
    }

    // Clear any running auto timers
    if (pathTimerRef.current)   { clearTimeout(pathTimerRef.current);   pathTimerRef.current   = null; }
    if (pathFadeRef.current !== null) { cancelAnimationFrame(pathFadeRef.current); pathFadeRef.current = null; }
    if (pinnedTimerRef.current) { clearTimeout(pinnedTimerRef.current); pinnedTimerRef.current = null; }

    const rx = packet.rxNodeId ? nodes.get(packet.rxNodeId) : undefined;

    setPacketPath(null);

    if (packet.rxNodeId && packet.path?.length && rx?.lat && rx?.lon) {
      const src  = packet.srcNodeId ? (nodes.get(packet.srcNodeId) ?? null) : null;
      const hops = packet.hopCount != null ? packet.path.slice(0, packet.hopCount) : packet.path;
      const result = resolveBetaPath(
        hops, src?.lat && src?.lon ? src : null, rx, nodes, coverage, linkPairs,
      );
      setBetaPacketPath(result ? result.path : null);
    } else {
      setBetaPacketPath(null);
    }

    setPathOpacity(0.75);
    setPinnedPacketId(packet.id);

    // Auto-release after 30s with a 1s fade
    pinnedTimerRef.current = setTimeout(() => {
      const FADE_MS   = 1_000;
      const startTime = performance.now();
      const animate   = (now: number) => {
        const t = Math.min(1, (now - startTime) / FADE_MS);
        setPathOpacity(0.75 * (1 - t));
        if (t < 1) {
          pathFadeRef.current = requestAnimationFrame(animate);
        } else {
          pathFadeRef.current = null;
          setPacketPath(null);
          setBetaPacketPath(null);
          setPathOpacity(0.75);
          setPinnedPacketId(null);
          pinnedTimerRef.current = null;
        }
      };
      pathFadeRef.current = requestAnimationFrame(animate);
    }, 9_000);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pinnedPacketId, nodes, coverage, linkPairs, filters.betaPaths, filters.betaPathThreshold]);

  const handleMessage = useCallback((msg: WSMessage) => {
    if (msg.type === 'initial_state') {
      const data = msg.data as Parameters<typeof handleInitialState>[0] & {
        viable_pairs?: [string, string][];
      };
      handleInitialState(data);
      if (data.viable_pairs) {
        setLinkPairs(new Set(data.viable_pairs.map(([a, b]) => linkKey(a, b))));
        setViablePairsArr(data.viable_pairs);
      }
    } else if (msg.type === 'packet') {
      handlePacket(msg.data as LivePacketData);
    } else if (msg.type === 'node_update') {
      handleNodeUpdate(msg.data as { nodeId: string; ts: number });
    } else if (msg.type === 'node_upsert') {
      handleNodeUpsert(msg.data as Partial<MeshNode> & { node_id: string });
    } else if (msg.type === 'coverage_update') {
      handleCoverageUpdate(msg.data as { node_id: string; geom: { type: string; coordinates: unknown } });
    } else if (msg.type === 'link_update') {
      const d = msg.data as {
        node_a_id: string; node_b_id: string;
        observed_count: number; itm_viable: boolean | null;
      };
      if (d.itm_viable && d.observed_count >= MIN_LINK_OBSERVATIONS) {
        const key = linkKey(d.node_a_id, d.node_b_id);
        setLinkPairs((prev) => {
          if (prev.has(key)) return prev;
          const next = new Set(prev);
          next.add(key);
          return next;
        });
        setViablePairsArr((prev) => {
          if (prev.some(([a, b]) => linkKey(a, b) === key)) return prev;
          return [...prev, [d.node_a_id, d.node_b_id]];
        });
      }
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
        showLinks={filters.links}
        viablePairsArr={viablePairsArr}
        packetPath={packetPath}
        betaPath={betaPacketPath}
        showBetaPaths={filters.betaPaths || pinnedPacketId !== null}
        pathOpacity={pathOpacity}
        onMapReady={setMap}
      />

      {/* ── Filter Panel (desktop only — absolute overlay) ──────────────── */}
      <FilterPanel filters={filters} onChange={setFilters} />

      {/* ── Live Packet Feed ───────────────────────────────────────────── */}
      {filters.livePackets && (
        <PacketFeed
          packets={packets}
          nodes={nodes}
          onPacketClick={handlePacketPin}
          pinnedPacketId={pinnedPacketId}
        />
      )}

      {/* ── Disclaimer modal ───────────────────────────────────────────── */}
      {showDisclaimer && <DisclaimerModal onClose={dismissDisclaimer} />}
    </div>
  );
};
