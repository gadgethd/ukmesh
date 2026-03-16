import React, { useState, useEffect, useMemo, useRef } from 'react';

interface MeshNode {
  node_id: string;
  name?: string;
  lat?: number;
  lon?: number;
  iata?: string;
  role?: number;
  last_seen: string;
  is_online: boolean;
  hardware_model?: string;
  public_key?: string;
  advert_count?: number;
  elevation_m?: number;
}

interface NodeLink {
  peer_id: string;
  peer_name: string | null;
  observed_count: number;
  itm_path_loss_db: number | null;
  count_this_to_peer: number;
  count_peer_to_this: number;
}

interface PacketHistory {
  time: string;
  packet_hash: string;
  src_node_id: string;
  topic: string;
  packet_type: number;
  hop_count: number;
  rssi: number;
  snr: number;
}

interface AdvertPacket {
  time: string;
  packet_hash: string;
}

function timeAgo(iso: string): string {
  const secs = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}

function predictNextAdvert<T extends { time: string; packet_hash: string }>(packets: T[]): { nextAdvert: Date; avgInterval: number; samples: number } | null {
  if (packets.length < 2) return null;

  // Sort by time ascending (oldest first)
  const sorted = [...packets].sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());

  // First deduplicate by packet_hash (same advert received by different nodes)
  // Keep the earliest time for each unique hash
  const byHash = new Map<string, T>();
  for (const pkt of sorted) {
    if (!byHash.has(pkt.packet_hash)) {
      byHash.set(pkt.packet_hash, pkt);
    }
  }
  let unique = Array.from(byHash.values()).sort((a, b) =>
    new Date(a.time).getTime() - new Date(b.time).getTime()
  );

  // Filter out packets that are within 30 seconds of each other
  // (these are duplicates from different observers receiving the same packet)
  const MIN_INTERVAL = 30;
  const filtered: T[] = [];
  for (const pkt of unique) {
    if (filtered.length === 0) {
      filtered.push(pkt);
    } else {
      const lastTime = new Date(filtered[filtered.length - 1].time).getTime();
      const thisTime = new Date(pkt.time).getTime();
      if ((thisTime - lastTime) / 1000 >= MIN_INTERVAL) {
        filtered.push(pkt);
      }
    }
  }
  unique = filtered;

  if (unique.length < 2) return null;

  // Take last 10 unique packets
  const recent = unique.slice(-10);

  // Calculate intervals between consecutive packets
  const intervals: number[] = [];
  for (let i = 1; i < recent.length; i++) {
    const prev = new Date(recent[i - 1].time).getTime();
    const curr = new Date(recent[i].time).getTime();
    const interval = (curr - prev) / 1000; // convert to seconds
    if (interval > 0 && interval < 259200) { // ignore invalid intervals (> 3 days)
      intervals.push(interval);
    }
  }

  if (intervals.length < 1) return null;

  // Filter out outliers - only use intervals within 50% of median
  // This captures the recent consistent pattern and excludes old outlier intervals
  const sortedIntervals = [...intervals].sort((a, b) => a - b);
  const medianIdx = Math.floor(sortedIntervals.length / 2);
  const median = sortedIntervals.length % 2 === 0
    ? (sortedIntervals[medianIdx - 1] + sortedIntervals[medianIdx]) / 2
    : sortedIntervals[medianIdx];

  const filteredIntervals = intervals.filter(i =>
    i >= median * 0.5 && i <= median * 1.5
  );

  // Use filtered intervals if we have enough, otherwise fall back to all intervals
  const intervalsToUse = filteredIntervals.length >= 2 ? filteredIntervals : intervals;

  const avgInterval = intervalsToUse.reduce((a, b) => a + b, 0) / intervalsToUse.length;
  const lastPacketTime = new Date(recent[recent.length - 1].time).getTime();
  const nextAdvert = new Date(lastPacketTime + avgInterval * 1000);

  return { nextAdvert, avgInterval, samples: intervals.length };
}

function formatInterval(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${Math.round(seconds / 3600)}h`;
}

function formatTimeUntil(date: Date): string {
  const secs = Math.floor((date.getTime() - Date.now()) / 1000);
  if (secs <= 0) return 'now';
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

export const UKRepeaterSearchPage: React.FC = () => {
  const [searchQuery, setSearchQuery] = useState('');
  const [showResults, setShowResults] = useState(false);
  const [nodes, setNodes] = useState<MeshNode[]>([]);
  const [selectedNode, setSelectedNode] = useState<MeshNode | null>(null);
  const [links, setLinks] = useState<NodeLink[]>([]);
  const [history, setHistory] = useState<PacketHistory[]>([]);
  const [adverts, setAdverts] = useState<AdvertPacket[]>([]);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [copiedKey, setCopiedKey] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  // Load nodes on mount
  useEffect(() => {
    fetch('/api/nodes?network=ukmesh')
      .then(r => r.json())
      .then(data => setNodes(Array.isArray(data) ? data : []))
      .catch(() => setNodes([]));
  }, []);

  // Click outside to close search dropdown
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
        setShowResults(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const searchResults = useMemo(() => {
    if (!searchQuery.trim()) return [];
    const q = searchQuery.toLowerCase();
    return nodes
      .filter(n => {
        // Exclude nodes marked as disabled (🚫 in name)
        if (n.name && n.name.includes('🚫')) return false;
        // Repeaters only (role=2); exclude companion radios (role=1) and room servers (role=3)
        if (n.role !== undefined && n.role !== 2) return false;
        const nameMatch = n.name && n.name.toLowerCase().includes(q);
        const keyMatch = n.public_key && n.public_key.toLowerCase().includes(q);
        const iataMatch = n.iata && n.iata.toLowerCase().includes(q);
        return nameMatch || keyMatch || iataMatch;
      })
      .slice(0, 10);
  }, [searchQuery, nodes]);

  // Calculate predicted next advert based on advert packets
  const prediction = useMemo(() => {
    if (!adverts.length) return null;
    return predictNextAdvert(adverts);
  }, [adverts]);

  const selectNode = async (node: MeshNode) => {
    setSelectedNode(node);
    setSearchQuery(node.name || node.public_key?.slice(0, 16) || '');
    setShowResults(false);
    setLoadingDetails(true);
    setLinks([]);
    setHistory([]);
    setAdverts([]);
    setCopiedKey(false);

    try {
      const [linksRes, historyRes, advertsRes] = await Promise.all([
        fetch(`/api/nodes/${node.node_id}/links`),
        fetch(`/api/nodes/${node.node_id}/history?hours=24`),
        fetch(`/api/nodes/${node.public_key}/adverts?hours=168`)
      ]);

      const linksData = await linksRes.json();
      const historyData = await historyRes.json();
      const advertsData = await advertsRes.json();

      setLinks(Array.isArray(linksData) ? linksData : []);
      setHistory(Array.isArray(historyData) ? historyData : []);
      setAdverts(Array.isArray(advertsData) ? advertsData : []);
    } catch {
      // Ignore errors
    } finally {
      setLoadingDetails(false);
    }
  };

  const copyPublicKey = async () => {
    if (selectedNode?.public_key) {
      await navigator.clipboard.writeText(selectedNode.public_key);
      setCopiedKey(true);
      setTimeout(() => setCopiedKey(false), 2000);
    }
  };

  return (
    <>
      <section className="site-page-hero">
        <div className="site-content">
          <h1>Repeater Search</h1>
          <p>Search for a repeater by name or public key to view detailed information.</p>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="repeater-search-box" ref={searchRef}>
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => { setSearchQuery(e.target.value); setShowResults(true); }}
              onFocus={() => setShowResults(true)}
              placeholder="Search by repeater name, IATA code, or public key..."
              className="repeater-search-box__input"
              autoFocus
            />
            {showResults && (
              <div className="repeater-search-box__results">
                {searchQuery && searchResults.length === 0 ? (
                  <div className="repeater-search-box__no-results">
                    No repeaters found matching "{searchQuery}"
                  </div>
                ) : (
                  searchResults.map(node => (
                    <button
                      key={node.node_id}
                      className="repeater-search-box__result"
                      onClick={() => selectNode(node)}
                    >
                      <span className="repeater-search-box__result-name">{node.name || 'Unknown'}</span>
                      <span className="repeater-search-box__result-meta">
                        {node.iata ? `${node.iata} · ` : ''}{node.public_key?.slice(0, 16)}... · {node.is_online ? 'Online' : 'Offline'}
                      </span>
                    </button>
                  ))
                )}
                {searchResults.length > 0 && (
                  <div className="repeater-search-box__count">
                    {searchResults.length} result{searchResults.length !== 1 ? 's' : ''}
                  </div>
                )}
              </div>
            )}
          </div>

          {!selectedNode ? (
            <div className="repeater-details-card">
              <div className="repeater-details-card__empty">
                <svg className="repeater-details-card__empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
                  <circle cx="11" cy="11" r="8" />
                  <path d="m21 21-4.35-4.35" />
                </svg>
                <h3>Select a Repeater</h3>
                <p>Search for a repeater above to view its details, neighbours, and packet history.</p>
              </div>
            </div>
          ) : (
            <div className="repeater-details-card">
              <div className="repeater-details-card__header">
                <h2>{selectedNode.name || 'Unknown Repeater'}</h2>
                <span className={`repeater-details-card__status ${selectedNode.is_online ? 'repeater-details-card__status--online' : 'repeater-details-card__status--offline'}`}>
                  {selectedNode.is_online ? 'Online' : 'Offline'}
                </span>
              </div>

              <div className="repeater-details-card__section">
                <h3>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="repeater-details-card__section-icon">
                    <circle cx="12" cy="12" r="10" />
                    <path d="M12 6v6l4 2" />
                  </svg>
                  Details
                </h3>
                <div className="repeater-details-card__grid">
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Public Key</span>
                    <span className="repeater-details-card__value repeater-details-card__value--mono">
                      {selectedNode.public_key
                        ? `${selectedNode.public_key.slice(0, 16)}…${selectedNode.public_key.slice(-8)}`
                        : 'N/A'}
                    </span>
                    {selectedNode.public_key && (
                      <button
                        className="repeater-details-card__copy-btn"
                        onClick={copyPublicKey}
                      >
                        {copiedKey ? (
                          <>
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="repeater-details-card__copy-icon">
                              <polyline points="20 6 9 17 4 12" />
                            </svg>
                            Copied!
                          </>
                        ) : (
                          <>
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="repeater-details-card__copy-icon">
                              <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
                              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                            </svg>
                            Copy
                          </>
                        )}
                      </button>
                    )}
                  </div>
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Position</span>
                    <span className="repeater-details-card__value">
                      {selectedNode.lat && selectedNode.lon
                        ? `${selectedNode.lat.toFixed(5)}, ${selectedNode.lon.toFixed(5)}`
                        : 'Unknown'}
                    </span>
                  </div>
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Elevation</span>
                    <span className="repeater-details-card__value">
                      {selectedNode.elevation_m !== undefined && selectedNode.elevation_m !== null
                        ? `${Math.round(selectedNode.elevation_m)} m`
                        : 'N/A'}
                    </span>
                  </div>
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Network</span>
                    <span className="repeater-details-card__value">{selectedNode.iata || 'N/A'}</span>
                  </div>
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Hardware</span>
                    <span className="repeater-details-card__value">{selectedNode.hardware_model || 'Unknown'}</span>
                  </div>
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Last Seen</span>
                    <span className="repeater-details-card__value">{timeAgo(selectedNode.last_seen)}</span>
                  </div>
                  <div className="repeater-details-card__field">
                    <span className="repeater-details-card__label">Advert Count</span>
                    <span className="repeater-details-card__value">{selectedNode.advert_count?.toLocaleString() || '0'}</span>
                  </div>
                  {prediction && (
                    <div className="repeater-details-card__field">
                      <span className="repeater-details-card__label">Predicted Next Advert</span>
                      <span className="repeater-details-card__value">
                        {prediction.samples >= 3 ? formatTimeUntil(prediction.nextAdvert) : 'Collecting data…'}
                      </span>
                      <span className="repeater-details-card__meta">
                        ~{formatInterval(prediction.avgInterval)} interval ({prediction.samples} samples{prediction.samples < 3 ? ' — need 3+' : ''})
                      </span>
                    </div>
                  )}
                </div>
              </div>

              {loadingDetails ? (
                <div className="repeater-details-card__loading">
                  <div className="repeater-details-card__spinner"></div>
                  Loading details...
                </div>
              ) : (
                <>
                  <div className="repeater-details-card__section">
                    <h3>
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="repeater-details-card__section-icon">
                        <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
                        <circle cx="9" cy="7" r="4" />
                        <path d="M23 21v-2a4 4 0 0 0-3-3.87" />
                        <path d="M16 3.13a4 4 0 0 1 0 7.75" />
                      </svg>
                      Confirmed Neighbours {links.length > 0 && <span className="repeater-details-card__count-badge">{links.length}</span>}
                    </h3>
                    {links.length === 0 ? (
                      <p className="repeater-details-card__empty-msg">No neighbours found for this node.</p>
                    ) : (
                      <>
                        <p className="repeater-details-card__desc">Nodes with confirmed two-way communication</p>
                        <div className="repeater-details-card__neighbours">
                          {links.map(link => (
                            <div key={link.peer_id} className="repeater-details-card__neighbour">
                              <div className="repeater-details-card__neighbour-main">
                                <span className="repeater-details-card__neighbour-name">
                                  {link.peer_name || `${link.peer_id.slice(0, 12)}...`}
                                </span>
                                <span className="repeater-details-card__neighbour-id">
                                  {link.peer_id.slice(0, 16)}…{link.peer_id.slice(-8)}
                                </span>
                              </div>
                              <div className="repeater-details-card__neighbour-stats">
                                <span>Seen {link.observed_count}×</span>
                                {link.itm_path_loss_db !== null && (
                                  <span> · {Math.round(link.itm_path_loss_db)} dB loss</span>
                                )}
                                <span> · TX: {link.count_this_to_peer}</span>
                                <span> · RX: {link.count_peer_to_this}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </>
                    )}
                  </div>

                  <div className="repeater-details-card__section">
                    <h3>
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="repeater-details-card__section-icon">
                        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                      </svg>
                      Recent Packets {history.length > 0 && <span className="repeater-details-card__count-badge">{history.length}</span>}
                    </h3>
                    {history.length === 0 ? (
                      <p className="repeater-details-card__empty-msg">No packet history available for this node.</p>
                    ) : (
                      <>
                        <p className="repeater-details-card__desc">Last 24 hours of packet activity</p>
                        <div className="repeater-details-card__table-wrap">
                          <table className="repeater-details-card__table">
                            <thead>
                              <tr>
                                <th>Time</th>
                                <th>Hops</th>
                                <th>RSSI</th>
                                <th>SNR</th>
                                <th>From</th>
                              </tr>
                            </thead>
                            <tbody>
                              {history.slice(0, 50).map((pkt, idx) => (
                                <tr key={idx}>
                                  <td>{timeAgo(pkt.time)}</td>
                                  <td>{pkt.hop_count ?? '-'}</td>
                                  <td>{pkt.rssi ?? '-'}</td>
                                  <td>{pkt.snr ?? '-'}</td>
                                  <td>{pkt.src_node_id?.slice(0, 12) || '-'}...</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </>
                    )}
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </section>
    </>
  );
};
