import React, { FormEvent, useEffect, useMemo, useState } from 'react';
import './owner-portal.css';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { OwnerLoginSection } from '../components/owner/OwnerPortalSections.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { useVisibilityPoll } from '../hooks/useVisibilityPoll.js';
import { ApiResponseError, fetchJson } from '../utils/api.js';

import {
  PACKET_LABELS,
  ROUTE_LABELS,
  cleanPacketBody,
  fetchOwnerCsrfToken,
  fmtTs,
  formatCompactTs,
  formatPathLoss,
  isOwnerLastHopStrengthResponse,
  isOwnerLiveResponse,
  isOwnerSessionResponse,
  isRecord,
  isValidMapCoord,
  lastHopSeriesCache,
  linkBadge,
  nodeRoleLabel,
  publishOwnerSession,
  type LastHopStrengthPoint,
  type MappedPeer,
  type OwnerDashboard,
  type OwnerLastHopStrengthResponse,
  type OwnerLiveResponse,
  type OwnerSessionResponse,
} from './owner/ownerPortalModel.js';
import {
  LastHopStrengthChart,
  TELEMETRY_SERIES,
  TelemetryMiniChart,
  TelemetryStatCard,
  TrendBars,
  formatUptime,
} from './owner/OwnerPortalCharts.js';
import { OwnerMapView } from './owner/OwnerMapView.js';
import { OwnerHeardNeighbors } from './owner/OwnerHeardNeighbors.js';
import { OwnerStatusFields } from './owner/OwnerStatusFields.js';
export const OwnerPortalPage: React.FC = () => {
  const { privacyGeneration } = useRuntimeFeatures();
  const [mqttUsername, setMqttUsername] = useState('');
  const [mqttPassword, setMqttPassword] = useState('');
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dashboard, setDashboard] = useState<OwnerDashboard | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string>('');
  const [live, setLive] = useState<OwnerLiveResponse | null>(null);
  const [liveError, setLiveError] = useState<string | null>(null);
  const [lastHopStrength, setLastHopStrength] = useState<LastHopStrengthPoint[]>([]);
  const [ownerSessionKey, setOwnerSessionKey] = useState<string | null>(null);

  const clearOwnerSession = () => {
    lastHopSeriesCache.clear();
    setDashboard(null);
    setOwnerSessionKey(null);
    setSelectedNodeId('');
    setLive(null);
    setLiveError(null);
    setLastHopStrength([]);
    setError(null);
    publishOwnerSession(null);
  };

  useEffect(() => {
    const controller = new AbortController();
    fetchJson<OwnerSessionResponse>(
      '/api/owner/session',
      { cache: 'no-store', signal: controller.signal },
      { timeoutMs: 10_000, maxBytes: 2 * 1024 * 1024, validate: isOwnerSessionResponse },
    )
      .then((json) => {
        if (controller.signal.aborted) return;
        setDashboard(json.dashboard);
        setOwnerSessionKey(json.mqttUsername ?? null);
        publishOwnerSession(json.mqttUsername ?? null);
        if (json.dashboard.nodes[0]?.node_id) {
          setSelectedNodeId(json.dashboard.nodes[0].node_id);
        }
        setLoading(false);
      })
      .catch(() => {
        if (controller.signal.aborted) return;
        clearOwnerSession();
        setLoading(false);
      });
    return () => controller.abort();
  // Session bootstrap runs once. Subsequent authenticated refreshes are owned
  // by the visibility-aware poller below.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Keep the node list in sync with keys learned from MQTT after login. The
  // backend resolves the username on every session request, so a newly attached
  // observer appears without making the owner log out and back in.
  const hasDashboard = dashboard !== null;
  useVisibilityPoll(async (signal) => {
    const json = await fetchJson<OwnerSessionResponse>(
      '/api/owner/session',
      { cache: 'no-store', signal },
      { timeoutMs: 10_000, maxBytes: 2 * 1024 * 1024, validate: isOwnerSessionResponse },
    );
    if (signal.aborted) return;
    setDashboard(json.dashboard);
    setOwnerSessionKey(json.mqttUsername ?? ownerSessionKey);
    publishOwnerSession(json.mqttUsername ?? ownerSessionKey);
    setSelectedNodeId((current) => (
      json.dashboard.nodes.some((node) => node.node_id === current)
        ? current
        : (json.dashboard.nodes[0]?.node_id ?? '')
    ));
  }, {
    enabled: hasDashboard,
    scopeKey: `owner-session:${ownerSessionKey ?? 'pending'}`,
    intervalMs: 15_000,
    timeoutMs: 10_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
      }
    },
  });

  const handleLogin = (event: FormEvent) => {
    event.preventDefault();
    if (!mqttUsername.trim() || !mqttPassword) {
      setError('Enter your MQTT username and password.');
      return;
    }
    setSubmitting(true);
    setError(null);
    const loginUsername = mqttUsername.trim();
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<OwnerSessionResponse>('/api/owner/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken,
          },
          body: JSON.stringify({
            mqttUsername: loginUsername,
            mqttPassword,
          }),
        }, {
          timeoutMs: 15_000,
          maxBytes: 2 * 1024 * 1024,
          validate: isOwnerSessionResponse,
        }),
      )
      .then((json) => {
        lastHopSeriesCache.clear();
        setDashboard(json.dashboard);
        setOwnerSessionKey(json.mqttUsername ?? loginUsername);
        publishOwnerSession(json.mqttUsername ?? loginUsername);
        if (json.dashboard.nodes[0]?.node_id) {
          setSelectedNodeId(json.dashboard.nodes[0].node_id);
        }
        setMqttUsername('');
        setMqttPassword('');
      })
      .catch((err: Error) => {
        setError(err.message);
      })
      .finally(() => setSubmitting(false));
  };

  const handleLogout = () => {
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<{ ok: boolean }>('/api/owner/logout', {
          method: 'POST',
          headers: { 'X-CSRF-Token': csrfToken },
        }, {
          timeoutMs: 10_000,
          maxBytes: 8 * 1024,
          validate: (value): value is { ok: boolean } => isRecord(value) && value['ok'] === true,
        }),
      )
      .finally(() => {
        clearOwnerSession();
      });
  };

  useEffect(() => {
    setLive(null);
    setLiveError(null);
  }, [ownerSessionKey, selectedNodeId]);

  useVisibilityPoll(async (signal) => {
    const json = await fetchJson<OwnerLiveResponse>(
      `/api/owner/live?nodeId=${encodeURIComponent(selectedNodeId)}`,
      { cache: 'no-store', signal },
      { timeoutMs: 10_000, maxBytes: 4 * 1024 * 1024, validate: isOwnerLiveResponse },
    );
    if (signal.aborted) return;
    setLive(json);
    setLiveError(null);
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: `owner-live:${ownerSessionKey ?? 'none'}:${privacyGeneration}:${selectedNodeId}`,
    intervalMs: 10_000,
    timeoutMs: 10_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setLiveError(pollError instanceof Error ? pollError.message : 'Unable to load live data');
    },
  });

  const lastHopScope = `owner:${ownerSessionKey ?? 'none'}|privacy:${privacyGeneration}`;
  useEffect(() => {
    const cached = selectedNodeId ? lastHopSeriesCache.get(lastHopScope, selectedNodeId) : undefined;
    setLastHopStrength(cached ?? []);
  }, [lastHopScope, selectedNodeId]);

  useVisibilityPoll(async (signal) => {
    const points = await lastHopSeriesCache.getOrLoad(lastHopScope, selectedNodeId, async () => {
      const json = await fetchJson<OwnerLastHopStrengthResponse>(
        `/api/owner/live-last-hop?nodeId=${encodeURIComponent(selectedNodeId)}`,
        { cache: 'no-store', signal },
        { timeoutMs: 15_000, maxBytes: 8 * 1024 * 1024, validate: isOwnerLastHopStrengthResponse },
      );
      return json.points;
    });
    if (!signal.aborted) setLastHopStrength(points);
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: `owner-last-hop:${lastHopScope}:${selectedNodeId}`,
    intervalMs: 60_000,
    timeoutMs: 15_000,
  });

  const mapPoints = useMemo(() => {
    const ownerNode = live?.ownerNode;
    const points: Array<{ lat: number; lon: number }> = [];
    if (isValidMapCoord(ownerNode?.lat ?? null, ownerNode?.lon ?? null)) {
      points.push({ lat: ownerNode!.lat as number, lon: ownerNode!.lon as number });
    }
    for (const peer of live?.incomingPeers ?? []) {
      if (!isValidMapCoord(peer.lat, peer.lon)) continue;
      points.push({ lat: peer.lat as number, lon: peer.lon as number });
    }
    return points;
  }, [live]);

  const ownerCoord = useMemo(() => {
    const ownerNode = live?.ownerNode;
    const lat = ownerNode?.lat ?? null;
    const lon = ownerNode?.lon ?? null;
    if (!isValidMapCoord(lat, lon)) return null;
    return { lat: lat as number, lon: lon as number };
  }, [live]);

  const mapPeers = useMemo<MappedPeer[]>(
    () => (live?.incomingPeers ?? [])
      .filter((peer) => isValidMapCoord(peer.lat, peer.lon))
      .map((peer) => ({ ...peer, lat: peer.lat as number, lon: peer.lon as number })),
    [live],
  );

  const strongestLink = useMemo(() => {
    const links = live?.linkHealth ?? [];
    return links
      .filter((link) => link.itm_path_loss_db != null)
      .sort((a, b) => (a.itm_path_loss_db ?? Number.POSITIVE_INFINITY) - (b.itm_path_loss_db ?? Number.POSITIVE_INFINITY))[0] ?? null;
  }, [live]);

  const viableLinkCount = useMemo(
    () => (live?.linkHealth ?? []).filter((link) => link.itm_viable || link.force_viable).length,
    [live],
  );

  const latestTelemetry = useMemo(() => {
    const points = live?.telemetry24h ?? [];
    return points.length > 0 ? points[points.length - 1]! : null;
  }, [live]);

  return (
    <>

      <div
        className="site-content site-prose site-prose--wide"
        data-update-blocking={submitting || Boolean(mqttUsername) || Boolean(mqttPassword) ? 'true' : undefined}
      >
        {loading ? <LoadingIndicator label="Checking login session..." variant="block" /> : null}
        {!loading && !dashboard ? (
          <OwnerLoginSection username={mqttUsername} password={mqttPassword} submitting={submitting} error={error} onUsername={setMqttUsername} onPassword={setMqttPassword} onSubmit={handleLogin} />
        ) : null}

        {!loading && dashboard ? (
          <>
            <section className="prose-section">
              <div className="owner-head">
                <div>
                  <h1>Repeater owner dashboard</h1>
                  <p className="prose-note">Live status, identity history, telemetry, and packet activity for your owned nodes.</p>
                </div>
                <button type="button" className="site-btn site-btn--ghost" onClick={handleLogout}>
                  Logout
                </button>
              </div>
              <div className="owner-node-identities" aria-label="Owned repeater identities">
                {dashboard.nodes.map((node) => (
                  <article
                    key={node.canonicalId}
                    className={`owner-node-identity${node.node_id === selectedNodeId ? ' owner-node-identity--selected' : ''}`}
                    data-canonical-id={node.canonicalId}
                    onClick={() => setSelectedNodeId(node.node_id)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        setSelectedNodeId(node.node_id);
                      }
                    }}
                    role="button"
                    tabIndex={0}
                    aria-pressed={node.node_id === selectedNodeId}
                    title={dashboard.nodes.length > 1 ? `View ${node.name ?? 'this node'}` : undefined}
                  >
                    <div className="owner-node-identity__head">
                      <strong>{node.name ?? 'Unnamed node'}</strong>
                      <span>{node.members.length} member key{node.members.length === 1 ? '' : 's'}</span>
                    </div>
                    <p className="owner-node-identity__canonical">Canonical ID <code>{node.canonicalId}</code></p>
                    <ul className="owner-node-identity__members" aria-label={`Member keys for ${node.name ?? node.canonicalId}`}>
                      {node.members.map((member) => <li key={member}><code>{member}</code></li>)}
                    </ul>
                  </article>
                ))}
              </div>
              <div className="site-stats-grid site-stats-grid--6 owner-summary-grid">
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.name ?? 'Unnamed'}</span><span className="site-stat__label">{nodeRoleLabel(live?.ownerNode.role ?? null)}</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.network ?? '-'}</span><span className="site-stat__label">Network</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.iata ?? '-'}</span><span className="site-stat__label">IATA</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.ownerNode.advert_count ?? 0}</span><span className="site-stat__label">Adverts</span></div>
                <div className="site-stat"><span className="site-stat__value">{fmtTs(live?.ownerNode.last_seen ?? null)}</span><span className="site-stat__label">Last Seen</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.incomingPeers.length ?? 0}</span><span className="site-stat__label">Direct Senders (24h)</span></div>
                <div className="site-stat"><span className="site-stat__value">{viableLinkCount}</span><span className="site-stat__label">Viable Links</span></div>
                <div className="site-stat"><span className="site-stat__value">{strongestLink?.peer_name ?? '-'}</span><span className="site-stat__label">Strongest Link</span></div>
                <div className="site-stat"><span className="site-stat__value">{formatPathLoss(strongestLink?.itm_path_loss_db ?? null)}</span><span className="site-stat__label">Best Path Loss</span></div>
                <div className="site-stat"><span className="site-stat__value">{(live?.advertTrend24h ?? []).reduce((sum, point) => sum + point.adverts, 0)}</span><span className="site-stat__label">Adverts (24h)</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.packetsSent24h ?? 0}</span><span className="site-stat__label">Packets Sent (24h)</span></div>
                <div className="site-stat"><span className="site-stat__value">{live?.packetsReceived24h ?? 0}</span><span className="site-stat__label">Packets Received (24h)</span></div>
              </div>
              {liveError ? <p className="prose-note owner-login__error">Live data error: {liveError}</p> : null}
            </section>

            <section className="owner-panel owner-telemetry-panel">
              <div className="owner-panel__head">
                <div>
                  <h2>Node Telemetry</h2>
                  <p className="prose-note">Battery level and rolling radio utilisation from MQTT status samples over the last 24 hours.</p>
                </div>
              </div>
              <div className="owner-telemetry-strip">
                {TELEMETRY_SERIES.map((series) => (
                  <TelemetryMiniChart
                    key={series.key}
                    title={series.title}
                    stroke={series.stroke}
                    suffix={series.suffix}
                    points={live?.telemetry24h ?? []}
                    metric={series.key}
                    meta={series.meta}
                  />
                ))}
                <TelemetryStatCard
                  title="Uptime"
                  value={formatUptime(latestTelemetry?.uptimeSecs ?? null)}
                  meta={latestTelemetry?.uptimeSecs == null ? 'No telemetry yet' : `${latestTelemetry.uptimeSecs}s reported`}
                />
                <LastHopStrengthChart nodeId={selectedNodeId} points={lastHopStrength} isPassiveRepeater={live !== null && (live.incomingPeers.length === 0 && live.recentPackets.length === 0 && live.heardBy.length > 0)} />
              </div>
            </section>

            <section className="owner-panel owner-status-panel">
              <div className="owner-panel__head">
                <div>
                  <h2>Node status</h2>
                  <p className="prose-note">Latest nullable diagnostics reported by the node.</p>
                </div>
                <span className="owner-status-panel__sample">{live?.status?.sampled_at ? fmtTs(live.status.sampled_at) : 'No sample'}</span>
              </div>
              <OwnerStatusFields status={live?.status ?? null} />
            </section>

            <div className="owner-dashboard-grid">
              <section className="prose-section owner-panel owner-panel--map">
                <div className="owner-panel__head">
                  <div><h2>Direct Sender Map</h2></div>
                </div>
                <div className="owner-map-wrap">
                  <OwnerMapView ownerCoord={ownerCoord} peers={mapPeers} allPoints={mapPoints} />
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--alerts">
                <div className="owner-panel__head"><h2>Alerts</h2></div>
                <div className="owner-alerts">
                  {(live?.alerts ?? []).map((alert, idx) => (
                    <article key={`${alert.level}-${idx}`} className={`owner-alert owner-alert--${alert.level}`}>
                      <strong>{alert.level.toUpperCase()}</strong>
                      <span>{alert.message}</span>
                    </article>
                  ))}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--trend">
                <div className="owner-panel__head"><h2>Advert Trend</h2></div>
                <TrendBars points={live?.advertTrend24h ?? []} />
              </section>

              <section className="prose-section owner-panel owner-panel--links">
                <div className="owner-panel__head"><h2>RF Link Health</h2></div>
                <div className="owner-list">
                  {(live?.linkHealth ?? []).slice(0, 8).map((link) => (
                    <article key={link.peer_node_id} className="owner-list__row">
                      <div className="owner-list__primary">
                        <strong>{link.peer_name ?? link.peer_node_id}</strong>
                        <span>{link.peer_network ?? '-'}</span>
                      </div>
                      <div className="owner-list__metrics">
                        <span>{linkBadge(link)}</span>
                        <span>{formatPathLoss(link.itm_path_loss_db)}</span>
                        <span>{link.owner_to_peer}/{link.peer_to_owner}</span>
                        <span>{link.observed_count} obs</span>
                      </div>
                    </article>
                  ))}
                  {(live?.linkHealth ?? []).length === 0 ? (
                    <p className="prose-note">No link health data has been calculated for this node yet.</p>
                  ) : null}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--senders">
                <div className="owner-panel__head"><h2>Direct Senders</h2></div>
                <div className="owner-list">
                  {(live?.incomingPeers ?? []).slice(0, 8).map((peer) => (
                    <article key={peer.node_id} className="owner-list__row">
                      <div className="owner-list__primary">
                        <strong>{peer.name ?? peer.node_id}</strong>
                        <span>{peer.network ?? '-'} · {peer.iata ?? '-'}</span>
                      </div>
                      <div className="owner-list__metrics">
                        <span>{peer.packets_24h} / 24h</span>
                        <span>{formatCompactTs(peer.last_seen)}</span>
                      </div>
                    </article>
                  ))}
                  {(live?.incomingPeers ?? []).length === 0 ? (
                    <p className="prose-note">No direct sender nodes found in the last 24 hours.</p>
                  ) : null}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--senders">
                <div className="owner-panel__head">
                  <div>
                    <h2>Nodes That Heard This Repeater</h2>
                    <p className="prose-note" style={{ marginTop: 0 }}>Other nodes that received packets transmitted by this repeater (last 7d)</p>
                  </div>
                </div>
                <div className="owner-list">
                  {(live?.heardBy ?? []).slice(0, 8).map((peer) => (
                    <article key={peer.node_id} className="owner-list__row">
                      <div className="owner-list__primary">
                        <strong>{peer.name ?? peer.node_id}</strong>
                        <span>{peer.network ?? '-'} · {peer.iata ?? '-'}</span>
                      </div>
                      <div className="owner-list__metrics">
                        <span>{peer.packets_24h} / 24h</span>
                        <span>{peer.packets_7d} / 7d</span>
                        <span>{peer.best_hops != null ? `${peer.best_hops} hops` : '-'}</span>
                        <span>{formatCompactTs(peer.last_seen)}</span>
                      </div>
                    </article>
                  ))}
                  {(live?.heardBy ?? []).length === 0 ? (
                    <p className="prose-note">No nodes have received packets from this node in the last 7 days.</p>
                  ) : null}
                </div>
              </section>

              <section className="prose-section owner-panel owner-panel--neighbors">
                <div className="owner-panel__head">
                  <div>
                    <h2>Heard neighbors</h2>
                    <p className="prose-note" style={{ marginTop: 0 }}>The latest neighbor sample, sorted by last-seen recency (up to 32 nodes).</p>
                  </div>
                </div>
                <OwnerHeardNeighbors neighbors={live?.heardNeighbors ?? []} />
              </section>

              <section className="prose-section owner-panel owner-panel--packets">
                <div className="owner-panel__head"><h2>Live Packets Received By {nodeRoleLabel(live?.ownerNode.role ?? null)}</h2></div>
                <div className="owner-packets">
                  {(live?.recentPackets ?? []).map((packet, idx) => (
                    <article key={`${packet.time}-${packet.packet_hash ?? `row-${idx}`}`} className="owner-packet">
                      <div className="owner-packet__head">
                        <strong>{PACKET_LABELS[Number(packet.packet_type ?? -1)] ?? `Type ${packet.packet_type ?? '?'}`}</strong>
                        <span>{fmtTs(packet.time)}</span>
                      </div>
                      <div className="owner-packet__meta">
                        <span>From: {packet.src_node_name ?? packet.src_node_id ?? '-'}</span>
                        <span>Sender: {packet.sender ?? '-'}</span>
                        <span>Hops: {packet.hop_count ?? '-'}</span>
                        <span>Route: {ROUTE_LABELS[Number(packet.route_type ?? -1)] ?? (packet.route_type ?? '-')}</span>
                      </div>
                      {cleanPacketBody(packet) ? <p className="owner-packet__body">{cleanPacketBody(packet)}</p> : null}
                    </article>
                  ))}
                  {(live?.recentPackets ?? []).length === 0 ? (
                    <p className="prose-note">No packets received by this node yet.</p>
                  ) : null}
                </div>
              </section>
            </div>
          </>
        ) : null}
      </div>
    </>
  );
};
