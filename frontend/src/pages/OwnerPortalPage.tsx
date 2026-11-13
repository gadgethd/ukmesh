import './docs-pages.css';
import './owner-portal.css';
import React from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { OwnerLoginSection, OwnerPacketSharingSection, OwnerPublicFeedSection } from '../components/owner/OwnerPortalSections.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { useOwnerSession } from '../features/owner/useOwnerSession.js';
import { useOwnerSharing } from '../features/owner/useOwnerSharing.js';
import { useOwnerLive } from '../features/owner/useOwnerLive.js';
import { PACKET_LABELS, ROUTE_LABELS, cleanPacketBody, fmtTs, formatCompactTs, formatPathLoss,
  isPacketSharingOwner, linkBadge, nodeRoleLabel, type LivePeer } from '../features/owner/model.js';
import { LastHopStrengthChart, TELEMETRY_SERIES, TelemetryMiniChart, TelemetryStatCard,
  TrendBars, formatUptime } from './owner/OwnerPortalCharts.js';
import { OwnerMapView } from './owner/OwnerMapView.js';
import { OwnerPanel, OwnerPanelState } from './owner/OwnerPanelState.js';
import { OwnerHeardNeighbors } from './owner/OwnerHeardNeighbors.js';
import { OwnerStatusFields } from './owner/OwnerStatusFields.js';

export const OwnerPortalPage: React.FC = () => {
  const { privacyGeneration } = useRuntimeFeatures();
  const session = useOwnerSession();
  const { mqttUsername, setMqttUsername, mqttPassword, setMqttPassword, loading, submitting, error,
    dashboard, selectedNodeId, setSelectedNodeId, ownerSessionKey, ownerSessionGeneration, handleLogin, handleLogout } = session;
  const { packetSharing, packetSharingLoading, packetSharingSaving, packetSharingError,
    packetSharingDestinationIds, packetSharingHasDraft, retryPacketSharing, changePacketSharingDestination, publicFeed, publicFeedLoading,
    publicFeedSaving, publicFeedError, publicFeedDraft, retryPublicFeed, savePublicFeed } = useOwnerSharing({
      hasDashboard: dashboard !== null, ownerSessionKey, ownerSessionGeneration, privacyGeneration, selectedNodeId, clearOwnerSession: session.clearOwnerSession });
  const { live, livePanelState, mapPoints, ownerCoord, mapPeers, strongestLink, viableLinkCount,
    latestTelemetry, compareNodeIds, setCompareNodeIds, toggleCompareNode, compareChartSeries,
    lastHopResult, lastHopViewScope, lastHopError, retryLastHop, lastHopStrength, lastHopWindow,
    setLastHopWindow } = useOwnerLive({ privacyGeneration, ownerSessionGeneration, dashboard, selectedNodeId,
      setSelectedNodeId, ownerSessionKey, clearOwnerSession: session.clearOwnerSession });

  const peerGroups: { label: string; peers: (LivePeer & { packets_7d?: number; best_hops?: number | null })[]; empty: string; description?: string }[] = [
    { label: 'Direct Senders', peers: live?.incomingPeers ?? [], empty: 'No direct sender nodes found in the last 24 hours.' },
    { label: 'Nodes That Heard This Repeater', peers: live?.heardBy ?? [], empty: 'No nodes have received packets from this node in the last 7 days.',
      description: 'Other nodes that received packets transmitted by this repeater (last 7d)' },
  ];

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
              {dashboard.mqttDiagnostic ? (
                <div className="prose-note" role="status">
                  <strong>Recent MQTT publish denial</strong>
                  <p>{dashboard.mqttDiagnostic.message}</p>
                  <p>Last observed: {fmtTs(dashboard.mqttDiagnostic.observedAt)}. This warning expires 15 minutes after the last denial.</p>
                  <a href="/install#mqtt-observer">MQTT setup and troubleshooting</a>
                </div>
              ) : null}
              <div className="owner-node-identities" aria-label="Owned repeater identities">
                {dashboard.nodes.map((node) => (
                  <article
                    key={node.canonicalId}
                    className={`owner-node-identity${node.node_id === selectedNodeId ? ' owner-node-identity--selected' : ''}${compareNodeIds.includes(node.node_id) ? ' owner-node-identity--comparing' : ''}`}
                    data-canonical-id={node.canonicalId}
                    onClick={() => {
                      setSelectedNodeId(node.node_id);
                      if (compareNodeIds.length > 1) setCompareNodeIds([node.node_id]);
                    }}
                    onKeyDown={(e) => {
                      if (e.target !== e.currentTarget) return;
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        setSelectedNodeId(node.node_id);
                        if (compareNodeIds.length > 1) setCompareNodeIds([node.node_id]);
                      }
                    }}
                    role="button"
                    tabIndex={0}
                    aria-pressed={node.node_id === selectedNodeId}
                    title={dashboard.nodes.length > 1 ? `View ${node.name ?? 'this node'}` : undefined}
                  >
                    {dashboard.nodes.length > 1 ? (
                      <button
                        type="button"
                        className={`owner-node-identity__compare${compareNodeIds.includes(node.node_id) ? ' owner-node-identity__compare--active' : ''}`}
                        aria-pressed={compareNodeIds.includes(node.node_id)}
                        aria-label={`${compareNodeIds.includes(node.node_id) ? 'Remove' : 'Add'} ${node.name ?? 'this repeater'} ${compareNodeIds.includes(node.node_id) ? 'from' : 'to'} the RX strength comparison`}
                        title={compareNodeIds.includes(node.node_id) ? 'Remove from comparison' : 'Add to comparison'}
                        onClick={(event) => {
                          event.stopPropagation();
                          toggleCompareNode(node.node_id);
                        }}
                      >
                        {compareNodeIds.includes(node.node_id) ? '✓' : '+'}
                      </button>
                    ) : null}
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
              <OwnerPanelState label="Live summary" {...livePanelState}>
              <div className="site-stats-grid site-stats-grid--6 owner-summary-grid">
                {[
                  [nodeRoleLabel(live?.ownerNode.role ?? null), live?.ownerNode.name ?? 'Unnamed'],
                  ['Network', live?.ownerNode.network ?? '-'], ['IATA', live?.ownerNode.iata ?? '-'],
                  ['Adverts', live?.ownerNode.advert_count ?? 0], ['Last Seen', fmtTs(live?.ownerNode.last_seen ?? null)],
                  ['Direct Senders (24h)', live?.incomingPeers.length ?? 0], ['Viable Links', viableLinkCount],
                  ['Strongest Link', strongestLink?.peer_name ?? '-'], ['Best Path Loss', formatPathLoss(strongestLink?.itm_path_loss_db ?? null)],
                  ['Adverts (24h)', (live?.advertTrend24h ?? []).reduce((sum, point) => sum + point.adverts, 0)],
                  ['Packets Sent (24h)', live?.packetsSent24h ?? 0], ['Packets Received (24h)', live?.packetsReceived24h ?? 0],
                ].map(([label, value]) => <div className="site-stat" key={label}>
                  <span className="site-stat__value">{value}</span><span className="site-stat__label">{label}</span>
                </div>)}
              </div>
              </OwnerPanelState>
            </section>

            <section className="owner-panel owner-telemetry-panel">
              <div className="owner-panel__head">
                <div>
                  <h2>Node Telemetry</h2>
                  <p className="prose-note">Battery level and rolling radio utilisation from MQTT status samples over the last 24 hours.</p>
                </div>
              </div>
              <div className="owner-telemetry-strip">
                <OwnerPanelState label="Node telemetry" {...livePanelState}>
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
                </OwnerPanelState>
                <OwnerPanelState label="RX strength" hasData={lastHopResult?.scope === lastHopViewScope} error={lastHopError} onRetry={retryLastHop}>
                <LastHopStrengthChart nodeId={selectedNodeId} points={lastHopStrength} timeWindow={lastHopWindow} onTimeWindowChange={setLastHopWindow} isPassiveRepeater={live !== null && (live.incomingPeers.length === 0 && live.recentPackets.length === 0 && live.heardBy.length > 0)} compareSeries={compareChartSeries} />
                </OwnerPanelState>
              </div>
            </section>

            <section className="owner-panel owner-status-panel">
              <div className="owner-panel__head">
                <div>
                  <h2>Node status</h2>
                  <p className="prose-note">Latest nullable diagnostics reported by the node.</p>
                </div>
                <span className="owner-status-panel__sample">{live ? (live.status?.sampled_at ? fmtTs(live.status.sampled_at) : 'No sample') : '—'}</span>
              </div>
              <OwnerPanelState label="Node status" {...livePanelState}>
                <OwnerStatusFields status={live?.status ?? null} />
              </OwnerPanelState>
            </section>

            <div className="owner-dashboard-grid">
              <OwnerPanel className="prose-section owner-panel owner-panel--map" label="Direct Sender Map" {...livePanelState}>
                <div className="owner-map-wrap">
                  <OwnerMapView ownerCoord={ownerCoord} peers={mapPeers} allPoints={mapPoints} />
                </div>
                </OwnerPanel>

              <OwnerPanel className="prose-section owner-panel owner-panel--alerts" label="Alerts" {...livePanelState}>
                <div className="owner-alerts">
                  {live?.alerts.length === 0 ? <p className="prose-note">No active alerts.</p> : null}
                  {(live?.alerts ?? []).map((alert, idx) => (
                    <article key={`${alert.level}-${idx}`} className={`owner-alert owner-alert--${alert.level}`}>
                      <strong>{alert.level.toUpperCase()}</strong>
                      <span>{alert.message}</span>
                    </article>
                  ))}
                </div>
                </OwnerPanel>

              <OwnerPanel className="prose-section owner-panel owner-panel--trend" label="Advert Trend" {...livePanelState}>
                {(live?.advertTrend24h ?? []).some((point) => point.adverts > 0)
                  ? <TrendBars points={live?.advertTrend24h ?? []} />
                  : <p className="prose-note">No adverts observed in the last 24 hours.</p>}
                </OwnerPanel>

              <OwnerPanel className="prose-section owner-panel owner-panel--links" label="RF Link Health" {...livePanelState}>
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
                </OwnerPanel>

              {peerGroups.map(({ label, peers, empty, description }) => <OwnerPanel key={label} className="prose-section owner-panel owner-panel--senders"
                label={label} description={description} {...livePanelState}>
                <div className="owner-list">
                  {peers.slice(0, 8).map(peer => <article key={peer.node_id} className="owner-list__row">
                    <div className="owner-list__primary">
                      <strong>{peer.name ?? peer.node_id}</strong><span>{peer.network ?? '-'} · {peer.iata ?? '-'}</span>
                    </div>
                    <div className="owner-list__metrics">
                      <span>{peer.packets_24h} / 24h</span>
                      {description && <><span>{peer.packets_7d} / 7d</span><span>{peer.best_hops != null ? `${peer.best_hops} hops` : '-'}</span></>}
                      <span>{formatCompactTs(peer.last_seen)}</span>
                    </div>
                  </article>)}
                  {peers.length === 0 && <p className="prose-note">{empty}</p>}
                </div>
              </OwnerPanel>)}

              <OwnerPanel className="prose-section owner-panel owner-panel--neighbors" label="Heard neighbors" description={'Reported neighbors or directly received senders, sorted by last-seen recency (up to 32 nodes).'} {...livePanelState}>
                <OwnerHeardNeighbors neighbors={live?.heardNeighbors ?? []} source={live?.heardNeighborsSource} sampledAt={live?.heardNeighborsSampledAt} />
                </OwnerPanel>

              <OwnerPanel className="prose-section owner-panel owner-panel--packets" label="Live packets" title={<>Live Packets Received By {nodeRoleLabel(live?.ownerNode.role ?? null)}</>} {...livePanelState}>
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
                    <p className="prose-note">No packets received by this node in the last 30 days.</p>
                  ) : null}
                </div>
                </OwnerPanel>
            </div>

            {isPacketSharingOwner(ownerSessionKey) ? (
              <OwnerPacketSharingSection
                settings={packetSharing}
                loading={packetSharingLoading}
                saving={packetSharingSaving}
                error={packetSharingError}
                selectedDestinationIds={packetSharingDestinationIds}
                hasDraft={packetSharingHasDraft}
                onRetry={retryPacketSharing}
                onDestination={changePacketSharingDestination}
              />
            ) : null}
            <OwnerPublicFeedSection
              settings={publicFeed}
              loading={publicFeedLoading}
              saving={publicFeedSaving}
              error={publicFeedError}
              onEnabled={savePublicFeed}
              draftEnabled={publicFeedDraft}
              onRetry={retryPublicFeed}
            />
          </>
        ) : null}
      </div>
    </>
  );
};
