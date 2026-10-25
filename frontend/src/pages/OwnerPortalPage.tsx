import React, { FormEvent, useEffect, useMemo, useState } from 'react';
import './owner-portal.css';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { OwnerLoginSection, OwnerPacketSharingSection, OwnerPublicFeedSection } from '../components/owner/OwnerPortalSections.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { useVisibilityPoll } from '../hooks/useVisibilityPoll.js';
import { ApiResponseError, fetchJson } from '../utils/api.js';

import {
  PACKET_LABELS,
  ROUTE_LABELS,
  cleanPacketBody,
  fetchOwnerCsrfToken,
  fetchOwnerPacketSharing,
  fetchOwnerPublicFeed,
  fmtTs,
  formatCompactTs,
  formatPathLoss,
  isOwnerLastHopStrengthResponse,
  isOwnerLiveResponse,
  isOwnerPublicFeedResponse,
  isOwnerSessionResponse,
  isPacketSharingOwner,
  isRecord,
  isValidMapCoord,
  lastHopSeriesCache,
  linkBadge,
  nodeRoleLabel,
  ownerLoginCredentials,
  ownerLoginValidationError,
  publishOwnerSession,
  type LastHopStrengthPoint,
  type LastHopWindow,
  type MappedPeer,
  type OwnerDashboard,
  type OwnerLastHopStrengthResponse,
  type OwnerLiveResponse,
  type OwnerPacketSharing,
  type OwnerPublicFeed,
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
import { OwnerPanelState } from './owner/OwnerPanelState.js';
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
  const [liveResult, setLiveResult] = useState<{ scope: string; data: OwnerLiveResponse } | null>(null);
  const [liveLoading, setLiveLoading] = useState(false);
  const [liveError, setLiveError] = useState<string | null>(null);
  const [lastHopResult, setLastHopResult] = useState<{ scope: string; points: LastHopStrengthPoint[] } | null>(null);
  const [lastHopError, setLastHopError] = useState<string | null>(null);
  const [lastHopLoading, setLastHopLoading] = useState(false);
  const [lastHopWindow, setLastHopWindow] = useState<LastHopWindow>('7d');
  const [compareNodeIds, setCompareNodeIds] = useState<string[]>([]);
  const [compareSeries, setCompareSeries] = useState<Array<{ id: string; points: LastHopStrengthPoint[] }>>([]);
  const [ownerSessionKey, setOwnerSessionKey] = useState<string | null>(null);
  const [packetSharing, setPacketSharing] = useState<OwnerPacketSharing | null>(null);
  const [packetSharingLoading, setPacketSharingLoading] = useState(false);
  const [packetSharingSaving, setPacketSharingSaving] = useState(false);
  const [packetSharingError, setPacketSharingError] = useState<string | null>(null);
  const [packetSharingDestinationIds, setPacketSharingDestinationIds] = useState<string[]>([]);
  const [publicFeed, setPublicFeed] = useState<OwnerPublicFeed | null>(null);
  const [publicFeedLoading, setPublicFeedLoading] = useState(false);
  const [publicFeedSaving, setPublicFeedSaving] = useState(false);
  const [publicFeedError, setPublicFeedError] = useState<string | null>(null);

  const liveScope = `owner-live:${ownerSessionKey ?? 'none'}:${privacyGeneration}:${selectedNodeId}`;
  const live = liveResult?.scope === liveScope ? liveResult.data : null;
  const lastHopViewScope = `${liveScope}|window:${lastHopWindow}`;
  const lastHopStrength = lastHopResult?.scope === lastHopViewScope ? lastHopResult.points : [];
  const compareKey = compareNodeIds.join('|');
  const compareActive = compareNodeIds.length >= 2;
  const compareChartSeries = useMemo(() => {
    if (!compareActive || compareSeries.length < 2) return undefined;
    return compareSeries.map((entry) => ({
      id: entry.id,
      name: dashboard?.nodes.find((node) => node.node_id === entry.id)?.name ?? entry.id.slice(0, 8),
      points: entry.points,
    }));
  }, [compareActive, compareSeries, dashboard]);

  const clearOwnerSession = () => {
    lastHopSeriesCache.clear();
    setDashboard(null);
    setOwnerSessionKey(null);
    setSelectedNodeId('');
    setLiveResult(null);
    setLiveError(null);
    setLastHopResult(null);
    setCompareNodeIds([]);
    setCompareSeries([]);
    setPacketSharing(null);
    setPacketSharingDestinationIds([]);
    setPacketSharingError(null);
    setPublicFeed(null);
    setPublicFeedLoading(false);
    setPublicFeedSaving(false);
    setPublicFeedError(null);
    setError(null);
    publishOwnerSession(null);
  };

  // Toggles a repeater in/out of the RX-strength comparison set. The first toggle
  // seeds the set with the current primary node so a newly started comparison
  // always contains what the owner was just looking at.
  const toggleCompareNode = (nodeId: string) => {
    setCompareNodeIds((current) => {
      if (current.includes(nodeId)) {
        const next = current.filter((id) => id !== nodeId);
        if (next.length === 1) setSelectedNodeId(next[0]!);
        return next;
      }
      const base = current.length > 0 ? current : (selectedNodeId ? [selectedNodeId] : []);
      return base.includes(nodeId) ? base : [...base, nodeId];
    });
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
    const validationError = ownerLoginValidationError(mqttUsername, mqttPassword);
    if (validationError) {
      setError(validationError);
      return;
    }
    setSubmitting(true);
    setError(null);
    const credentials = ownerLoginCredentials(mqttUsername, mqttPassword);
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<OwnerSessionResponse>('/api/owner/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrfToken,
          },
          body: JSON.stringify(credentials),
        }, {
          timeoutMs: 15_000,
          maxBytes: 2 * 1024 * 1024,
          validate: isOwnerSessionResponse,
          readErrorMessage: true,
        }),
      )
      .then((json) => {
        lastHopSeriesCache.clear();
        setDashboard(json.dashboard);
        setOwnerSessionKey(json.mqttUsername ?? credentials.mqttUsername);
        publishOwnerSession(json.mqttUsername ?? credentials.mqttUsername);
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
    setLiveResult(null);
    setLiveError(null);
    setLiveLoading(false);
  }, [liveScope]);

  useEffect(() => {
    if (!hasDashboard || !isPacketSharingOwner(ownerSessionKey) || !selectedNodeId) {
      setPacketSharing(null);
      setPacketSharingLoading(false);
      setPacketSharingError(null);
      setPacketSharingDestinationIds([]);
      return;
    }
    setPacketSharing(null);
    setPacketSharingLoading(true);
    setPacketSharingError(null);
    setPacketSharingDestinationIds([]);
  }, [hasDashboard, ownerSessionKey, selectedNodeId]);

  const refreshPacketSharing = useVisibilityPoll(async (signal) => {
    if (packetSharingSaving) return;
    const settings = await fetchOwnerPacketSharing(selectedNodeId, signal);
    if (signal.aborted) return;
    setPacketSharing(settings);
    setPacketSharingDestinationIds(settings.destinations.filter((destination) => destination.selected).map((destination) => destination.id));
    setPacketSharingLoading(false);
    setPacketSharingError(null);
  }, {
    enabled: hasDashboard && isPacketSharingOwner(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: `owner-packet-sharing:${ownerSessionKey ?? 'none'}:${selectedNodeId}`,
    intervalMs: 5_000,
    timeoutMs: 10_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setPacketSharingLoading(false);
      setPacketSharingError(pollError instanceof Error ? pollError.message : 'Unable to load packet-sharing status');
    },
  });

  useEffect(() => {
    if (!hasDashboard || !ownerSessionKey) {
      setPublicFeed(null);
      setPublicFeedLoading(false);
      setPublicFeedError(null);
      return;
    }
    setPublicFeed(null);
    setPublicFeedLoading(true);
    setPublicFeedError(null);
  }, [hasDashboard, ownerSessionKey]);

  const refreshPublicFeed = useVisibilityPoll(async (signal) => {
    if (publicFeedSaving) return;
    const settings = await fetchOwnerPublicFeed(signal);
    if (signal.aborted) return;
    setPublicFeed(settings);
    setPublicFeedLoading(false);
    setPublicFeedError(null);
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey),
    scopeKey: `owner-public-feed:${ownerSessionKey ?? 'none'}`,
    intervalMs: 5_000,
    timeoutMs: 10_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setPublicFeedLoading(false);
      setPublicFeedError(pollError instanceof Error ? pollError.message : 'Unable to load public-feed status');
    },
  });

  const savePublicFeed = (enabled: boolean) => {
    if (!publicFeed || !publicFeed.featureEnabled) return;
    setPublicFeedSaving(true);
    setPublicFeedError(null);
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<OwnerPublicFeed>('/api/owner/public-feed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ enabled }),
      }, {
        timeoutMs: 10_000,
        maxBytes: 32 * 1024,
        validate: (value): value is OwnerPublicFeed => isOwnerPublicFeedResponse(value),
      }))
      .then((result) => setPublicFeed(result))
      .catch((saveError: Error) => setPublicFeedError(saveError.message))
      .finally(() => {
        setPublicFeedSaving(false);
        refreshPublicFeed();
      });
  };

  const savePacketSharing = (nextDestinationIds: string[]) => {
    if (!packetSharing || !packetSharing.featureEnabled) return;
    const destinationIds = Array.from(new Set(nextDestinationIds)).sort();
    setPacketSharingSaving(true);
    setPacketSharingError(null);
    fetchOwnerCsrfToken()
      .then((csrfToken) => fetchJson<{ nodeId: string; enabled: boolean; destinationIds: string[] }>('/api/owner/packet-sharing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({
          nodeId: selectedNodeId,
          enabled: destinationIds.length > 0,
          destinationIds,
        }),
      }, {
        timeoutMs: 10_000,
        maxBytes: 32 * 1024,
        validate: (value): value is { nodeId: string; enabled: boolean; destinationIds: string[] } => (
          isRecord(value)
          && typeof value['nodeId'] === 'string'
          && typeof value['enabled'] === 'boolean'
          && Array.isArray(value['destinationIds'])
          && value['destinationIds'].every((id) => typeof id === 'string')
        ),
      }))
      .then((result) => {
        setPacketSharing((current) => current ? {
          ...current,
          enabled: result.enabled,
          destinations: current.destinations.map((destination) => ({
            ...destination,
            selected: result.destinationIds.includes(destination.id),
          })),
        } : current);
        setPacketSharingDestinationIds(result.destinationIds);
      })
      .catch((saveError: Error) => setPacketSharingError(saveError.message))
      .finally(() => {
        setPacketSharingSaving(false);
        refreshPacketSharing();
      });
  };

  const changePacketSharingDestination = (destinationId: string, selected: boolean) => {
    const nextDestinationIds = selected
      ? Array.from(new Set([...packetSharingDestinationIds, destinationId])).sort()
      : packetSharingDestinationIds.filter((id) => id !== destinationId);
    setPacketSharingDestinationIds(nextDestinationIds);
    savePacketSharing(nextDestinationIds);
  };

  const retryLive = useVisibilityPoll(async (signal) => {
    setLiveLoading(true);
    setLiveError(null);
    const json = await fetchJson<OwnerLiveResponse>(
      `/api/owner/live?nodeId=${encodeURIComponent(selectedNodeId)}`,
      { cache: 'no-store', signal },
      { timeoutMs: 10_000, maxBytes: 4 * 1024 * 1024, validate: isOwnerLiveResponse },
    );
    if (signal.aborted) return;
    setLiveResult({ scope: liveScope, data: json });
    setLiveLoading(false);
    setLiveError(null);
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: liveScope,
    intervalMs: 10_000,
    timeoutMs: 10_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setLiveLoading(false);
      setLiveError(pollError instanceof Error ? pollError.message : 'Unable to load live data');
    },
  });

  const lastHopScope = `owner:${ownerSessionKey ?? 'none'}|privacy:${privacyGeneration}|window:${lastHopWindow}`;
  useEffect(() => {
    const cached = selectedNodeId ? lastHopSeriesCache.get(lastHopScope, selectedNodeId) : undefined;
    setLastHopResult(cached ? { scope: lastHopViewScope, points: cached } : null);
    setLastHopError(null);
    setLastHopLoading(false);
  }, [lastHopScope, lastHopViewScope, selectedNodeId]);

  const retryLastHop = useVisibilityPoll(async (signal) => {
    setLastHopLoading(true);
    setLastHopError(null);
    const points = await lastHopSeriesCache.getOrLoad(lastHopScope, selectedNodeId, async () => {
      const json = await fetchJson<OwnerLastHopStrengthResponse>(
        `/api/owner/live-last-hop?nodeId=${encodeURIComponent(selectedNodeId)}&window=${lastHopWindow}`,
        { cache: 'no-store', signal },
        { timeoutMs: 15_000, maxBytes: 8 * 1024 * 1024, validate: isOwnerLastHopStrengthResponse },
      );
      return json.points;
    });
    if (!signal.aborted) {
      setLastHopResult({ scope: lastHopViewScope, points });
      setLastHopLoading(false);
    }
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && Boolean(selectedNodeId),
    scopeKey: `owner-last-hop:${lastHopScope}:${selectedNodeId}`,
    intervalMs: 60_000,
    timeoutMs: 15_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
        return;
      }
      setLastHopLoading(false);
      setLastHopError(pollError instanceof Error ? pollError.message : 'Unable to load RX strength');
    },
  });

  // Multi-repeater comparison: keep the last-hop series for every repeater in the
  // compare set warm (same per-scope cache as the primary selection) so the RX
  // chart can overlay them. Individual repeater failures are skipped.
  useVisibilityPoll(async (signal) => {
    const results: Array<{ id: string; points: LastHopStrengthPoint[] }> = [];
    for (const nodeId of compareNodeIds) {
      if (signal.aborted) return;
      try {
        const points = await lastHopSeriesCache.getOrLoad(lastHopScope, nodeId, async () => {
          const json = await fetchJson<OwnerLastHopStrengthResponse>(
            `/api/owner/live-last-hop?nodeId=${encodeURIComponent(nodeId)}&window=${lastHopWindow}`,
            { cache: 'no-store' },
            { timeoutMs: 15_000, maxBytes: 8 * 1024 * 1024, validate: isOwnerLastHopStrengthResponse },
          );
          return json.points;
        });
        results.push({ id: nodeId, points });
      } catch {
        // Skip repeaters that fail to load; the chart shows the ones that did.
      }
    }
    if (!signal.aborted) setCompareSeries(results);
  }, {
    enabled: hasDashboard && Boolean(ownerSessionKey) && compareActive,
    scopeKey: `owner-last-hop-compare:${lastHopScope}:${compareKey}`,
    intervalMs: 60_000,
    timeoutMs: 20_000,
    onError: (pollError) => {
      if (pollError instanceof ApiResponseError && pollError.status === 401) {
        clearOwnerSession();
      }
    },
  });

  const livePanelState = { hasData: live !== null, loading: liveLoading, error: liveError, onRetry: retryLive };

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
                <OwnerPanelState label="RX strength" hasData={lastHopResult?.scope === lastHopViewScope} loading={lastHopLoading} error={lastHopError} onRetry={retryLastHop}>
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
              <section className="prose-section owner-panel owner-panel--map">
                <div className="owner-panel__head">
                  <div><h2>Direct Sender Map</h2></div>
                </div>
                <OwnerPanelState label="Direct Sender Map" {...livePanelState}>
                <div className="owner-map-wrap">
                  <OwnerMapView ownerCoord={ownerCoord} peers={mapPeers} allPoints={mapPoints} />
                </div>
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--alerts">
                <div className="owner-panel__head"><h2>Alerts</h2></div>
                <OwnerPanelState label="Alerts" {...livePanelState}>
                <div className="owner-alerts">
                  {live?.alerts.length === 0 ? <p className="prose-note">No active alerts.</p> : null}
                  {(live?.alerts ?? []).map((alert, idx) => (
                    <article key={`${alert.level}-${idx}`} className={`owner-alert owner-alert--${alert.level}`}>
                      <strong>{alert.level.toUpperCase()}</strong>
                      <span>{alert.message}</span>
                    </article>
                  ))}
                </div>
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--trend">
                <div className="owner-panel__head"><h2>Advert Trend</h2></div>
                <OwnerPanelState label="Advert Trend" {...livePanelState}>
                {(live?.advertTrend24h ?? []).some((point) => point.adverts > 0)
                  ? <TrendBars points={live?.advertTrend24h ?? []} />
                  : <p className="prose-note">No adverts observed in the last 24 hours.</p>}
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--links">
                <div className="owner-panel__head"><h2>RF Link Health</h2></div>
                <OwnerPanelState label="RF Link Health" {...livePanelState}>
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
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--senders">
                <div className="owner-panel__head"><h2>Direct Senders</h2></div>
                <OwnerPanelState label="Direct Senders" {...livePanelState}>
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
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--senders">
                <div className="owner-panel__head">
                  <div>
                    <h2>Nodes That Heard This Repeater</h2>
                    <p className="prose-note" style={{ marginTop: 0 }}>Other nodes that received packets transmitted by this repeater (last 7d)</p>
                  </div>
                </div>
                <OwnerPanelState label="Nodes That Heard This Repeater" {...livePanelState}>
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
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--neighbors">
                <div className="owner-panel__head">
                  <div>
                    <h2>Heard neighbors</h2>
                    <p className="prose-note" style={{ marginTop: 0 }}>Reported neighbors or directly received senders, sorted by last-seen recency (up to 32 nodes).</p>
                  </div>
                </div>
                <OwnerPanelState label="Heard neighbors" {...livePanelState}>
                <OwnerHeardNeighbors neighbors={live?.heardNeighbors ?? []} source={live?.heardNeighborsSource} sampledAt={live?.heardNeighborsSampledAt} />
                </OwnerPanelState>
              </section>

              <section className="prose-section owner-panel owner-panel--packets">
                <div className="owner-panel__head"><h2>Live Packets Received By {nodeRoleLabel(live?.ownerNode.role ?? null)}</h2></div>
                <OwnerPanelState label="Live packets" {...livePanelState}>
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
                </OwnerPanelState>
              </section>
            </div>

            {isPacketSharingOwner(ownerSessionKey) ? (
              <OwnerPacketSharingSection
                settings={packetSharing}
                loading={packetSharingLoading}
                saving={packetSharingSaving}
                error={packetSharingError}
                selectedDestinationIds={packetSharingDestinationIds}
                onDestination={changePacketSharingDestination}
              />
            ) : null}
            <OwnerPublicFeedSection
              settings={publicFeed}
              loading={publicFeedLoading}
              saving={publicFeedSaving}
              error={publicFeedError}
              onEnabled={savePublicFeed}
            />
          </>
        ) : null}
      </div>
    </>
  );
};
