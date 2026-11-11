import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { RegionsMap } from '../components/Map/RegionsMap.js';
import { getCurrentSite } from '../config/site.js';
import type { SiteConfig } from '../config/site.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { fetchJson, withScopeParams } from '../utils/api.js';
import {
  compactNumber,
  confidenceFor,
  formatGeneratedAt,
  isEstablished,
  memberIdSet,
  regionColor,
  regionsForDisplay,
  type RegionRepeater,
  type RegionRepeatersPayload,
  type RegionSummary,
  type RegionSummaryPayload,
  type UnattributedPayload,
} from './regionsModel.js';
import './regions-page.css';

const CONFIDENCE_LABELS: Record<string, string> = {
  confirmed: 'Strong evidence — 4 or more scoped packets',
  probable: 'Moderate evidence — 2-3 scoped packets',
  single: 'Single observation — could be chance',
};

export const RegionsPage: React.FC = () => {
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const [summary, setSummary] = useState<RegionSummaryPayload | null>(null);
  const [repeatersPayload, setRepeatersPayload] = useState<RegionRepeatersPayload | null>(null);
  const [unattributedPayload, setUnattributedPayload] = useState<UnattributedPayload | null>(null);
  const [unnamedVisible, setUnnamedVisible] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [shapesVisible, setShapesVisible] = useState(true);
  const [repeatersVisible, setRepeatersVisible] = useState(true);
  const [roleFilter, setRoleFilter] = useState<'repeaters' | 'all'>('repeaters');
  const [statusFilter, setStatusFilter] = useState<'all' | 'green' | 'red'>('all');
  const [includeLowConfidence, setIncludeLowConfidence] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    setSummary(null);
    setRepeatersPayload(null);
    setUnattributedPayload(null);
    setError(null);
    setSelectedRegion(null);
    setSelectedNodeId(null);
    Promise.all([
      fetchJson<RegionSummaryPayload>(
        withScopeParams('/api/regions/summary', { network, observer }),
        { signal: controller.signal, cache: 'no-store' },
        { timeoutMs: 15_000, maxBytes: 8 * 1024 * 1024 },
      ),
      fetchJson<RegionRepeatersPayload>(
        withScopeParams('/api/regions/repeaters', { network, observer }),
        { signal: controller.signal, cache: 'no-store' },
        { timeoutMs: 15_000, maxBytes: 8 * 1024 * 1024 },
      ),
      fetchJson<UnattributedPayload>(
        withScopeParams('/api/regions/unattributed', { network, observer }),
        { signal: controller.signal, cache: 'no-store' },
        { timeoutMs: 15_000, maxBytes: 4 * 1024 * 1024 },
      ).catch(() => null),
    ])
      .then(([nextSummary, nextRepeaters, nextUnattributed]) => {
        if (controller.signal.aborted) return;
        setSummary(nextSummary);
        setRepeatersPayload(nextRepeaters);
        setUnattributedPayload(nextUnattributed);
      })
      .catch((reason: unknown) => {
        if ((reason as DOMException).name !== 'AbortError') setError((reason as Error).message);
      });
    return () => controller.abort();
  }, [network, observer, privacyGeneration]);

  const regions = useMemo(() => summary?.regions ?? [], [summary]);
  const displayRegions = useMemo(
    () => regionsForDisplay(regions, includeLowConfidence),
    [regions, includeLowConfidence],
  );
  const hiddenRegionCount = regions.length - displayRegions.length;
  const repeaters = useMemo(() => repeatersPayload?.repeaters ?? [], [repeatersPayload]);
  const unattributedClusters = useMemo(
    () => unattributedPayload?.clusters ?? [],
    [unattributedPayload],
  );
  const stats = useMemo(() => {
    let scopedPackets = 0;
    let memberships = 0;
    for (const region of regions) {
      scopedPackets += region.packetsTotal;
      memberships += region.memberCount;
    }
    let green = 0;
    let red = 0;
    for (const repeater of repeaters) {
      if (repeater.status === 'green') green += 1;
      else red += 1;
    }
    return {
      regions: regions.length,
      scopedPackets,
      memberships,
      green,
      red,
      unnamedClusters: unattributedPayload?.totals.clusters ?? 0,
      unnamedPackets: unattributedPayload?.totals.packets ?? 0,
    };
  }, [regions, repeaters, unattributedPayload]);
  const selectedRegionData = useMemo(
    () => regions.find((region) => region.name === selectedRegion) ?? null,
    [regions, selectedRegion],
  );
  const selectedRepeater = useMemo(
    () => repeaters.find((repeater) => repeater.nodeId === selectedNodeId) ?? null,
    [repeaters, selectedNodeId],
  );
  const memberIds = useMemo(
    () => (selectedRegionData ? memberIdSet(selectedRegionData) : null),
    [selectedRegionData],
  );
  const handleRegionSelect = useCallback((region: string) => {
    setSelectedRegion(region);
    setSelectedNodeId(null);
  }, []);
  const handleRepeaterSelect = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
  }, []);
  const handleLowConfidenceToggle = useCallback((checked: boolean) => {
    setIncludeLowConfidence(checked);
    if (!checked) {
      // A region that just became hidden must not stay selected.
      setSelectedRegion((current) => {
        const target = current ? regions.find((region) => region.name === current) : undefined;
        return target && !isEstablished(target) ? null : current;
      });
    }
  }, [regions]);
  const generatedLabel = formatGeneratedAt(summary?.generatedAt ?? null);

  return (
    <div className="regions-page site-content">
      <header className="regions-page__header">
        <div>
          <p className="regions-page__eyebrow">Network intelligence</p>
          <h1>MeshCore regions</h1>
          <p>
            MeshCore scoped flood traffic carries a per-region transport code. This map shows the
            regions active on the UK mesh, the repeaters observed relaying scoped traffic (green),
            and repeaters with no scoped relay observed (red). Attribution is a cryptographic
            candidate-name test against the region transport code — private regions stay
            unattributed by design. Region names are community-defined keys, so shapes show where
            scoped traffic is relayed (bridge repeaters excluded) rather than official boundaries.
          </p>
        </div>
        <div className="regions-page__controls">
          <label className="regions-page__toggle">
            <input type="checkbox" checked={shapesVisible} onChange={(event) => setShapesVisible(event.target.checked)} />
            Region shapes
          </label>
          <label className="regions-page__toggle">
            <input type="checkbox" checked={repeatersVisible} onChange={(event) => setRepeatersVisible(event.target.checked)} />
            Repeaters
          </label>
          <label
            className="regions-page__toggle"
            title="Regions with 1-2 observed packets — may be chance matches or one-off tests"
          >
            <input
              type="checkbox"
              checked={includeLowConfidence}
              onChange={(event) => handleLowConfidenceToggle(event.target.checked)}
            />
            Low-confidence regions{hiddenRegionCount > 0 ? ` (${hiddenRegionCount})` : ''}
          </label>
          <label className="regions-page__toggle">
            <input
              type="checkbox"
              checked={unnamedVisible}
              onChange={(event) => setUnnamedVisible(event.target.checked)}
            />
            Unnamed footprints{stats.unnamedClusters > 0 ? ` (${stats.unnamedClusters})` : ''}
          </label>
          <label className="regions-page__toggle">Roles
            <select
              className="regions-page__select"
              value={roleFilter}
              onChange={(event) => setRoleFilter(event.target.value === 'all' ? 'all' : 'repeaters')}
            >
              <option value="repeaters">Repeaters only</option>
              <option value="all">All roles</option>
            </select>
          </label>
          <label className="regions-page__toggle">Status
            <select
              className="regions-page__select"
              value={statusFilter}
              onChange={(event) => {
                const value = event.target.value;
                setStatusFilter(value === 'green' || value === 'red' ? value : 'all');
              }}
            >
              <option value="all">All</option>
              <option value="green">Green only</option>
              <option value="red">Red only</option>
            </select>
          </label>
          <label className="regions-page__toggle">Region
            <select
              className="regions-page__select"
              aria-label="Filter by region"
              value={selectedRegion ?? 'all'}
              onChange={(event) => setSelectedRegion(event.target.value === 'all' ? null : event.target.value)}
            >
              <option value="all">All regions</option>
              {displayRegions.map((region) => (
                <option key={region.name} value={region.name}>{region.name}</option>
              ))}
            </select>
          </label>
        </div>
      </header>

      {error && <div className="regions-page__error" role="alert">{error}</div>}
      {!summary && !error && <LoadingIndicator label="Loading region scoping…" variant="inline" />}

      {summary && (
        <>
          <div className="regions-page__stats">
            <div><strong>{stats.regions}</strong><span>observed regions</span></div>
            <div><strong>{compactNumber(stats.scopedPackets)}</strong><span>scoped packets</span></div>
            <div><strong>{compactNumber(stats.green)}</strong><span>region-capable nodes</span></div>
            <div><strong>{compactNumber(stats.red)}</strong><span>no scoped relay observed</span></div>
            <div><strong>{compactNumber(stats.memberships)}</strong><span>relay memberships</span></div>
            <div><strong>{compactNumber(stats.unnamedClusters)}</strong><span>unnamed footprints</span></div>
          </div>

          <div className="regions-page__workspace">
            <RegionsMap
              regions={displayRegions}
              includeLowConfidence={includeLowConfidence}
              repeaters={repeaters}
              shapesVisible={shapesVisible}
              repeatersVisible={repeatersVisible}
              unattributed={unattributedClusters}
              unattributedVisible={unnamedVisible}
              roleFilter={roleFilter}
              statusFilter={statusFilter}
              regionFilter={selectedRegion}
              memberIds={memberIds}
              selectedRegion={selectedRegion}
              selectedNodeId={selectedNodeId}
              network={network}
              observer={observer}
              privacyGeneration={privacyGeneration}
              onRegionSelect={handleRegionSelect}
              onRepeaterSelect={handleRepeaterSelect}
            />

            <aside className="regions-page__side">
              {selectedRepeater ? (
                <RepeaterPanel
                  repeater={selectedRepeater}
                  site={site}
                  onClear={() => setSelectedNodeId(null)}
                />
              ) : selectedRegionData ? (
                <RegionPanel
                  region={selectedRegionData}
                  onMemberSelect={handleRepeaterSelect}
                  onClear={() => setSelectedRegion(null)}
                />
              ) : (
                <>
                  <h2>Most active regions</h2>
                  <ol className="regions-page__list">
                    {displayRegions.slice(0, includeLowConfidence ? 50 : 15).map((region) => (
                      <li key={region.name}>
                        <button
                          type="button"
                          title={`${region.name} — ${region.packetsTotal} scoped packets${isEstablished(region) ? '' : ' (low confidence)'}`}
                          onClick={() => handleRegionSelect(region.name)}
                        >
                          <span>
                            <i style={{ backgroundColor: regionColor(region.name) }} />
                            {region.name}
                            {!isEstablished(region) && <em className="regions-page__low-tag">low</em>}
                          </span>
                          <strong>{compactNumber(region.packetsTotal)}</strong>
                        </button>
                      </li>
                    ))}
                  </ol>
                  {unattributedPayload && unattributedPayload.clusters.length > 0 && (
                    <>
                      <h2>Unnamed footprints</h2>
                      <p className="regions-page__note">
                        {compactNumber(unattributedPayload.totals.packets)} scoped packets match no public
                        region name. Private regions derive their transport key from stored key material, so
                        the name cannot be recovered from traffic — these shapes show where the traffic is heard
                        instead.
                      </p>
                      <ol className="regions-page__list">
                        {unattributedPayload.clusters.slice(0, 8).map((cluster) => (
                          <li key={cluster.id}>
                            <div
                              className="regions-page__unnamed-row"
                              title={cluster.topNodes.length > 0
                                ? cluster.topNodes.map((node) => `${node.name ?? node.nodeId.slice(0, 8)} (${node.packets})`).join(', ')
                                : 'No located nodes'}
                            >
                              <span>
                                <i className="regions-page__unnamed-dot" />
                                Unnamed {cluster.id.toUpperCase()} · {cluster.nodeCount} nodes
                              </span>
                              <strong>{compactNumber(cluster.packets)}</strong>
                            </div>
                          </li>
                        ))}
                      </ol>
                    </>
                  )}
                  <p className="regions-page__note">
                    Region names are community-chosen keys, not official boundaries — shapes show where each
                    region's scoped traffic is relayed. Low-confidence regions (1–2 observations) are hidden;
                    use the toggle to include them. Window: last {summary.windowDays ?? 92} days
                    {generatedLabel ? ` · refreshed ${generatedLabel}` : ''}.
                  </p>
                </>
              )}
            </aside>
          </div>
        </>
      )}
    </div>
  );
};

export function RepeaterPanel({ repeater, site, onClear }: {
  repeater: RegionRepeater;
  site: SiteConfig;
  onClear: () => void;
}): React.ReactElement {
  const regionEntries = Object.entries(repeater.regions).sort((a, b) => b[1] - a[1]).slice(0, 12);
  const tier = repeater.status === 'green' ? confidenceFor(repeater.scopedPackets) : null;
  const regionCount = Object.keys(repeater.regions).length;
  return (
    <div className="regions-page__selected">
      <h2>Selected repeater</h2>
      <strong>{repeater.name ?? repeater.nodeId.slice(0, 12)}</strong>
      <span className={`regions-page__badge regions-page__badge--${repeater.status}`}>
        {repeater.status === 'green' ? 'Region-capable' : 'No scoped relay observed'}
      </span>
      {repeater.status === 'green' ? (
        regionCount > 0 ? (
          <span>
            Relayed {compactNumber(repeater.scopedPackets)} cryptographically scoped packets
            across {regionCount} region{regionCount === 1 ? '' : 's'}.
          </span>
        ) : (
          <span>
            Relayed {compactNumber(repeater.scopedPackets)} cryptographically scoped packets, but no
            single region exceeded the 10% membership floor.
          </span>
        )
      ) : (
        <span>
          Unscoped relay evidence: {compactNumber(repeater.unscopedPackets7d)} packets in the last
          7 days. No region-scoped traffic was observed relaying through it.
        </span>
      )}
      {tier && <span className="regions-page__note">{CONFIDENCE_LABELS[tier]}</span>}
      {regionEntries.length > 0 && (
        <div className="regions-page__chips">
          {regionEntries.map(([name, packets]) => (
            <span key={name} className="regions-page__chip">{name} <strong>{compactNumber(packets)}</strong></span>
          ))}
        </div>
      )}
      {repeater.lat != null && repeater.lon != null && (
        <a
          className="site-btn site-btn--primary"
          href={`${site.mapHomeUrl}?map=${repeater.lat.toFixed(5)},${repeater.lon.toFixed(5)},13&node=${encodeURIComponent(repeater.nodeId)}`}
        >
          Zoom to repeater on live map
        </a>
      )}
      <button type="button" className="regions-page__clear" onClick={onClear}>Clear selection</button>
    </div>
  );
}

export function RegionPanel({ region, onMemberSelect, onClear }: {
  region: RegionSummary;
  onMemberSelect: (nodeId: string) => void;
  onClear: () => void;
}): React.ReactElement {
  const topMembers = [...region.members].sort((a, b) => b.packets - a.packets).slice(0, 10);
  return (
    <div className="regions-page__selected">
      <h2>Selected region</h2>
      <strong><i className="regions-page__region-dot" style={{ backgroundColor: regionColor(region.name) }} />{region.name}</strong>
      <span className="regions-page__badge regions-page__badge--muted">{CONFIDENCE_LABELS[region.confidence]}</span>
      <span>
        {compactNumber(region.packetsTotal)} scoped packets · {region.memberCount} significant relay
        members ({region.repeaterMemberCount} repeaters)
      </span>
      {topMembers.length > 0 ? (
        <>
          <span className="regions-page__note">Top relay members</span>
          <ol className="regions-page__list">
            {topMembers.map((member) => (
              <li key={member.nodeId}>
                <button type="button" title={member.name ?? member.nodeId} onClick={() => onMemberSelect(member.nodeId)}>
                  <span>{member.name ?? member.nodeId.slice(0, 10)}</span>
                  <strong>{compactNumber(member.packets)}</strong>
                </button>
              </li>
            ))}
          </ol>
        </>
      ) : (
        <span>No significant relay members — no repeater relayed at least 10% of this region's traffic, or its relay hops could not be attributed.</span>
      )}
      <p className="regions-page__note">
        Members are repeaters relaying at least 10% of this region's scoped packets; shapes are the
        convex hull of their positions, excluding universal bridge repeaters that relay many regions —
        an approximation of the region's active footprint.
      </p>
      <button type="button" className="regions-page__clear" onClick={onClear}>Clear selection</button>
    </div>
  );
}
