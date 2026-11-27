import React, { lazy, useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { LazyMapBoundary } from '../components/Map/LazyMapBoundary.js';
import { getCurrentSite } from '../config/site.js';
import type { SiteConfig } from '../config/site.js';
import { FAIL_CLOSED_RUNTIME_FEATURES, useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { fetchJson, withScopeParams } from '../utils/api.js';
import { liveMapLink } from '../utils/exploreLinks.js';
import { validRegionName } from '../utils/regionUrlState.js';
import { scopeKey } from '../utils/scope.js';
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
} from '../features/regions/model.js';
import './regions-page.css';

const RegionsMap = lazy(() => import('../components/Map/RegionsMap.js').then(module => ({ default: module.RegionsMap })));

const CONFIDENCE_LABELS: Record<string, string> = {
  confirmed: 'Strong evidence — 4 or more scoped packets',
  probable: 'Moderate evidence — 2-3 scoped packets',
  single: 'Single observation — could be chance',
};

export const RegionsPage: React.FC = () => {
  const site = getCurrentSite();
  const runtime = useRuntimeFeatures();
  const { privacyGeneration, regionScoping } = runtime;
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const [unnamedVisible, setUnnamedVisible] = useState(true);
  const [attempt, setAttempt] = useState(0);
  const key = scopeKey({ network, observer, privacyGeneration });
  const [result, setResult] = useState<{ key: string; attempt: number; data: {
    summary: RegionSummaryPayload; repeatersPayload: RegionRepeatersPayload; unattributedPayload: UnattributedPayload | null;
  } | null }>();
  const current = result?.key === key && result.attempt === attempt ? result.data : undefined;
  const { summary, repeatersPayload, unattributedPayload } = current ?? {};
  const error = current === null || runtime === FAIL_CLOSED_RUNTIME_FEATURES;
  const [params, setParams] = useSearchParams();
  const regionValues = params.getAll('region');
  const requestedRegion = regionValues.length === 1 && validRegionName(regionValues[0] ?? '') ? regionValues[0] : undefined;
  const [nodeSelection, setNodeSelection] = useState<{ key: string; region: string | undefined; id: string }>();
  const selectedNodeId = nodeSelection?.key === key && nodeSelection.region === requestedRegion ? nodeSelection.id : null;
  const [shapesVisible, setShapesVisible] = useState(true);
  const [repeatersVisible, setRepeatersVisible] = useState(true);
  const [roleFilter, setRoleFilter] = useState<'repeaters' | 'all'>('repeaters');
  const [statusFilter, setStatusFilter] = useState<'all' | 'green' | 'red'>('all');
  const [includeLowConfidence, setIncludeLowConfidence] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    // Wait for the initial authority snapshot; starting at generation zero
    // would fetch, display, then discard the same workspace as config arrives.
    if (privacyGeneration < 1) return () => controller.abort();
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
        setResult({ key, attempt, data: { summary: nextSummary, repeatersPayload: nextRepeaters, unattributedPayload: nextUnattributed } });
      })
      .catch(() => {
        if (!controller.signal.aborted) setResult({ key, attempt, data: null });
      });
    return () => controller.abort();
  }, [network, observer, privacyGeneration, attempt, key]);

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
    () => regions.find((region) => region.name === requestedRegion) ?? null,
    [regions, requestedRegion],
  );
  const selectedRegion = selectedRegionData?.name ?? null;
  const availableRegions = selectedRegionData && !displayRegions.includes(selectedRegionData)
    ? [selectedRegionData, ...displayRegions] : displayRegions;
  const selectedRepeater = useMemo(
    () => repeaters.find((repeater) => repeater.nodeId === selectedNodeId) ?? null,
    [repeaters, selectedNodeId],
  );
  const memberIds = useMemo(
    () => (selectedRegionData ? memberIdSet(selectedRegionData) : null),
    [selectedRegionData],
  );
  const handleRegionSelect = useCallback((region: string | null) => {
    const next = new URLSearchParams(params);
    if (region) next.set('region', region);
    else next.delete('region');
    if (next.toString() !== params.toString()) setParams(next);
    setNodeSelection(undefined);
  }, [params, setParams]);
  const handleRepeaterSelect = useCallback((nodeId: string) => {
    setNodeSelection({ key, region: requestedRegion, id: nodeId });
  }, [key, requestedRegion]);
  const handleLowConfidenceToggle = useCallback((checked: boolean) => {
    setIncludeLowConfidence(checked);
    if (!checked && selectedRegionData && !isEstablished(selectedRegionData)) handleRegionSelect(null);
  }, [handleRegionSelect, selectedRegionData]);
  const generatedLabel = formatGeneratedAt(summary?.generatedAt ?? null);

  return (
    <div className="regions-page site-content">
      <header className="regions-page__header">
        <div>
          <h1>MeshCore regions</h1>
          <p>
            Choose a region to see which repeaters relay its traffic. The shapes show where
            repeaters have relayed traffic for these named radio groups.
          </p>
          <details><summary>How regions are identified</summary><p>UKMesh checks public region names against the transport
            code on received packets. Green repeaters have relayed scoped traffic. Red repeaters have no scoped relay
            observations. Private region names stay unattributed, and region shapes exclude bridge repeaters.</p></details>
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
              value={selectedRegion ?? ''}
              onChange={(event) => handleRegionSelect(event.target.value || null)}
            >
              <option value="">All regions</option>
              {availableRegions.map((region) => (
                <option key={region.name} value={region.name}>{region.name}</option>
              ))}
            </select>
          </label>
        </div>
      </header>

      {error && <div className="regions-page__error" role="alert">
        <p>Could not load the regions list.</p>
        <button type="button" className="site-btn site-btn--ghost" onClick={() => {
          if (privacyGeneration < 1) window.location.reload();
          else setAttempt(value => value + 1);
        }}>Retry regions</button>
      </div>}
      {!summary && !error && <div className="regions-page__pending"><LoadingIndicator label="Loading region scoping…" variant="inline" /></div>}
      {summary && regionValues.length > 0 && !selectedRegionData && <div className="regions-page__error" role="status">
        <p>This region is unavailable. Choose another region or browse all regions.</p>
        <button type="button" className="site-btn site-btn--ghost" onClick={() => handleRegionSelect(null)}>Browse all regions</button>
      </div>}

      {summary && (
        <>
          <div className="regions-page__stats">
            {[
              ['observed regions', stats.regions], ['scoped packets', compactNumber(stats.scopedPackets)],
              ['region-capable nodes', compactNumber(stats.green)], ['no scoped relay observed', compactNumber(stats.red)],
              ['relay memberships', compactNumber(stats.memberships)], ['unnamed footprints', compactNumber(stats.unnamedClusters)],
            ].map(([label, value]) => <div key={label}><strong>{value}</strong><span>{label}</span></div>)}
          </div>

          <div className="regions-page__workspace">
            <LazyMapBoundary className="regions-page__map" label="Regional repeater map">
            <RegionsMap
              regions={availableRegions}
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
            </LazyMapBoundary>

            <aside className="regions-page__side">
              {selectedRepeater ? (
                <RepeaterPanel
                  repeater={selectedRepeater}
                  site={site}
                  region={regionScoping ? selectedRegion : null}
                  onClear={() => setNodeSelection(undefined)}
                />
              ) : selectedRegionData ? (
                <RegionPanel
                  region={selectedRegionData}
                  mapUrl={regionScoping ? site.mapHomeUrl : undefined}
                  onMemberSelect={handleRepeaterSelect}
                  onClear={() => handleRegionSelect(null)}
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

export function RepeaterPanel({ repeater, site, region, onClear }: {
  repeater: RegionRepeater;
  site: SiteConfig;
  region?: string | null;
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
          href={liveMapLink(site.mapHomeUrl, { region: region ?? undefined,
            node: { id: repeater.nodeId, lat: repeater.lat, lon: repeater.lon } })}
        >
          Zoom to repeater on live map
        </a>
      )}
      <button type="button" className="regions-page__clear" onClick={onClear}>Clear selection</button>
    </div>
  );
}

export function RegionPanel({ region, mapUrl, onMemberSelect, onClear }: {
  region: RegionSummary;
  mapUrl?: string;
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
      {mapUrl && <a className="site-btn site-btn--primary" href={liveMapLink(mapUrl, { region: region.name })}>Open region on Live Map</a>}
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
