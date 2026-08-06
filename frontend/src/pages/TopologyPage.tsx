import React, { useEffect, useMemo, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { TopologyMap } from '../components/Map/TopologyMap.js';
import { getCurrentSite } from '../config/site.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { fetchJson, withScopeParams } from '../utils/api.js';
import './network-intelligence.css';

type TopologyNode = {
  nodeId: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  degree: number;
  observations: number;
  region?: string | null;
};

type TopologyLink = {
  source: string;
  target: string;
  observations: number;
  strongObservations: number;
  pathLossDb: number | null;
  lastObserved: string;
};

type TopologyPayload = {
  generatedAt: string;
  windowDays: number;
  limited: boolean;
  summary: {
    nodes: number; links: number; observations: number;
    connectedComponents: number; likelyBridges: number; isolatedNodes: number;
  };
  nodes: TopologyNode[];
  links: TopologyLink[];
  analysis: { connectedComponents: number; bridgeNodeIds: string[]; isolatedNodeIds: string[] };
};

type RfValidationPayload = {
  methodology: string;
  summary: { evaluated: number; matches: number; mismatches: number; observedUnexpected: number; operatorOverrides: number; weakModelEvidence: number };
  mismatches: Array<{
    source: string; target: string; sourceName: string | null; targetName: string | null;
    observations: number; strongObservations: number; pathLossDb: number | null;
    classification: 'observed_unexpected' | 'operator_override' | 'weak_model_evidence';
  }>;
};

function compactNumber(value: number): string {
  return new Intl.NumberFormat('en-GB', { notation: 'compact', maximumFractionDigits: 1 }).format(value);
}

export const TopologyPage: React.FC = () => {
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const [payload, setPayload] = useState<TopologyPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [strongOnly, setStrongOnly] = useState(false);
  const [rfValidation, setRfValidation] = useState<RfValidationPayload | null>(null);
  const [region, setRegion] = useState('all');

  useEffect(() => {
    const controller = new AbortController();
    setPayload(null);
    setError(null);
    setSelectedNodeId(null);
    fetchJson<TopologyPayload>(
      withScopeParams('/api/topology?limit=300', { network, observer }),
      { signal: controller.signal, cache: 'no-store' },
      { timeoutMs: 15_000, maxBytes: 8 * 1024 * 1024 },
    )
      .then(setPayload)
      .catch((reason: unknown) => {
        if ((reason as DOMException).name !== 'AbortError') setError((reason as Error).message);
      });
    return () => controller.abort();
  }, [network, observer, privacyGeneration]);

  useEffect(() => {
    const controller = new AbortController();
    setRfValidation(null);
    fetchJson<RfValidationPayload>(
      withScopeParams('/api/rf-validation?limit=100', { network, observer }),
      { signal: controller.signal, cache: 'no-store' },
      { timeoutMs: 15_000, maxBytes: 4 * 1024 * 1024 },
    )
      .then(setRfValidation)
      .catch(() => setRfValidation(null));
    return () => controller.abort();
  }, [network, observer, privacyGeneration]);

  const plot = useMemo(() => {
    const nodes = (payload?.nodes ?? []).filter((node) => region === 'all' || (node.region ?? 'Unknown') === region);
    const ids = new Set(nodes.map((node) => node.nodeId));
    return {
      nodes,
      links: (payload?.links ?? []).filter((link) => ids.has(link.source) && ids.has(link.target) && (!strongOnly || link.strongObservations > 0)),
    };
  }, [payload?.links, payload?.nodes, region, strongOnly]);
  const regions = useMemo(() => [...new Set((payload?.nodes ?? []).map((node) => node.region ?? 'Unknown'))].sort(), [payload]);

  const selected = payload?.nodes.find((node) => node.nodeId === selectedNodeId) ?? null;
  const bridgeIds = useMemo(() => new Set(payload?.analysis.bridgeNodeIds ?? []), [payload]);
  const isolatedIds = useMemo(() => new Set(payload?.analysis.isolatedNodeIds ?? []), [payload]);

  return (
    <div className="topology-page site-content">
      <header className="topology-page__header">
        <div>
          <p className="topology-page__eyebrow">Network intelligence</p>
          <h1>Repeater topology</h1>
          <p>
            The strongest viable repeater relationships observed during the last {payload?.windowDays ?? 30} days.
            Lines are evidence of observed relay relationships, not guaranteed routes.
          </p>
        </div>
        <label className="topology-page__toggle">
          <input type="checkbox" checked={strongOnly} onChange={(event) => setStrongOnly(event.target.checked)} />
          Multibyte evidence only
        </label>
        <label className="topology-page__toggle topology-page__region-toggle">Region
          <select
            className="topology-page__region-select"
            aria-label="Filter topology by region"
            value={region}
            onChange={(event) => setRegion(event.target.value)}
          >
            <option value="all">All regions</option>
            {regions.map((value) => <option key={value} value={value}>{value}</option>)}
          </select>
        </label>
      </header>

      {error && <div className="topology-page__error" role="alert">{error}</div>}
      {!payload && !error && <LoadingIndicator label="Loading network topology…" variant="inline" />}

      {payload && (
        <>
          <div className="topology-page__stats topology-page__stats--six">
            <div><strong>{compactNumber(payload.summary.nodes)}</strong><span>connected repeaters</span></div>
            <div><strong>{compactNumber(payload.summary.links)}</strong><span>viable links</span></div>
            <div><strong>{compactNumber(payload.summary.observations)}</strong><span>observations</span></div>
            <div><strong>{payload.summary.connectedComponents}</strong><span>graph components</span></div>
            <div><strong>{payload.summary.likelyBridges}</strong><span>likely bridge repeaters</span></div>
            <div><strong>{payload.summary.isolatedNodes}</strong><span>recently active, isolated</span></div>
          </div>

          <div className="topology-page__workspace">
            <TopologyMap
              nodes={plot.nodes}
              links={plot.links}
              selectedNodeId={selectedNodeId}
              bridgeNodeIds={bridgeIds}
              isolatedNodeIds={isolatedIds}
              strongOnly={strongOnly}
              network={network}
              observer={observer}
              privacyGeneration={privacyGeneration}
              onNodeSelect={setSelectedNodeId}
            />

            <aside className="topology-page__hubs">
              <h2>{selected ? 'Selected repeater' : 'Most connected'}</h2>
              {selected ? (
                <div className="topology-page__selected">
                  <strong>{selected.name ?? selected.nodeId.slice(0, 12)}</strong>
                  <span>{selected.degree} viable relationships</span>
                  <span>{compactNumber(selected.observations)} observations</span>
                  {bridgeIds.has(selected.nodeId) && <span className="topology-page__flag">Likely bridge between network segments</span>}
                  {isolatedIds.has(selected.nodeId) && <span className="topology-page__flag">No recent viable relationships</span>}
                  {selected.lat != null && selected.lon != null && (
                    <a className="site-btn site-btn--primary" href={`${site.mapHomeUrl}?map=${selected.lat.toFixed(5)},${selected.lon.toFixed(5)},13&node=${encodeURIComponent(selected.nodeId)}`}>
                      Zoom to node on map
                    </a>
                  )}
                  <button type="button" onClick={() => setSelectedNodeId(null)}>Show hub ranking</button>
                </div>
              ) : (
                <ol>
                  {payload.nodes.filter((node) => node.degree > 0).slice(0, 12).map((node) => (
                    <li key={node.nodeId}>
                      <button
                        type="button"
                        title={node.name ?? node.nodeId}
                        onClick={() => setSelectedNodeId(node.nodeId)}
                      >
                        <span title={node.name ?? node.nodeId}>{node.name ?? node.nodeId.slice(0, 10)}</span>
                        <strong>{node.degree}</strong>
                      </button>
                    </li>
                  ))}
                </ol>
              )}
              {payload.limited && <p className="topology-page__note">Showing the 300 strongest recent links.</p>}
            </aside>
          </div>

          {rfValidation && (
            <section className="topology-page__validation">
              <div className="topology-page__validation-head">
                <div><p className="topology-page__eyebrow">Model validation</p><h2>Predicted versus observed RF</h2></div>
                <p>{rfValidation.methodology}</p>
              </div>
              <div className="topology-page__validation-stats">
                <div><strong>{rfValidation.summary.evaluated}</strong><span>links evaluated</span></div>
                <div><strong>{rfValidation.summary.observedUnexpected}</strong><span>unexpected observed links</span></div>
                <div><strong>{rfValidation.summary.operatorOverrides}</strong><span>operator overrides</span></div>
                <div><strong>{rfValidation.summary.weakModelEvidence}</strong><span>weak evidence</span></div>
              </div>
              {rfValidation.mismatches.length > 0 && (
                <div className="topology-page__validation-table" role="table" aria-label="RF model mismatches">
                  <div role="row" className="topology-page__validation-table-head">
                    <span role="columnheader">Link</span>
                    <span role="columnheader">Observations</span>
                    <span role="columnheader">Modelled path loss</span>
                    <span role="columnheader">Classification</span>
                  </div>
                  {rfValidation.mismatches.slice(0, 20).map((link) => (
                    <div role="row" key={`${link.source}:${link.target}`}>
                      <span role="cell">{link.sourceName ?? link.source.slice(0, 8)} ↔ {link.targetName ?? link.target.slice(0, 8)}</span>
                      <span role="cell">{link.observations} obs · {link.strongObservations} strong</span>
                      <span role="cell">{link.pathLossDb == null ? 'No model dB' : `${link.pathLossDb.toFixed(1)} dB`}</span>
                      <strong role="cell">{link.classification.replace(/_/g, ' ')}</strong>
                    </div>
                  ))}
                </div>
              )}
            </section>
          )}
        </>
      )}
    </div>
  );
};
