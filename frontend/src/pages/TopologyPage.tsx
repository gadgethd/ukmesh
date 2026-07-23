import React, { useEffect, useMemo, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { getCurrentSite } from '../config/site.js';
import { withScopeParams } from '../utils/api.js';
import './network-intelligence.css';

type TopologyNode = {
  nodeId: string;
  name: string | null;
  lat: number | null;
  lon: number | null;
  degree: number;
  observations: number;
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

type PlotNode = TopologyNode & { x: number; y: number };

function compactNumber(value: number): string {
  return new Intl.NumberFormat('en-GB', { notation: 'compact', maximumFractionDigits: 1 }).format(value);
}

export const TopologyPage: React.FC = () => {
  const site = getCurrentSite();
  const [payload, setPayload] = useState<TopologyPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [strongOnly, setStrongOnly] = useState(false);
  const [rfValidation, setRfValidation] = useState<RfValidationPayload | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    fetch(withScopeParams('/api/topology?limit=300', { network: site.networkFilter }), { signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error(`Topology request failed (${response.status})`);
        return response.json() as Promise<TopologyPayload>;
      })
      .then(setPayload)
      .catch((reason: unknown) => {
        if ((reason as DOMException).name !== 'AbortError') setError((reason as Error).message);
      });
    return () => controller.abort();
  }, [site.networkFilter]);

  useEffect(() => {
    const controller = new AbortController();
    fetch(withScopeParams('/api/rf-validation?limit=100', { network: site.networkFilter }), { signal: controller.signal })
      .then((response) => response.ok ? response.json() as Promise<RfValidationPayload> : Promise.reject(new Error('RF validation unavailable')))
      .then(setRfValidation)
      .catch(() => setRfValidation(null));
    return () => controller.abort();
  }, [site.networkFilter]);

  const plot = useMemo(() => {
    const located = (payload?.nodes ?? []).filter(
      (node): node is TopologyNode & { lat: number; lon: number } => node.lat != null && node.lon != null,
    );
    if (located.length === 0) return { nodes: [] as PlotNode[], links: [] as TopologyLink[] };
    const lats = located.map((node) => node.lat);
    const lons = located.map((node) => node.lon);
    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);
    const minLon = Math.min(...lons);
    const maxLon = Math.max(...lons);
    const latSpan = Math.max(0.2, maxLat - minLat);
    const lonSpan = Math.max(0.2, maxLon - minLon);
    const nodes = located.map<PlotNode>((node) => ({
      ...node,
      x: 40 + ((node.lon - minLon) / lonSpan) * 920,
      y: 560 - ((node.lat - minLat) / latSpan) * 520,
    }));
    const ids = new Set(nodes.map((node) => node.nodeId));
    return {
      nodes,
      links: (payload?.links ?? []).filter((link) => (
        ids.has(link.source) && ids.has(link.target) && (!strongOnly || link.strongObservations > 0)
      )),
    };
  }, [payload, strongOnly]);

  const nodesById = useMemo(() => new Map(plot.nodes.map((node) => [node.nodeId, node])), [plot.nodes]);
  const selected = payload?.nodes.find((node) => node.nodeId === selectedNodeId) ?? null;
  const maxObservations = Math.max(1, ...plot.links.map((link) => link.observations));
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
            <section className="topology-page__graph" aria-label="Geographic repeater topology graph">
              <svg viewBox="0 0 1000 600" role="img" aria-label={`${plot.nodes.length} positioned repeaters and ${plot.links.length} links`}>
                <g className="topology-page__links">
                  {plot.links.map((link) => {
                    const source = nodesById.get(link.source);
                    const target = nodesById.get(link.target);
                    if (!source || !target) return null;
                    const highlighted = selectedNodeId === link.source || selectedNodeId === link.target;
                    return (
                      <line
                        key={`${link.source}:${link.target}`}
                        x1={source.x} y1={source.y} x2={target.x} y2={target.y}
                        className={highlighted ? 'topology-page__link topology-page__link--active' : 'topology-page__link'}
                        strokeWidth={0.6 + (link.observations / maxObservations) * 3}
                      />
                    );
                  })}
                </g>
                <g className="topology-page__nodes">
                  {plot.nodes.map((node) => (
                    <circle
                      key={node.nodeId}
                      cx={node.x} cy={node.y}
                      r={Math.min(10, 2.5 + Math.sqrt(node.degree))}
                      className={[
                        'topology-page__node',
                        selectedNodeId === node.nodeId ? 'topology-page__node--active' : '',
                        bridgeIds.has(node.nodeId) ? 'topology-page__node--bridge' : '',
                        isolatedIds.has(node.nodeId) ? 'topology-page__node--isolated' : '',
                      ].filter(Boolean).join(' ')}
                      role="button"
                      tabIndex={0}
                      aria-label={`${node.name ?? node.nodeId.slice(0, 8)}, ${node.degree} links`}
                      onClick={() => setSelectedNodeId(node.nodeId)}
                      onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') setSelectedNodeId(node.nodeId); }}
                    >
                      <title>{node.name ?? node.nodeId.slice(0, 8)} · {node.degree} links · {compactNumber(node.observations)} observations</title>
                    </circle>
                  ))}
                </g>
              </svg>
              <div className="topology-page__legend">Dot size = connections · amber ring = likely bridge · hollow = isolated</div>
            </section>

            <aside className="topology-page__hubs">
              <h2>{selected ? 'Selected repeater' : 'Most connected'}</h2>
              {selected ? (
                <div className="topology-page__selected">
                  <strong>{selected.name ?? selected.nodeId.slice(0, 12)}</strong>
                  <span>{selected.degree} viable relationships</span>
                  <span>{compactNumber(selected.observations)} observations</span>
                  {bridgeIds.has(selected.nodeId) && <span className="topology-page__flag">Likely bridge between network segments</span>}
                  {isolatedIds.has(selected.nodeId) && <span className="topology-page__flag">No recent viable relationships</span>}
                  <button type="button" onClick={() => setSelectedNodeId(null)}>Show hub ranking</button>
                </div>
              ) : (
                <ol>
                  {payload.nodes.filter((node) => node.degree > 0).slice(0, 12).map((node) => (
                    <li key={node.nodeId}>
                      <button type="button" onClick={() => setSelectedNodeId(node.nodeId)}>
                        <span>{node.name ?? node.nodeId.slice(0, 10)}</span>
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
