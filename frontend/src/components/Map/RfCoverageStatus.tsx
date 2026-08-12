import { useState } from 'react';

import {
  rfNodeCoverageState,
  type RfCoverageMeta,
  type RfCoverageProgress,
  type RfCoverageTierName,
} from '../../hooks/useRfCoverage.js';

export function formatRfEta(seconds: number | undefined): string | null {
  if (seconds === undefined || !Number.isFinite(seconds) || seconds < 0) return null;
  if (seconds < 60) return '<1 min';
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  return remainder ? `${hours}h ${remainder}m` : `${hours}h`;
}

function stageLabel(stage: string): string {
  return stage.replace(/^computing_coverage_?/, '').replace(/_/g, ' ').trim() || 'standard';
}

export function RfCoverageStatus({
  meta,
  progress,
  availableTiers,
  tier,
  onTierChange,
  nodePublicKey,
  onClearNode,
  visible,
}: {
  meta: RfCoverageMeta | null;
  progress: RfCoverageProgress | null;
  availableTiers: RfCoverageTierName[];
  tier: RfCoverageTierName;
  onTierChange: (tier: RfCoverageTierName) => void;
  nodePublicKey?: string | null;
  onClearNode?: () => void;
  visible: boolean;
}) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  if (!visible) return null;
  const nodeEntry = nodePublicKey ? meta?.node_coverage?.[nodePublicKey.toLowerCase()] : undefined;
  const product = nodePublicKey ? nodeEntry?.standard : meta?.coverage?.[tier];
  const nodeState = nodePublicKey ? rfNodeCoverageState(meta, nodePublicKey) : null;
  const eta = formatRfEta(progress?.eta_seconds);
  const running = !!progress && progress.stage !== 'done' && progress.stage !== 'error';
  const failed = nodePublicKey
    ? nodeState === 'error'
    : progress?.stage === 'error'
      || !!meta?.run?.failure
      || meta?.run?.tiers?.[tier]?.state === 'failed';

  return (
    <section className="rf-coverage-status" aria-label="RF coverage status">
      {!nodePublicKey && running && (
        <div className="rf-coverage-progress" role="status">
          <strong>{stageLabel(progress.stage)}</strong>
          <span>{progress.backend?.replace('_', ' ') ?? 'preparing'}</span>
          <span>{Math.max(0, Math.min(100, progress.percent)).toFixed(0)}%</span>
          {eta && <span>ETA {eta}</span>}
        </div>
      )}
      {nodePublicKey && nodeState === 'pending' && (
        <div className="rf-coverage-progress" role="status">
          Repeater coverage is pending an on-demand calculation.
        </div>
      )}
      {failed && (
        <div className="rf-coverage-progress rf-coverage-progress--error" role="status">
          {nodePublicKey ? (nodeEntry?.failure ?? 'Repeater coverage failed.') : 'Coverage refresh failed; the last published tiles remain live.'}
        </div>
      )}
      <div className="rf-coverage-card">
        <div className="rf-coverage-card__header">
          <strong>{nodePublicKey ? 'Repeater RF footprint' : 'HopReach RF coverage'}</strong>
          {nodePublicKey && onClearNode && (
            <button type="button" className="rf-coverage-back" onClick={onClearNode}>Network coverage</button>
          )}
          {!nodePublicKey && availableTiers.length > 1 && (
            <div className="rf-coverage-tier" aria-label="Coverage detail">
              {availableTiers.map((value) => (
                <button
                  type="button"
                  key={value}
                  aria-pressed={tier === value}
                  onClick={() => onTierChange(value)}
                >
                  {value[0].toUpperCase() + value.slice(1)}
                </button>
              ))}
            </div>
          )}
        </div>
        <div className="rf-coverage-gradient" aria-hidden="true" />
        <div className="rf-coverage-gradient-labels"><span>Below threshold</span><span>Strong margin</span></div>
        {product ? (
          <div className={`rf-coverage-details-wrap${detailsOpen ? ' rf-coverage-details-wrap--open' : ''}`}>
          <dl className="rf-coverage-details">
            <div><dt>Frequency</dt><dd>{product.frequency_mhz.toFixed(3)} MHz</dd></div>
            <div><dt>Generated</dt><dd>{new Date(product.generated_at ?? meta?.generated_at ?? '').toLocaleString()}</dd></div>
            <div><dt>Model</dt><dd>{nodePublicKey ? 'Single transmitter' : (meta?.run?.model ?? 'HopReach')}</dd></div>
            <div><dt>Source</dt><dd>{meta?.run?.source_version ?? meta?.version ?? 'unknown'}</dd></div>
            {nodePublicKey && <div><dt>Status</dt><dd>{nodeState === 'pending' ? 'Pending' : nodeState === 'stale' ? 'Stale' : 'Available'}</dd></div>}
          </dl>
            {product?.assumptions?.note && <p className="rf-coverage-note">{product.assumptions.note}</p>}
            <button
              type="button"
              className="rf-coverage-details-toggle"
              onClick={() => setDetailsOpen((value) => !value)}
              aria-expanded={detailsOpen}
            >
              {detailsOpen ? 'Hide details ▴' : 'Show details ▾'}
            </button>
          </div>
        ) : (
          <p className="rf-coverage-waiting">
            {nodePublicKey
              ? nodeEntry?.state === 'failed'
                ? 'Coverage could not be computed for this repeater.'
                : 'Coverage is pending an on-demand calculation.'
              : 'Standard coverage is being prepared.'}
          </p>
        )}
      </div>
    </section>
  );
}
