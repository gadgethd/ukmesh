import type {
  RfCoverageMeta,
  RfCoverageProgress,
  RfCoverageTierName,
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
  visible,
}: {
  meta: RfCoverageMeta | null;
  progress: RfCoverageProgress | null;
  availableTiers: RfCoverageTierName[];
  tier: RfCoverageTierName;
  onTierChange: (tier: RfCoverageTierName) => void;
  visible: boolean;
}) {
  if (!visible) return null;
  const product = meta?.coverage?.[tier];
  const eta = formatRfEta(progress?.eta_seconds);
  const running = !!progress && progress.stage !== 'done' && progress.stage !== 'error';
  const failed = progress?.stage === 'error'
    || !!meta?.run?.failure
    || meta?.run?.tiers?.[tier]?.state === 'failed';

  return (
    <section className="rf-coverage-status" aria-label="RF coverage status">
      {running && (
        <div className="rf-coverage-progress" role="status">
          <strong>{stageLabel(progress.stage)}</strong>
          <span>{progress.backend?.replace('_', ' ') ?? 'preparing'}</span>
          <span>{Math.max(0, Math.min(100, progress.percent)).toFixed(0)}%</span>
          {eta && <span>ETA {eta}</span>}
        </div>
      )}
      {failed && (
        <div className="rf-coverage-progress rf-coverage-progress--error" role="status">
          Coverage refresh failed; the last published tiles remain live.
        </div>
      )}
      <div className="rf-coverage-card">
        <div className="rf-coverage-card__header">
          <strong>HopReach RF coverage</strong>
          {availableTiers.length > 1 && (
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
          <dl className="rf-coverage-details">
            <div><dt>Frequency</dt><dd>{product.frequency_mhz.toFixed(3)} MHz</dd></div>
            <div><dt>Generated</dt><dd>{new Date(product.generated_at ?? meta?.generated_at ?? '').toLocaleString()}</dd></div>
            <div><dt>Model</dt><dd>{meta?.run?.model ?? 'HopReach'}</dd></div>
            <div><dt>Source</dt><dd>{meta?.run?.source_version ?? meta?.version ?? 'unknown'}</dd></div>
          </dl>
        ) : (
          <p className="rf-coverage-waiting">Standard coverage is being prepared.</p>
        )}
        {product?.assumptions?.note && <p className="rf-coverage-note">{product.assumptions.note}</p>}
      </div>
    </section>
  );
}
