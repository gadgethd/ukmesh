import { useEffect, useState } from 'react';
import { fetchJson, withScopeParams } from '../../utils/api.js';
import { validRegionName } from '../../utils/regionUrlState.js';
import './region-selector.css';

export function RegionSelector({ network, region, onChange }: {
  network: string;
  region?: string;
  onChange: (region?: string) => void;
}) {
  const [names, setNames] = useState<string[]>([]);
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');
  useEffect(() => {
    const controller = new AbortController();
    setStatus('loading');
    void fetchJson<{ regions: Array<{ name: string }> }>(
      withScopeParams('/api/regions/summary', { network }),
      { signal: controller.signal },
    ).then((payload) => {
      if (controller.signal.aborted) return;
      setNames([...new Set(payload.regions.map((item) => item.name).filter(validRegionName))].sort());
      setStatus('ready');
    }).catch(() => {
      if (!controller.signal.aborted) setStatus('error');
    });
    return () => controller.abort();
  }, [network]);

  const options = region && !names.includes(region) ? [region, ...names] : names;
  return <label className="region-selector" title="Filters relay members and scoped packets. Counters show UK Mesh network totals.">
    <span>Region</span>
    <select aria-label="Region" value={region ?? ''} onChange={(event) => onChange(event.target.value || undefined)}>
      <option value="">All regions</option>
      {options.map((name) => <option key={name} value={name}>#{name}</option>)}
    </select>
    {status !== 'ready' && <span className="region-selector__status" role="status">{status === 'loading' ? 'Loading regions…' : 'Regions unavailable'}</span>}
  </label>;
}
