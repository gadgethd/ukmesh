import React, { useEffect, useMemo, useState } from 'react';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { TropoMap } from '../components/Map/TropoMap.js';
import {
  TROPO_TIERS,
  bestStep,
  formatGenerated,
  formatRun,
  formatValid,
  formatValidShort,
  initialStepIndex,
  isQuiet,
  stalenessHours,
  tierFor,
  type TropoIndex,
} from './tropoModel.js';
import './tropo-page.css';

const FORECAST_URL = '/tropo/data/index.json';
const PLAY_INTERVAL_MS = 1400;

export const TropoPage: React.FC = () => {
  const [data, setData] = useState<TropoIndex | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState(0);
  const [playing, setPlaying] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    setData(null);
    setError(null);
    fetch(FORECAST_URL, { signal: controller.signal, cache: 'no-store' })
      .then(async (response) => {
        if (!response.ok) throw new Error(`Forecast data is unavailable (HTTP ${response.status}).`);
        return (await response.json()) as TropoIndex;
      })
      .then((payload) => {
        if (!Array.isArray(payload.steps) || payload.steps.length === 0) {
          throw new Error('The forecast manifest contains no steps.');
        }
        setData(payload);
        setSelected(initialStepIndex(payload));
      })
      .catch((reason: unknown) => {
        if ((reason as DOMException).name !== 'AbortError') {
          setError((reason as Error).message || 'Could not load the forecast.');
        }
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    if (!playing || !data) return undefined;
    const timer = window.setInterval(() => {
      setSelected((current) => (current + 1) % data.steps.length);
    }, PLAY_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [playing, data]);

  const step = data?.steps[selected] ?? null;
  const best = useMemo(() => bestStep(data), [data]);
  const quiet = isQuiet(data);
  const stale = data ? stalenessHours(data.generated_at) > 12 : false;
  const hasDucting = Boolean(data?.steps.some((entry) => entry.peak_duct > 0 || entry.pct_ducting > 0));
  const enhanced = Boolean(data?.steps.some((entry) => entry.max_index >= 55));
  const tier = step ? tierFor(step.max_index) : TROPO_TIERS[0];
  const statusLabel = stale ? 'Stale forecast' : hasDucting ? 'Ducting' : enhanced ? 'Enhanced' : 'Quiet';
  const statusClass = stale
    ? 'tropo-page__badge--stale'
    : hasDucting
      ? 'tropo-page__badge--active'
      : enhanced
        ? 'tropo-page__badge--enhanced'
        : 'tropo-page__badge--quiet';

  const setStep = (next: number) => {
    if (!data) return;
    setPlaying(false);
    setSelected(Math.max(0, Math.min(data.steps.length - 1, next)));
  };

  return (
    <div className="tropo-page site-content">
      <header className="tropo-page__header">
        <div>
          <p className="tropo-page__eyebrow">Propagation intelligence</p>
          <h1>Tropospheric ducting</h1>
          <p>
            A five-day forecast of anomalous VHF/UHF propagation over the UK and surrounding waters,
            derived from NOAA&apos;s GFS pressure-level profiles. Headline figures use UK land cells;
            the shading shows the wider forecast area, including surrounding waters. Index 100 is the
            physical ducting threshold.
          </p>
        </div>
        {data && (
          <div className="tropo-page__status">
            <span className={`tropo-page__badge ${statusClass}`}>
              {statusLabel}
            </span>
            <span className="tropo-page__updated">Updated {formatGenerated(data.generated_at)}</span>
          </div>
        )}
      </header>

      {error && (
        <div className="tropo-page__error" role="alert">
          <strong>Forecast unavailable</strong>
          <span>{error}</span>
          <span>The page will pick up the next NOAA GFS run automatically.</span>
        </div>
      )}

      {!data && !error && <LoadingIndicator label="Loading NOAA GFS forecast…" variant="inline" />}

      {data && step && (
        <>
          {stale && (
            <div className="tropo-page__notice" role="status">
              The latest run could not be refreshed. Showing the {formatRun(data.run)} model run; treat it as
              guidance rather than a live forecast.
            </div>
          )}
          {!stale && quiet && (
            <div className="tropo-page__notice" role="status">
              No UK land cell reaches the ducting threshold in this run. The shading still shows where
              the lower atmosphere is closest to ducting.
            </div>
          )}
          {!stale && !quiet && !hasDucting && (
            <div className="tropo-page__notice" role="status">
              Enhanced bending is forecast, but no UK land cell reaches the ducting threshold. The
              shading shows where the lower atmosphere is closest to trapping.
            </div>
          )}

          <div className="tropo-page__stats">
            <div>
              <strong>{data.cycle}</strong>
              <span>model run · {formatRun(data.run)}</span>
            </div>
            <div>
              <strong style={{ color: tier.color }}>{Math.round(step.max_index)}</strong>
              <span>UK land peak / 150</span>
            </div>
            <div>
              <strong>{step.peak_duct > 0 ? Math.round(step.peak_duct) : '—'}</strong>
              <span>peak duct strength N/km</span>
            </div>
            <div>
              <strong>{step.pct_ducting > 0 ? `${step.pct_ducting.toFixed(1)}%` : 'None'}</strong>
              <span>UK land at ducting threshold</span>
            </div>
            <div className="tropo-page__stat--time">
              <strong>{best ? formatValidShort(best.valid) : '—'}</strong>
              <span>strongest window</span>
            </div>
          </div>

          <div className="tropo-page__workspace">
            <div className="tropo-page__map">
              <TropoMap
                bounds={data.bounds}
                imageUrl={`/tropo/data/${step.file}`}
                peak={step.peak}
                maxIndex={step.max_index}
              />
              <div className="tropo-page__map-meta">
                <strong>{data.model}</strong>
                <span>{formatValid(step.valid)} · {step.forecast_hour === 0 ? 'analysis' : `+${step.forecast_hour} h`}</span>
                <span>Forecast area 49–62°N · 12°W–5°E</span>
                <span>Basemap © OpenStreetMap contributors · Natural Earth</span>
              </div>
              <div className="tropo-page__legend">
                <strong>Ducting potential</strong>
                {TROPO_TIERS.map((entry) => (
                  <div key={entry.label} className="tropo-page__legend-row">
                    <i style={{ background: entry.color }} />
                    <span>{entry.shortLabel}</span>
                    <em>{entry.min}–{entry.max}</em>
                  </div>
                ))}
                <p>100+ = dM/dz at or below zero</p>
              </div>
            </div>

            <aside className="tropo-page__side">
              <h2>Forecast timeline</h2>
              <div className="tropo-page__time-controls">
                <button type="button" onClick={() => setStep(selected - 1)} disabled={selected === 0} aria-label="Previous forecast step">◀</button>
                <button type="button" className="tropo-page__play" onClick={() => setPlaying((value) => !value)}>
                  {playing ? 'Pause' : 'Play'}
                </button>
                <button type="button" onClick={() => setStep(selected + 1)} disabled={selected === data.steps.length - 1} aria-label="Next forecast step">▶</button>
              </div>
              <input
                className="tropo-page__range"
                type="range"
                min={0}
                max={data.steps.length - 1}
                value={selected}
                aria-label="Forecast time"
                onChange={(event) => {
                  setPlaying(false);
                  setSelected(Number(event.target.value));
                }}
              />
              <div className="tropo-page__time-label" aria-live="polite">
                <strong>{formatValid(step.valid)}</strong>
                <span>{step.forecast_hour === 0 ? 'Analysis' : `+${step.forecast_hour} h`}</span>
              </div>

              <div className="tropo-page__meter">
                <div>
                  <span>UK land peak index</span>
                  <strong style={{ color: tier.color }}>{Math.round(step.max_index)}</strong>
                </div>
                <div className="tropo-page__meter-bar" aria-hidden="true">
                  <i style={{ width: `${Math.min(100, (step.max_index / 150) * 100)}%`, background: tier.color }} />
                </div>
                <span>{tier.description}</span>
              </div>

              {best && (
                <button type="button" className="tropo-page__jump" onClick={() => setStep(best.index)}>
                  Jump to strongest window · {formatValidShort(best.valid)}
                </button>
              )}

              <h2>How to read it</h2>
              <ul className="tropo-page__tips">
                <li><strong>UK land basis.</strong> Headline index, peak and threshold area use UK land grid cells; the map shading covers the surrounding waters too.</li>
                <li><strong>Index 100</strong> means the modified-refractivity gradient has reached zero — the physical threshold for trapping.</li>
                <li><strong>Read the paths, not the pixel.</strong> What matters is whether a duct runs along the direction you care about.</li>
                <li><strong>Timing matters.</strong> Land paths often peak overnight and early morning; sea paths can peak in the afternoon.</li>
              </ul>

              <div className="tropo-page__source">
                <strong>Data</strong>
                <span>{data.source} · 0.25° · 3-hourly · 4 runs/day</span>
                <span>Analysis and forecast derived by UKMesh from public NOAA GFS fields.</span>
                <a href="https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast" target="_blank" rel="noopener noreferrer">
                  About NOAA GFS ↗
                </a>
              </div>
            </aside>
          </div>
        </>
      )}
    </div>
  );
};
