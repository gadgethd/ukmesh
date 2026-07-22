import React, { useEffect, useMemo, useState } from 'react';
import { useOverlayStore } from '../../store/overlayStore.js';

const SAVED_PLANS_KEY = 'meshcore-saved-plans-v1';
type Coordinate = { lat: number; lon: number };

function distanceKm(a: Coordinate, b: Coordinate): number {
  const toRad = (value: number) => value * Math.PI / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLon / 2) ** 2;
  return 6_371 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function circleOverlapKm2(a: Coordinate, radiusAKm: number, b: Coordinate, radiusBKm: number): number {
  const distance = distanceKm(a, b);
  if (distance >= radiusAKm + radiusBKm) return 0;
  if (distance <= Math.abs(radiusAKm - radiusBKm)) return Math.PI * Math.min(radiusAKm, radiusBKm) ** 2;
  const alpha = 2 * Math.acos((distance ** 2 + radiusAKm ** 2 - radiusBKm ** 2) / (2 * distance * radiusAKm));
  const beta = 2 * Math.acos((distance ** 2 + radiusBKm ** 2 - radiusAKm ** 2) / (2 * distance * radiusBKm));
  return 0.5 * radiusAKm ** 2 * (alpha - Math.sin(alpha)) + 0.5 * radiusBKm ** 2 * (beta - Math.sin(beta));
}

function parseCoordinates(value: string | null): Coordinate[] {
  if (!value) return [];
  return value.split(';').flatMap((entry) => {
    const [lat, lon] = entry.split(',').map(Number);
    return Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat!) <= 90 && Math.abs(lon!) <= 180
      ? [{ lat: lat!, lon: lon! }]
      : [];
  }).slice(0, 5);
}

export const PlannerComparison: React.FC<{ enabled: boolean }> = ({ enabled }) => {
  const plans = useOverlayStore((state) => state.plannedRepeaters);
  const planMode = useOverlayStore((state) => state.planRepeaterMode);
  const requestRestore = useOverlayStore((state) => state.requestPlanRestore);
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) return;
    const fromUrl = parseCoordinates(new URLSearchParams(window.location.search).get('plans'));
    if (fromUrl.length > 0 && useOverlayStore.getState().plannedRepeaters.length === 0) requestRestore(fromUrl);
  }, [enabled, requestRestore]);

  const comparison = useMemo(() => {
    const ready = plans.filter((plan) => plan.status === 'ready' && plan.coverage);
    const uniquePeers = new Set(ready.flatMap((plan) => plan.coverage?.predicted_links?.map((link) => link.peer_id) ?? []));
    let pairwiseOverlapKm2 = 0;
    for (let a = 0; a < ready.length; a += 1) {
      for (let b = a + 1; b < ready.length; b += 1) {
        const first = ready[a]!;
        const second = ready[b]!;
        pairwiseOverlapKm2 += circleOverlapKm2(
          first, (first.coverage?.radius_m ?? 0) / 1_000,
          second, (second.coverage?.radius_m ?? 0) / 1_000,
        );
      }
    }
    return { ready: ready.length, uniquePeers: uniquePeers.size, pairwiseOverlapKm2 };
  }, [plans]);

  const coordinates = plans.map(({ lat, lon }) => ({ lat, lon }));
  const save = () => {
    localStorage.setItem(SAVED_PLANS_KEY, JSON.stringify(coordinates));
    setMessage('Candidate coordinates saved in this browser.');
  };
  const restore = () => {
    try {
      const parsed = JSON.parse(localStorage.getItem(SAVED_PLANS_KEY) ?? '[]') as Coordinate[];
      requestRestore(parsed.filter((coordinate) => Number.isFinite(coordinate.lat) && Number.isFinite(coordinate.lon)));
      setMessage('Restoring saved candidates…');
    } catch {
      setMessage('No valid saved candidates were found.');
    }
  };
  const share = async () => {
    const url = new URL(window.location.href);
    url.searchParams.set('plans', coordinates.map((coordinate) => `${coordinate.lat.toFixed(5)},${coordinate.lon.toFixed(5)}`).join(';'));
    window.history.replaceState(null, '', url);
    await navigator.clipboard.writeText(url.toString()).catch(() => {});
    setMessage('Plan link copied.');
  };

  if (!enabled || (!planMode && plans.length === 0)) return null;
  return (
    <aside className="planner-comparison" aria-label="Repeater plan comparison">
      <header><div><span>Planning workspace</span><h2>{plans.length} candidate{plans.length === 1 ? '' : 's'}</h2></div></header>
      {plans.length > 0 ? (
        <>
          <div className="planner-comparison__summary">
            <div><strong>{comparison.ready}/{plans.length}</strong><span>computed</span></div>
            <div><strong>{comparison.uniquePeers}</strong><span>unique predicted peers</span></div>
            <div><strong>{comparison.pairwiseOverlapKm2.toFixed(0)} km²</strong><span>pairwise radial overlap</span></div>
          </div>
          <ol>{plans.map((plan, index) => (
            <li key={plan.id}><span>Site {index + 1}</span><code>{plan.lat.toFixed(4)}, {plan.lon.toFixed(4)}</code><strong>{plan.status === 'ready' ? `${plan.coverage?.predicted_links?.length ?? 0} links` : plan.status}</strong></li>
          ))}</ol>
          <div className="planner-comparison__actions">
            <button type="button" onClick={save}>Save</button><button type="button" onClick={share}>Share</button><button type="button" onClick={restore}>Restore saved</button>
          </div>
        </>
      ) : <p>Place up to five candidate repeaters on the map to compare reach and predicted peers.</p>}
      {message && <small role="status">{message}</small>}
    </aside>
  );
};
