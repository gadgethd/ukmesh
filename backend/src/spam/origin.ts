import type { ObserverObservation, OriginEstimate, ConfidenceLevel } from './types.js';
import type { SpamMessageConfig } from './config.js';

// ---------------------------------------------------------------------------
// Coarse origin estimation
//
// We never know exactly where a sender is. But every observer that hears a
// message constrains it: strong signal / low hop count means "near here".
// Combining many observers (which differ in location, path and signal)
// produces a rough weighted centroid plus an honest uncertainty radius and
// confidence. Output is intentionally coarse — a region, not an address.
// ---------------------------------------------------------------------------

/** Coarse UK region centroids for snapping an estimate to a named area. */
export const UK_REGIONS: Array<{ name: string; lat: number; lon: number }> = [
  { name: 'Greater London', lat: 51.5074, lon: -0.1278 },
  { name: 'South East England', lat: 51.2, lon: -0.7 },
  { name: 'Hampshire & Solent', lat: 50.95, lon: -1.36 },
  { name: 'Dorset', lat: 50.72, lon: -2.0 },
  { name: 'Kent', lat: 51.27, lon: 0.85 },
  { name: 'South West England', lat: 50.8, lon: -3.5 },
  { name: 'Cornwall', lat: 50.4, lon: -4.7 },
  { name: 'Bristol & Bath', lat: 51.45, lon: -2.58 },
  { name: 'East of England', lat: 52.2, lon: 0.5 },
  { name: 'Norfolk', lat: 52.63, lon: 1.0 },
  { name: 'West Midlands', lat: 52.48, lon: -1.9 },
  { name: 'East Midlands', lat: 52.95, lon: -1.15 },
  { name: 'Lincolnshire', lat: 53.2, lon: -0.54 },
  { name: 'Greater Manchester', lat: 53.48, lon: -2.24 },
  { name: 'Merseyside', lat: 53.41, lon: -2.98 },
  { name: 'West Yorkshire', lat: 53.8, lon: -1.55 },
  { name: 'South Yorkshire', lat: 53.38, lon: -1.47 },
  { name: 'East Yorkshire & Humber', lat: 53.74, lon: -0.35 },
  { name: 'North Yorkshire', lat: 54.0, lon: -1.5 },
  { name: 'Teesside', lat: 54.57, lon: -1.23 },
  { name: 'Tyne & Wear', lat: 54.97, lon: -1.61 },
  { name: 'Cumbria', lat: 54.58, lon: -2.79 },
  { name: 'Lancashire', lat: 53.8, lon: -2.7 },
  { name: 'North Wales', lat: 53.13, lon: -3.8 },
  { name: 'Mid Wales', lat: 52.4, lon: -3.9 },
  { name: 'South Wales', lat: 51.6, lon: -3.4 },
  { name: 'Dumfries & Galloway', lat: 55.05, lon: -3.9 },
  { name: 'Central Scotland', lat: 55.95, lon: -3.6 },
  { name: 'Edinburgh & Lothian', lat: 55.95, lon: -3.19 },
  { name: 'Greater Glasgow', lat: 55.86, lon: -4.25 },
  { name: 'North East Scotland', lat: 57.15, lon: -2.1 },
  { name: 'Scottish Highlands', lat: 57.3, lon: -4.5 },
  { name: 'Northern Ireland', lat: 54.6, lon: -5.93 },
  { name: 'Channel Islands', lat: 49.42, lon: -2.36 },
];

function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** Map an observer's signal/hop metadata to a 0..1 proximity weight. */
function observerWeight(o: ObserverObservation): number {
  // Hop count is the strongest cue: 0 hops ≈ direct reception.
  const hop = o.hopCount == null ? 4 : Math.max(0, o.hopCount);
  const hopFactor = 1 / (1 + hop);

  // SNR: roughly -10 dB (weak) .. +12 dB (strong) -> 0.2 .. 1.
  let snrFactor = 0.5;
  if (o.snr != null && Number.isFinite(o.snr)) {
    snrFactor = Math.max(0.2, Math.min(1, (o.snr + 10) / 22));
  }

  // RSSI: roughly -120 dBm (weak) .. -40 dBm (strong) -> 0.2 .. 1.
  let rssiFactor = 0.5;
  if (o.rssi != null && Number.isFinite(o.rssi)) {
    rssiFactor = Math.max(0.2, Math.min(1, (o.rssi + 120) / 80));
  }

  return hopFactor * (0.5 * snrFactor + 0.5 * rssiFactor) + 0.05;
}

/** Collapse multiple receptions by the same observer to its single best one. */
function dedupeObservers(observers: ObserverObservation[]): ObserverObservation[] {
  const best = new Map<string, ObserverObservation>();
  for (const o of observers) {
    if (!Number.isFinite(o.lat) || !Number.isFinite(o.lon)) continue;
    if (Math.abs(o.lat) < 1e-9 && Math.abs(o.lon) < 1e-9) continue; // null island
    const prev = best.get(o.observerId);
    if (!prev || observerWeight(o) > observerWeight(prev)) best.set(o.observerId, o);
  }
  return [...best.values()];
}

export function nearestRegion(lat: number, lon: number, snapKm: number): string {
  let bestName = 'United Kingdom (approximate)';
  let bestDist = Infinity;
  for (const r of UK_REGIONS) {
    const d = haversineKm(lat, lon, r.lat, r.lon);
    if (d < bestDist) {
      bestDist = d;
      bestName = r.name;
    }
  }
  return bestDist <= snapKm ? bestName : 'United Kingdom (approximate)';
}

export function levelFor(confidence: number, observerCount: number, cfg: SpamMessageConfig): ConfidenceLevel {
  if (observerCount < cfg.originMinObservers) return 'insufficient';
  if (confidence >= 0.66) return 'high';
  if (confidence >= 0.4) return 'medium';
  return 'low';
}

/** Signal-weighted centroid + weighted RMS spread of a set of observers. */
function weightedCloud(observers: ObserverObservation[]): { lat: number; lon: number; rmsKm: number } {
  let sumW = 0;
  let sumLat = 0;
  let sumLon = 0;
  for (const o of observers) {
    const w = observerWeight(o);
    sumW += w;
    sumLat += w * o.lat;
    sumLon += w * o.lon;
  }
  const lat = sumLat / sumW;
  const lon = sumLon / sumW;
  let sumWd2 = 0;
  for (const o of observers) {
    const w = observerWeight(o);
    const d = haversineKm(lat, lon, o.lat, o.lon);
    sumWd2 += w * d * d;
  }
  return { lat, lon, rmsKm: Math.sqrt(sumWd2 / sumW) };
}

/**
 * Estimate the rough origin area of a spam cluster from its observers.
 *
 * Key idea: a message floods outward from its source, so an observer that hears
 * it at a LOW hop count sits close to the source, while distant relays only
 * heard it after it crossed the network. We therefore anchor the estimate on the
 * CLOSEST available cohort of receivers (the lowest hop counts we have) — never
 * the geometric middle of the whole relay cloud. Confidence is driven by how
 * close that nearest reception was, both in absolute hops and relative to how far
 * the flood otherwise travelled. This works UK-wide: in dense areas the closest
 * cohort is a 0–2 hop direct reception (tight, high confidence); in sparse areas
 * it may be several hops out, which honestly widens the radius and lowers
 * confidence. Output is always coarse: a region, a radius and a confidence.
 */
export function estimateOrigin(
  rawObservers: ObserverObservation[],
  cfg: SpamMessageConfig,
): OriginEstimate {
  const observers = dedupeObservers(rawObservers);
  const observerCount = observers.length;

  if (observerCount < cfg.originMinObservers) {
    return {
      lat: null,
      lon: null,
      radiusKm: null,
      region: 'Unknown',
      confidence: 0,
      level: 'insufficient',
      observerCount,
      reasons: [
        observerCount === 0
          ? 'No geolocated observers heard this cluster'
          : `Only ${observerCount} geolocated observer(s) — not enough to triangulate`,
      ],
    };
  }

  const hops = observers
    .map((o) => o.hopCount)
    .filter((h): h is number => h != null && Number.isFinite(h));
  const minHop = hops.length ? Math.min(...hops) : null;
  const maxHop = hops.length ? Math.max(...hops) : null;

  // The closest-receiver cohort: observers within `min-hop + slack` hops (but at
  // least everyone inside `originNearHopMax`). These sit nearest the source and
  // anchor the estimate; the far end of the flood is excluded entirely.
  const cohortCut = minHop != null ? Math.max(cfg.originNearHopMax, minHop + cfg.originNearHopSlack) : null;
  const cohort =
    cohortCut != null
      ? observers.filter((o) => o.hopCount != null && o.hopCount <= cohortCut)
      : observers; // no hop data anywhere — cannot rank, use the full cloud

  const { lat, lon, rmsKm } = weightedCloud(cohort);

  // Radius: spread of the cohort + ~one link range per hop to the source.
  const hopAllowanceKm = (minHop != null ? minHop + 1 : 4) * cfg.originPerHopKm;
  let radiusKm = Math.max(cfg.originMinRadiusKm, rmsKm + hopAllowanceKm);
  radiusKm = Math.ceil(radiusKm / 5) * 5; // coarse 5 km buckets

  // Confidence is dominated by proximity of the nearest reception, both absolute
  // (low hop = close) and relative (much closer than the flood's far end).
  let proximity = 0;
  if (minHop != null) {
    const absCloseness = Math.max(0, Math.min(1, 1 - minHop / 6));
    const relCloseness = maxHop != null && maxHop > minHop ? (maxHop - minHop) / maxHop : 0;
    proximity = Math.max(0, Math.min(1, 0.5 * absCloseness + 0.5 * relCloseness));
  }
  const corroboration = Math.min(1, cohort.length / 3);
  const spreadScore = cohort.length >= 2 ? Math.max(0, Math.min(1, 1 - rmsKm / 60)) : 0.6;
  let confidence = Math.max(0, Math.min(1, 0.7 * proximity + 0.15 * corroboration + 0.15 * spreadScore));
  if (minHop == null) confidence = Math.min(confidence, 0.4); // no hop evidence -> never "high"

  const reasons: string[] = [`${observerCount} geolocated observers`];
  if (minHop != null) {
    reasons.push(
      `${cohort.length} heard it within ${cohortCut} hops of the source` +
        ` (closest ${minHop} hop${minHop === 1 ? '' : 's'})`,
    );
    if (maxHop != null && maxHop > minHop + 1) {
      reasons.push(`flood otherwise relayed up to ${maxHop} hops — closest receivers localise it`);
    }
    if (minHop > cfg.originNearHopMax) {
      reasons.push('closest reception is several hops out — broad area only');
    } else if (cohort.length === 1) {
      reasons.push('single near receiver — region well constrained, exact spot less so');
    } else {
      reasons.push(`closest receivers within ≈ ${Math.round(rmsKm)} km of each other`);
    }
  } else {
    reasons.push('no hop data — broad area only');
  }

  const region = nearestRegion(lat, lon, cfg.regionSnapKm);
  const level = levelFor(confidence, observerCount, cfg);

  return { lat, lon, radiusKm, region, confidence, level, observerCount, reasons };
}
