/**
 * Custom LOS computation: samples terrain between two points and returns
 * path segments coloured by whether terrain obstructs the line of sight.
 */
import { sampleTerrainProfile } from './terrainSampler.js';
import { TERRAIN_CONFIG } from '../components/Map/mapConfig.js';
import type { CustomLosPoint, CustomLosSegment } from '../components/Map/types.js';

const ANTENNA_H = 10; // metres above stated elevation
const K_FACTOR = 4 / 3; // effective Earth radius multiplier for radio propagation
const R_EARTH_M = 6_371_000;

function haversineM(lon1: number, lat1: number, lon2: number, lat2: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R_EARTH_M * Math.asin(Math.sqrt(a));
}

export async function computeCustomLos(
  start: CustomLosPoint,
  end: CustomLosPoint,
  nSamples = 120,
): Promise<CustomLosSegment[]> {
  const EXAG = TERRAIN_CONFIG.exaggeration;
  const profile = await sampleTerrainProfile(start.lon, start.lat, end.lon, end.lat, nSamples);

  const startAlt = start.elevation_m + ANTENNA_H;
  const endAlt = end.elevation_m + ANTENNA_H;
  const totalDist = haversineM(start.lon, start.lat, end.lon, end.lat);

  // Annotate each sample with obstruction status
  const samples = profile.map((p, i) => {
    const t = i / nSamples;
    const losAlt = startAlt + t * (endAlt - startAlt);
    // Earth curvature bulge at this point along the path (radio effective radius)
    const d = t * totalDist;
    const bulge = (d * (totalDist - d)) / (2 * K_FACTOR * R_EARTH_M);
    return {
      lon: p[0],
      lat: p[1],
      displayAlt: (losAlt - bulge) * EXAG,
      obstructed: p[2] + bulge > losAlt,
    };
  });

  // Group consecutive same-status samples into segments
  const segments: CustomLosSegment[] = [];
  let i = 0;
  while (i < samples.length) {
    const obstructed = samples[i].obstructed;
    const path: [number, number, number][] = [];
    while (i < samples.length && samples[i].obstructed === obstructed) {
      path.push([samples[i].lon, samples[i].lat, samples[i].displayAlt]);
      i++;
    }
    // Share boundary point with next segment for a seamless join
    if (i < samples.length) {
      path.push([samples[i].lon, samples[i].lat, samples[i].displayAlt]);
    }
    if (path.length >= 2) segments.push({ path, obstructed });
  }

  return segments;
}
