/**
 * Custom LOS computation: samples terrain between two points and returns
 * path segments coloured by whether terrain obstructs the line of sight.
 */
import { sampleTerrainProfile } from './terrainSampler.js';
import { TERRAIN_CONFIG } from '../components/Map/mapConfig.js';
import type { CustomLosPoint, CustomLosSegment } from '../components/Map/types.js';

const ANTENNA_H = 10; // metres above stated elevation

export async function computeCustomLos(
  start: CustomLosPoint,
  end: CustomLosPoint,
  nSamples = 120,
): Promise<CustomLosSegment[]> {
  const EXAG = TERRAIN_CONFIG.exaggeration;
  const profile = await sampleTerrainProfile(start.lon, start.lat, end.lon, end.lat, nSamples);

  const startAlt = start.elevation_m + ANTENNA_H;
  const endAlt = end.elevation_m + ANTENNA_H;

  // Annotate each sample with obstruction status
  const samples = profile.map((p, i) => {
    const t = i / nSamples;
    const losAlt = startAlt + t * (endAlt - startAlt);
    return {
      lon: p[0],
      lat: p[1],
      displayAlt: losAlt * EXAG,
      obstructed: p[2] > losAlt,
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
