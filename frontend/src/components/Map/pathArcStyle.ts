export type PathArcColor = [number, number, number, number];

/**
 * How long a completed live path remains fully visible. Change this to 30_000
 * for a 30-second lifetime, for example. The fade begins after this interval.
 */
export const PATH_LINE_TTL_MS = 15_000;

export const PATH_HOP_ANIMATION_MS = 400;
export const PATH_LINE_FADE_MS = 1_000;
export const PATH_ARC_HEIGHT = 0.15;
export const PATH_ARC_BLOOM_WIDTH = 10;
export const PATH_ARC_CORE_WIDTH = 2;

// Live-path endpoints sit just above the sampled DEM when MapLibre terrain is
// active. The peak is deliberately much higher so the animated hop reads as
// an airborne transmission rather than a line being dragged over the ground.
export const PATH_TERRAIN_CLEARANCE_M = 32;
export const PATH_HOP_PEAK_HEIGHT_M = 300;
export const PATH_HOP_PEAK_DISTANCE_SCALE = 0.1;

export type PathConfidenceBand = 'low' | 'mid' | 'high';

// Confidence colours are intentionally traffic-light ordered on every path
// surface: high is green, mid is yellow, and low/unknown is red.
export function pathConfidenceBand(confidence: number | null): PathConfidenceBand {
  if (confidence != null && confidence >= 0.75) return 'high';
  if (confidence != null && confidence >= 0.4) return 'mid';
  return 'low';
}

function alpha(value: number, opacity: number): number {
  return Math.round(value * Math.max(0, Math.min(1, opacity)));
}

export function pathArcColors(confidence: number | null, opacity = 1): {
  bloomSource: PathArcColor;
  bloomTarget: PathArcColor;
  coreSource: PathArcColor;
  coreTarget: PathArcColor;
} {
  const band = pathConfidenceBand(confidence);
  const rgb: [number, number, number] = band === 'high'
    ? [34, 197, 94]
    : band === 'mid'
      ? [250, 204, 21]
      : [239, 68, 68];
  return {
    bloomSource: [...rgb, alpha(35, opacity)],
    bloomTarget: [...rgb, alpha(70, opacity)],
    coreSource: [...rgb, alpha(200, opacity)],
    coreTarget: [...rgb, alpha(255, opacity)],
  };
}
