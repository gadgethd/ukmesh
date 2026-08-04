export type PathArcColor = [number, number, number, number];

/**
 * How long a completed live path remains fully visible. Change this to 30_000
 * for a 30-second lifetime, for example. The fade begins after this interval.
 */
export const PATH_LINE_TTL_MS = 5_000;

export const PATH_HOP_ANIMATION_MS = 400;
export const PATH_LINE_FADE_MS = 1_000;
// Kept for the non-terrain packet arcs in DeckGLOverlay. ArcLayer interprets
// this value as a multiplier of each segment's projected length.
export const PATH_ARC_HEIGHT = 0.15;
// Animated live paths convert this physical lift to ArcLayer's per-segment
// multiplier, so a short hop and a long hop use the same aerial scale.
export const PATH_ARC_HEIGHT_M = 120;
export const PATH_ARC_SEGMENTS = 32;
export const PATH_ARC_BLOOM_WIDTH = 10;
export const PATH_ARC_CORE_WIDTH = 2;

// Live-path endpoints sit just above the sampled DEM when MapLibre terrain is active.
export const PATH_TERRAIN_CLEARANCE_M = 32;

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
