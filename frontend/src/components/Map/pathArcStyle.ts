export type PathArcColor = [number, number, number, number];

/**
 * How long a completed live path remains fully visible. Change this to 30_000
 * for a 30-second lifetime, for example. The fade begins after this interval.
 */
export const PATH_LINE_TTL_MS = 60_000;

export const PATH_HOP_ANIMATION_MS = 400;
export const PATH_LINE_FADE_MS = 1_000;
export const PATH_ARC_HEIGHT = 0.15;
export const PATH_ARC_BLOOM_WIDTH = 10;
export const PATH_ARC_CORE_WIDTH = 2;

export type PathConfidenceBand = 'low' | 'high' | 'confirmed';

// These thresholds and RGB values are the confidence bands already used by
// DeckGLOverlay's evidence badges. Confirmed routes retain its cyan ArcLayer
// gradient; partial routes use its amber/red evidence colours.
export function pathConfidenceBand(confidence: number | null): PathConfidenceBand {
  if (confidence != null && confidence >= 0.75) return 'confirmed';
  if (confidence != null && confidence >= 0.4) return 'high';
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
  if (band === 'confirmed') {
    return {
      bloomSource: [0, 196, 255, alpha(35, opacity)],
      bloomTarget: [0, 196, 255, alpha(70, opacity)],
      coreSource: [120, 220, 255, alpha(200, opacity)],
      coreTarget: [200, 245, 255, alpha(255, opacity)],
    };
  }

  const rgb: [number, number, number] = band === 'high'
    ? [251, 191, 36]
    : [239, 68, 68];
  return {
    bloomSource: [...rgb, alpha(35, opacity)],
    bloomTarget: [...rgb, alpha(70, opacity)],
    coreSource: [...rgb, alpha(200, opacity)],
    coreTarget: [...rgb, alpha(255, opacity)],
  };
}
