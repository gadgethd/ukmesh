export type PathArcColor = [number, number, number, number];

/**
 * How long a completed live path remains fully visible. The fade begins after
 * this interval.
 */
export const PATH_LINE_TTL_MS = 30_000;

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

const GOLDEN_ANGLE_DEGREES = 137.508;
const PACKET_COLOR_SATURATION = 0.86;
const PACKET_COLOR_LIGHTNESS = 0.62;

function normalizedPacketHash(packetHash: string | null | undefined): string {
  const normalized = packetHash?.trim().toUpperCase();
  return normalized || 'UNKNOWN';
}

function packetHashSeed(packetHash: string): number {
  // FNV-1a gives a stable seed for both hex hashes and arbitrary fallback
  // identities without relying on locale, runtime, or render order.
  let seed = 2166136261;
  for (let index = 0; index < packetHash.length; index += 1) {
    seed ^= packetHash.charCodeAt(index);
    seed = Math.imul(seed, 16777619);
  }
  return seed >>> 0;
}

function hslToRgb(hue: number, saturation: number, lightness: number): [number, number, number] {
  const chroma = (1 - Math.abs(2 * lightness - 1)) * saturation;
  const hueSector = hue / 60;
  const secondary = chroma * (1 - Math.abs((hueSector % 2) - 1));
  const match = lightness - chroma / 2;
  let red = 0;
  let green = 0;
  let blue = 0;

  if (hueSector < 1) [red, green, blue] = [chroma, secondary, 0];
  else if (hueSector < 2) [red, green, blue] = [secondary, chroma, 0];
  else if (hueSector < 3) [red, green, blue] = [0, chroma, secondary];
  else if (hueSector < 4) [red, green, blue] = [0, secondary, chroma];
  else if (hueSector < 5) [red, green, blue] = [secondary, 0, chroma];
  else [red, green, blue] = [chroma, 0, secondary];

  return [
    Math.round((red + match) * 255),
    Math.round((green + match) * 255),
    Math.round((blue + match) * 255),
  ];
}

/**
 * Return a deterministic, vivid RGB color for a packet identity. The hash is
 * converted to a stable seed and advanced by the golden angle so independent
 * packet identities spread around the hue wheel instead of clustering in a
 * confidence palette.
 */
export function packetIdentityColor(packetHash: string | null | undefined): [number, number, number] {
  const seed = packetHashSeed(normalizedPacketHash(packetHash));
  const hue = (seed * GOLDEN_ANGLE_DEGREES) % 360;
  return hslToRgb(hue, PACKET_COLOR_SATURATION, PACKET_COLOR_LIGHTNESS);
}

function alpha(value: number, opacity: number): number {
  return Math.round(value * Math.max(0, Math.min(1, opacity)));
}

export function packetPathColors(packetHash: string | null | undefined, opacity = 1): {
  bloomSource: PathArcColor;
  bloomTarget: PathArcColor;
  coreSource: PathArcColor;
  coreTarget: PathArcColor;
} {
  const rgb = packetIdentityColor(packetHash);
  return {
    bloomSource: [...rgb, alpha(35, opacity)],
    bloomTarget: [...rgb, alpha(70, opacity)],
    coreSource: [...rgb, alpha(200, opacity)],
    coreTarget: [...rgb, alpha(255, opacity)],
  };
}
