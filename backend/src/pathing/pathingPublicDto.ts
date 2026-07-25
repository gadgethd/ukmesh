export type PublicPathPoint = [number, number];
export type PublicPathSegment = [PublicPathPoint, PublicPathPoint];

export type PublicPathExplanation = {
  evidenceLevel: 'high' | 'medium' | 'low';
  summary: string;
  reasons: string[];
  alternativesConsidered: number;
  limitations?: string[];
};

export type PublicBetaResultDto = {
  ok: boolean;
  packetHash: string;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  permutationCount: number;
  remainingHops: number | null;
  purplePath: PublicPathPoint[] | null;
  extraPurplePaths: PublicPathPoint[][];
  redPath: PublicPathPoint[] | null;
  redSegments: PublicPathSegment[];
  completionPaths: PublicPathPoint[][];
  explanation?: PublicPathExplanation;
};

export type PublicMultiObserverDto = {
  ok: boolean;
  packetHash: string;
  observerCount: number;
  sharedPrefixLength: number;
  results: PublicBetaResultDto[];
};

function ownValue(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor && 'value' in descriptor ? descriptor.value : undefined;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function finiteNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function nullableNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function pathPoint(value: unknown): PublicPathPoint | null {
  if (!Array.isArray(value) || value.length !== 2) return null;
  const lat = value[0];
  const lon = value[1];
  if (
    typeof lat !== 'number'
    || typeof lon !== 'number'
    || !Number.isFinite(lat)
    || !Number.isFinite(lon)
    || lat < -90
    || lat > 90
    || lon < -180
    || lon > 180
  ) return null;
  return [lat, lon];
}

function path(value: unknown): PublicPathPoint[] | null {
  if (!Array.isArray(value)) return null;
  const projected: PublicPathPoint[] = [];
  for (const entry of value) {
    const point = pathPoint(entry);
    if (!point) return null;
    projected.push(point);
  }
  return projected;
}

function paths(value: unknown): PublicPathPoint[][] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    const projected = path(entry);
    return projected ? [projected] : [];
  });
}

function segments(value: unknown): PublicPathSegment[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return [];
    const first = pathPoint(entry[0]);
    const second = pathPoint(entry[1]);
    return first && second ? [[first, second] as PublicPathSegment] : [];
  });
}

function explanation(value: unknown): PublicPathExplanation | undefined {
  if (!isPlainRecord(value)) return undefined;
  const evidence = ownValue(value, 'evidenceLevel');
  const summary = ownValue(value, 'summary');
  const rawReasons = ownValue(value, 'reasons');
  if (
    (evidence !== 'high' && evidence !== 'medium' && evidence !== 'low')
    || typeof summary !== 'string'
    || !Array.isArray(rawReasons)
  ) return undefined;
  const reasons = rawReasons.filter((reason): reason is string => typeof reason === 'string').map(String);
  const rawLimitations = ownValue(value, 'limitations');
  const limitations = Array.isArray(rawLimitations)
    ? rawLimitations.filter((item): item is string => typeof item === 'string').map(String)
    : undefined;
  return {
    evidenceLevel: evidence,
    summary,
    reasons,
    alternativesConsidered: finiteNumber(ownValue(value, 'alternativesConsidered')),
    ...(limitations ? { limitations } : {}),
  };
}

export function toPublicBetaResultDto(value: unknown): PublicBetaResultDto {
  if (!isPlainRecord(value)) throw new Error('INVALID_PATH_RESULT');
  const packetHash = ownValue(value, 'packetHash');
  const mode = ownValue(value, 'mode');
  if (
    typeof packetHash !== 'string'
    || (mode !== 'resolved' && mode !== 'fallback' && mode !== 'none')
  ) throw new Error('INVALID_PATH_RESULT');
  const publicExplanation = explanation(ownValue(value, 'explanation'));
  return {
    ok: ownValue(value, 'ok') === true,
    packetHash,
    mode,
    confidence: nullableNumber(ownValue(value, 'confidence')),
    permutationCount: finiteNumber(ownValue(value, 'permutationCount')),
    remainingHops: nullableNumber(ownValue(value, 'remainingHops')),
    purplePath: path(ownValue(value, 'purplePath')),
    extraPurplePaths: paths(ownValue(value, 'extraPurplePaths')),
    redPath: path(ownValue(value, 'redPath')),
    redSegments: segments(ownValue(value, 'redSegments')),
    completionPaths: paths(ownValue(value, 'completionPaths')),
    ...(publicExplanation ? { explanation: publicExplanation } : {}),
  };
}

export function toPublicMultiObserverDto(value: unknown): PublicMultiObserverDto {
  if (!isPlainRecord(value)) throw new Error('INVALID_PATH_RESULT');
  const packetHash = ownValue(value, 'packetHash');
  const rawResults = ownValue(value, 'results');
  if (typeof packetHash !== 'string' || !Array.isArray(rawResults)) {
    throw new Error('INVALID_PATH_RESULT');
  }
  return {
    ok: ownValue(value, 'ok') === true,
    packetHash,
    observerCount: finiteNumber(ownValue(value, 'observerCount')),
    sharedPrefixLength: finiteNumber(ownValue(value, 'sharedPrefixLength')),
    results: rawResults.map(toPublicBetaResultDto),
  };
}
