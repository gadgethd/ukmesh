export type PublicPathPoint = [number, number];
export type PublicPathSegment = [PublicPathPoint, PublicPathPoint];

export type PublicPathExplanation = {
  evidenceLevel: 'high' | 'medium' | 'low';
  summary: string;
  reasons: string[];
  alternativesConsidered: number;
  limitations?: string[];
};

export type PublicCanonicalPathNode = {
  position: number;
  hash: string;
  nodeId: string | null;
  name: string | null;
  lat: number | null;
  lon: number | null;
  ambiguous: boolean;
  confidence: number | null;
};

export type PublicPathObserver = { observerId: string };

export type PublicBetaResultDto = {
  ok: boolean;
  packetHash: string;
  mode: 'resolved' | 'fallback' | 'none';
  confidence: number | null;
  canonicalPath: PublicCanonicalPathNode[];
  observers: PublicPathObserver[];
  network: string;
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
  network: string;
  observerCount: number;
  sharedPrefixLength: number;
  canonicalPath: PublicCanonicalPathNode[];
  observers: PublicPathObserver[];
  confidence: number | null;
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

function projectPoint(value: unknown): PublicPathPoint | null {
  if (!Array.isArray(value) || value.length !== 2) return null;
  const [lat, lon] = value;
  if (typeof lat !== 'number' || typeof lon !== 'number'
    || !Number.isFinite(lat) || !Number.isFinite(lon)
    || lat < -90 || lat > 90 || lon < -180 || lon > 180) return null;
  return [lat, lon];
}

function projectPath(value: unknown): PublicPathPoint[] | null {
  if (!Array.isArray(value)) return null;
  const output: PublicPathPoint[] = [];
  for (const entry of value) {
    const point = projectPoint(entry);
    if (!point) return null;
    output.push(point);
  }
  return output;
}

function projectPaths(value: unknown): PublicPathPoint[][] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    const projected = projectPath(entry);
    return projected ? [projected] : [];
  });
}

function projectSegments(value: unknown): PublicPathSegment[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2) return [];
    const first = projectPoint(entry[0]);
    const second = projectPoint(entry[1]);
    return first && second ? [[first, second] as PublicPathSegment] : [];
  });
}

function projectExplanation(value: unknown): PublicPathExplanation | undefined {
  if (!isPlainRecord(value)) return undefined;
  const evidenceLevel = ownValue(value, 'evidenceLevel');
  const summary = ownValue(value, 'summary');
  const reasons = ownValue(value, 'reasons');
  if ((evidenceLevel !== 'high' && evidenceLevel !== 'medium' && evidenceLevel !== 'low')
    || typeof summary !== 'string' || !Array.isArray(reasons)) return undefined;
  const limitations = ownValue(value, 'limitations');
  return {
    evidenceLevel,
    summary,
    reasons: reasons.filter((item): item is string => typeof item === 'string'),
    alternativesConsidered: finiteNumber(ownValue(value, 'alternativesConsidered')),
    ...(Array.isArray(limitations)
      ? { limitations: limitations.filter((item): item is string => typeof item === 'string') }
      : {}),
  };
}

function projectCanonicalPath(value: unknown): PublicCanonicalPathNode[] {
  if (!Array.isArray(value)) return [];
  const nodes: PublicCanonicalPathNode[] = [];
  for (const item of value) {
    if (!isPlainRecord(item)) continue;
    const hash = ownValue(item, 'hash');
    const nodeId = ownValue(item, 'nodeId');
    const name = ownValue(item, 'name');
    if (typeof hash !== 'string') continue;
    nodes.push({
      position: finiteNumber(ownValue(item, 'position'), -1),
      hash,
      nodeId: typeof nodeId === 'string' ? nodeId : null,
      name: typeof name === 'string' ? name : null,
      lat: nullableNumber(ownValue(item, 'lat')),
      lon: nullableNumber(ownValue(item, 'lon')),
      ambiguous: ownValue(item, 'ambiguous') === true,
      confidence: nullableNumber(ownValue(item, 'confidence')),
    });
  }
  return nodes;
}

function projectObservers(value: unknown): PublicPathObserver[] {
  if (!Array.isArray(value)) return [];
  const observers: PublicPathObserver[] = [];
  for (const item of value) {
    if (isPlainRecord(item)) {
      const observerId = ownValue(item, 'observerId');
      if (typeof observerId === 'string') observers.push({ observerId });
    }
  }
  return observers;
}

export function toPublicBetaResultDto(value: unknown): PublicBetaResultDto {
  if (!isPlainRecord(value)) throw new Error('INVALID_PATH_RESULT');
  const packetHash = ownValue(value, 'packetHash');
  const mode = ownValue(value, 'mode');
  if (typeof packetHash !== 'string'
    || (mode !== 'resolved' && mode !== 'fallback' && mode !== 'none')) {
    throw new Error('INVALID_PATH_RESULT');
  }
  const explanation = projectExplanation(ownValue(value, 'explanation'));
  const network = ownValue(value, 'network');
  return {
    ok: ownValue(value, 'ok') === true,
    packetHash,
    mode,
    confidence: nullableNumber(ownValue(value, 'confidence')),
    canonicalPath: projectCanonicalPath(ownValue(value, 'canonicalPath')),
    observers: projectObservers(ownValue(value, 'observers')),
    network: typeof network === 'string' ? network : '',
    permutationCount: finiteNumber(ownValue(value, 'permutationCount')),
    remainingHops: nullableNumber(ownValue(value, 'remainingHops')),
    purplePath: projectPath(ownValue(value, 'purplePath')),
    extraPurplePaths: projectPaths(ownValue(value, 'extraPurplePaths')),
    redPath: projectPath(ownValue(value, 'redPath')),
    redSegments: projectSegments(ownValue(value, 'redSegments')),
    completionPaths: projectPaths(ownValue(value, 'completionPaths')),
    ...(explanation ? { explanation } : {}),
  };
}

export function toPublicMultiObserverDto(value: unknown): PublicMultiObserverDto {
  if (!isPlainRecord(value)) throw new Error('INVALID_PATH_RESULT');
  const packetHash = ownValue(value, 'packetHash');
  const results = ownValue(value, 'results');
  if (typeof packetHash !== 'string' || !Array.isArray(results)) throw new Error('INVALID_PATH_RESULT');
  const network = ownValue(value, 'network');
  return {
    ok: ownValue(value, 'ok') === true,
    packetHash,
    network: typeof network === 'string' ? network : '',
    observerCount: finiteNumber(ownValue(value, 'observerCount')),
    sharedPrefixLength: finiteNumber(ownValue(value, 'sharedPrefixLength')),
    canonicalPath: projectCanonicalPath(ownValue(value, 'canonicalPath')),
    observers: projectObservers(ownValue(value, 'observers')),
    confidence: nullableNumber(ownValue(value, 'confidence')),
    results: results.map(toPublicBetaResultDto),
  };
}
