import type { NormalizedMessage } from './types.js';
import { normalizeUsername } from './normalize.js';

// ---------------------------------------------------------------------------
// Fuzzy similarity primitives (all pure, all return 0..1 where useful)
// ---------------------------------------------------------------------------

/** Classic Levenshtein edit distance. */
export function levenshtein(a: string, b: string): number {
  if (a === b) return 0;
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;

  let prev = new Array<number>(b.length + 1);
  let curr = new Array<number>(b.length + 1);
  for (let j = 0; j <= b.length; j++) prev[j] = j;

  for (let i = 1; i <= a.length; i++) {
    curr[0] = i;
    const ca = a.charCodeAt(i - 1);
    for (let j = 1; j <= b.length; j++) {
      const cost = ca === b.charCodeAt(j - 1) ? 0 : 1;
      curr[j] = Math.min(
        prev[j]! + 1, // deletion
        curr[j - 1]! + 1, // insertion
        prev[j - 1]! + cost, // substitution
      );
    }
    [prev, curr] = [curr, prev];
  }
  return prev[b.length]!;
}

/** Levenshtein similarity normalized to 0..1 (1 = identical). */
export function levenshteinRatio(a: string, b: string): number {
  if (a.length === 0 && b.length === 0) return 1;
  const maxLen = Math.max(a.length, b.length);
  if (maxLen === 0) return 1;
  return 1 - levenshtein(a, b) / maxLen;
}

/** Character trigrams of a string (with a leading/trailing pad). */
export function trigrams(s: string): Set<string> {
  const padded = `  ${s} `;
  const grams = new Set<string>();
  for (let i = 0; i < padded.length - 2; i++) {
    grams.add(padded.slice(i, i + 3));
  }
  return grams;
}

/** Sørensen–Dice coefficient over two sets. */
export function diceCoefficient<T>(a: Set<T>, b: Set<T>): number {
  if (a.size === 0 && b.size === 0) return 1;
  if (a.size === 0 || b.size === 0) return 0;
  let overlap = 0;
  for (const x of a) if (b.has(x)) overlap++;
  return (2 * overlap) / (a.size + b.size);
}

/** Jaccard index over two sets. */
export function jaccard<T>(a: Set<T>, b: Set<T>): number {
  if (a.size === 0 && b.size === 0) return 1;
  let inter = 0;
  for (const x of a) if (b.has(x)) inter++;
  const union = a.size + b.size - inter;
  return union === 0 ? 0 : inter / union;
}

/**
 * Combined similarity (0..1) between two normalized messages.
 *
 * Short strings are dominated by edit distance; longer strings blend
 * trigram (handles typos / inserted chars) and token-set Jaccard (handles
 * reordered words). The max keeps any single strong signal from being
 * diluted by a weaker one.
 */
export function messageSimilarity(a: NormalizedMessage, b: NormalizedMessage): number {
  const sa = a.normalized;
  const sb = b.normalized;

  if (sa.length === 0 && sb.length === 0) return 1;
  if (sa.length === 0 || sb.length === 0) return 0;
  if (sa === sb) return 1;

  // Shared canonical URL is a very strong signal on its own.
  if (a.urls.length > 0 && b.urls.length > 0) {
    const setB = new Set(b.urls);
    if (a.urls.some((u) => setB.has(u))) {
      const lev = levenshteinRatio(sa, sb);
      return Math.max(0.85, lev);
    }
  }

  const lev = levenshteinRatio(sa, sb);
  if (Math.max(sa.length, sb.length) < 12) {
    return lev;
  }

  const dice = diceCoefficient(trigrams(sa), trigrams(sb));
  const tok = jaccard(new Set(a.tokens), new Set(b.tokens));
  return Math.max(dice, tok, lev);
}

/**
 * Username similarity (0..1). Treats one name being a prefix/substring of the
 * other (e.g. "John" vs "John2", "John" vs "John UK") as strong evidence —
 * a common pattern when a spammer cycles slight name variants.
 */
export function usernameSimilarity(a: string, b: string): number {
  const na = normalizeUsername(a);
  const nb = normalizeUsername(b);
  if (na.length === 0 || nb.length === 0) return 0;
  if (na === nb) return 1;

  const lev = levenshteinRatio(na, nb);

  const [shorter, longer] = na.length <= nb.length ? [na, nb] : [nb, na];
  let affinity = 0;
  if (shorter.length >= 3 && longer.startsWith(shorter)) {
    affinity = 0.85 + 0.1 * (shorter.length / longer.length);
  } else if (shorter.length >= 4 && longer.includes(shorter)) {
    affinity = 0.8;
  }

  const dice = diceCoefficient(trigrams(na), trigrams(nb));
  return Math.min(1, Math.max(lev, affinity, dice));
}
