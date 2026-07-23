import type { NormalizedMessage } from './types.js';

// ---------------------------------------------------------------------------
// Message text normalization
//
// Goal: collapse the cosmetic variations a spammer adds between repeats
// (case, whitespace, punctuation, stretched letters, tracking-tweaked URLs)
// so that near-duplicate messages normalize to (nearly) identical strings.
// ---------------------------------------------------------------------------

/** The spam-page URL spammers sometimes echo. Treated as a marker, not proof. */
export const SPAM_MARKER = 'ukmesh.com/spam';

// Matches http(s) URLs, scheme-less www. URLs, and bare domains with a TLD.
const URL_RE =
  /\b((?:https?:\/\/)?(?:www\.)?[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+(?:\/[^\s]*)?)/gi;

const TLD_HINT_RE = /\.(com|net|org|io|co|uk|me|app|dev|xyz|info|link|gg|tv|to|ru|cn|de|fr|nl)\b/i;

/**
 * Canonicalize a URL so cosmetic/tracking differences collapse:
 * drop scheme + leading "www.", lowercase host, strip query/fragment and
 * trailing punctuation, and drop a lone trailing slash.
 */
export function canonicalizeUrl(raw: string): string {
  let u = raw.trim().toLowerCase();
  u = u.replace(/^https?:\/\//, '');
  u = u.replace(/^www\./, '');
  // Strip query string and fragment — common spots for per-send tracking noise.
  u = u.replace(/[?#].*$/, '');
  // Strip trailing punctuation left over from sentence context.
  u = u.replace(/[.,!?;:)\]}'"]+$/, '');
  // Drop a lone trailing slash but keep meaningful paths.
  u = u.replace(/\/$/, '');
  return u;
}

/** True if the (already canonical) URL is the spam-page marker. */
function isSpamMarker(canonical: string): boolean {
  return canonical === SPAM_MARKER || canonical.startsWith(`${SPAM_MARKER}/`) ||
    canonical.startsWith(`${SPAM_MARKER}?`);
}

/** Collapse runs of the same character (3+) down to 2 ("heyyyyy" -> "heyy"). */
export function collapseRepeats(text: string): string {
  return text.replace(/(.)\1{2,}/g, '$1$1');
}

/**
 * Normalize a message body for fuzzy comparison.
 * Order matters: URLs are extracted first (before punctuation stripping would
 * mangle them), then the remaining text is de-cased / de-punctuated / squeezed.
 */
export function normalizeMessage(input: string | null | undefined): NormalizedMessage {
  const original = input ?? '';

  // Unicode-normalize so visually identical glyphs compare equal.
  let working = original.normalize('NFKC');

  const urls: string[] = [];
  let hasSpamMarker = false;

  // Extract + canonicalize URLs, replacing each with a stable placeholder token.
  working = working.replace(URL_RE, (match) => {
    // Avoid treating "e.g" / "3.5" style fragments as URLs unless they look domain-ish.
    if (!/\//.test(match) && !TLD_HINT_RE.test(match)) return match;
    const canonical = canonicalizeUrl(match);
    if (!canonical.includes('.')) return match;
    urls.push(canonical);
    if (isSpamMarker(canonical)) hasSpamMarker = true;
    return ` url:${canonical} `;
  });

  // Fallback marker check on the raw text (covers odd spacings the regex missed).
  if (!hasSpamMarker && original.toLowerCase().replace(/\s+/g, '').includes(SPAM_MARKER.replace(/\s+/g, ''))) {
    hasSpamMarker = true;
  }

  let normalized = working.toLowerCase();

  // Normalize fancy punctuation to ASCII equivalents.
  normalized = normalized
    .replace(/[‘’‛′]/g, "'")
    .replace(/[“”″]/g, '"')
    .replace(/[–—−]/g, '-')
    .replace(/[…]/g, '...');

  // Strip punctuation/symbols/emoji to spaces, but keep the url:/letters/digits.
  // We protect "url:" tokens by temporarily masking the colon.
  normalized = normalized.replace(/url:/g, 'urlsep');
  normalized = normalized.replace(/[^\p{L}\p{N}\s]/gu, ' ');
  normalized = normalized.replace(/urlsep/g, 'url:');

  normalized = collapseRepeats(normalized);

  // Squeeze whitespace and trim.
  normalized = normalized.replace(/\s+/g, ' ').trim();

  const tokens = normalized.length > 0 ? normalized.split(' ') : [];

  return { original, normalized, tokens, urls, hasSpamMarker };
}

/**
 * Normalize a username / display name for comparison: drop case, emoji, role
 * suffixes and separators so "John_UK 📻" and "john uk" compare closely.
 */
export function normalizeUsername(input: string | null | undefined): string {
  let name = (input ?? '').normalize('NFKC').toLowerCase();
  name = name.replace(/[^\p{L}\p{N}\s]/gu, ' ');
  name = collapseRepeats(name);
  return name.replace(/\s+/g, ' ').trim();
}
