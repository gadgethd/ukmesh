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

/**
 * Heuristic: does a display name look machine-generated (a keygen-style random
 * token) rather than a human handle? One signal among several for
 * rotating-identity botnet detection — never sufficient on its own.
 *
 * Observed botnet style: separator-free base32-ish tokens such as
 * "0CYT64KV", "1KG3RWU7", "43DBCEEF5C", "5ZA1J31M", "16V0OP2W".
 * Human / amateur-radio handles ("M7GQW", "MU6GBG", "2E0ABC", "Rob M7JCN V3",
 * "Dave-S75", "Grog 🚲") must NOT match: composite names carry separators and
 * callsigns keep a single digit next to letters.
 */
export function looksMachineGeneratedName(input: string | null | undefined): boolean {
  const raw = (input ?? '').normalize('NFKC').trim();
  if (raw.length === 0) return false;

  // Strip pictographic decoration, then require a single alphanumeric token:
  // any remaining separator/punctuation marks the name as composite (human).
  const stripped = raw
    .replace(/[\p{Extended_Pictographic}\uFE0F\u200D]/gu, '')
    .trim();
  if (stripped.length === 0) return false;
  if (/[^A-Za-z0-9]/.test(stripped)) return false;

  const compact = stripped;
  if (compact.length < 6 || compact.length > 14) return false;

  const upper = compact.toUpperCase();
  const letterChars = compact.replace(/[^A-Za-z]/g, '');
  const letters = upper.replace(/[^A-Z]/g, '');
  const digits = upper.replace(/[^0-9]/g, '');
  // At least one digit and mostly-uppercase token (keygen style); human-ish
  // mixed case is rejected. One digit is enough — the botnet also uses
  // single-digit ids like "BKGL7DMF"; callsign shapes are excluded below.
  if (letters.length < 3 || digits.length === 0) return false;

  // Mostly-uppercase token (keygen style); human-ish mixed case is rejected.
  const upperCount = (letterChars.match(/[A-Z]/g) ?? []).length;
  if (upperCount / letterChars.length < 0.7) return false;

  // Amateur-callsign shapes (M7GQW, G0SYR, MU6GBG, 2E0ABC, GW4ABC) are human.
  if (/^[A-Z]{1,3}[0-9][A-Z]{1,4}$/.test(upper)) return false;
  if (/^[0-9][A-Z][0-9][A-Z]{1,3}$/.test(upper)) return false;

  // Keygen strings are vowel-poor.
  const vowels = (letters.match(/[AEIOUY]/g) ?? []).length;
  if (vowels / letters.length > 0.34) return false;

  return true;
}
