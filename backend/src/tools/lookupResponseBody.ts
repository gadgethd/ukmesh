import { createHash } from 'node:crypto';

export const LOOKUP_RESPONSE_MAX_BYTES = 8 * 1024 * 1024;
export type LookupResponseSpec = { status: number; expectedStatus: number; populated: boolean };
export type LookupResponseEvidence = { entries: number | null; digest: string; dataDigest: string };

/** Keep the same content/hash assertions for both candidate and archived routes. */
export function validateLookupResponse(body: ArrayBuffer, spec: LookupResponseSpec): LookupResponseEvidence {
  if (body.byteLength > LOOKUP_RESPONSE_MAX_BYTES) throw new Error('response byte budget exceeded');
  if (spec.status !== spec.expectedStatus) throw new Error(`lookup response HTTP ${spec.status}`);
  const buffer = Buffer.from(body);
  let value;
  try { value = JSON.parse(buffer.toString()); }
  catch { throw new Error('lookup response was not valid JSON'); }
  const entries = Array.isArray(value) ? value.length : Array.isArray(value?.nodes) ? value.nodes.length : null;
  if (value === null || (spec.populated && entries === 0)) throw new Error('empty fixture response');
  const data = value?.page?.snapshot ? { ...value, page: { ...value.page, snapshot: '[request snapshot]' } } : value;
  return { entries, digest: createHash('sha256').update(buffer).digest('hex'),
    dataDigest: createHash('sha256').update(JSON.stringify(data)).digest('hex') };
}
