import { POLLING } from '../../config/polling.js';

/**
 * Ask the calculator for one repeater's RF footprint. The request is
 * asynchronous: the server enqueues it, writes a `computing` entry into
 * meta.json, and the existing poll in useRfCoverage picks up the finished
 * tiles. Returns true when the request was accepted or was already queued.
 */
export async function requestRfNodeCoverage(publicKey: string): Promise<boolean> {
  if (!/^[0-9a-f]{64}$/i.test(publicKey)) return false;
  const response = await fetch(`/rf-coverage/nodes/${publicKey.toLowerCase()}/request`, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(POLLING.ordinaryTimeout),
  });
  await response.body?.cancel();
  return response.ok || response.status === 409;
}
