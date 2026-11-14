import { parentPort } from 'node:worker_threads';
import { validateLookupResponse } from './lookupResponseBody.js';
import type { LookupResponseSpec } from './lookupResponseBody.js';

if (!parentPort) throw new Error('lookup response worker requires its owning benchmark');
parentPort.on('message', ({ id, body, spec }: { id: number; body: ArrayBuffer; spec: LookupResponseSpec }) => {
  try { parentPort!.postMessage({ id, result: validateLookupResponse(body, spec) }); }
  catch (error) { parentPort!.postMessage({ id, error: error instanceof Error ? error.message : 'lookup response validation failed' }); }
});
