import { parentPort } from 'node:worker_threads';
import type { LookupResponseSpec } from './lookupResponseBody.js';
import { createLookupResponseMemo } from './lookupResponseMemo.js';

if (!parentPort) throw new Error('lookup response worker requires its owning benchmark');
const memo = createLookupResponseMemo();
parentPort.on('message', ({ id, body, spec, command }: {
  id: number; body: ArrayBuffer; spec: LookupResponseSpec; command?: 'statistics';
}) => {
  try {
    if (command === 'statistics') parentPort!.postMessage({ id, statistics: memo.statistics() });
    else parentPort!.postMessage({ id, result: memo.validate(body, spec) });
  }
  catch (error) { parentPort!.postMessage({ id, error: error instanceof Error ? error.message : 'lookup response validation failed' }); }
});
