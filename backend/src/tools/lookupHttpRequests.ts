import http from 'node:http';
import { LookupResponseVerifier } from './lookupResponseVerifier.js';
import { LOOKUP_RESPONSE_MAX_BYTES } from './lookupResponseBody.js';
import { createLookupAgents, setLookupRequestDeadline } from './lookupHttpTransport.js';

export type LookupHttpCase = { name: string; route: string; expected?: number; populated?: boolean };
export type LookupHttpResult = { ms: number; bytes: number; status: number; digest: string; dataDigest: string;
  entries: number | null; startedAtMonoMs: number; receivedAtMonoMs: number;
  socketAtMonoMs: number | null; connectedAtMonoMs: number | null; actor: string };

/** Own the same bounded HTTP/validation admission path for both benchmark shapes. */
export function createLookupRequestFleet() {
  const agents = createLookupAgents();
  let requestId = 0;
  const verifier = new LookupResponseVerifier();
  function start(port: number, testCase: LookupHttpCase, actorIndex?: number) {
    const started = performance.now();
    const index = actorIndex ?? requestId++ % agents.length;
    if (!Number.isInteger(index) || index < 0 || index >= agents.length) throw new Error('invalid lookup actor');
    const actor = `127.0.1.${index + 1}`;
    const timing = { startedAtMonoMs: started, socketAtMonoMs: null as number | null,
      connectedAtMonoMs: null as number | null, receivedAtMonoMs: null as number | null,
      responseStatus: null as number | null, responseBytes: 0, actor };
    let assignedResolve: ((at: number) => void) | undefined, assignedReject: ((error: Error) => void) | undefined;
    const assigned = actorIndex === undefined ? undefined : new Promise<number>((resolve,reject) => {
      assignedResolve=resolve; assignedReject=reject;
    });
    const done = new Promise<LookupHttpResult>((resolve, reject) => {
      try {
      const req = http.get({ hostname: '127.0.0.1', port, path: testCase.route,
        agent: agents[index], headers: { Accept: 'application/json' } }, res => {
        timing.responseStatus = res.statusCode ?? 0;
        const chunks: Buffer[] = []; let bytes = 0;
        res.on('data', chunk => { bytes += chunk.length; timing.responseBytes=bytes; if (bytes > LOOKUP_RESPONSE_MAX_BYTES) req.destroy(new Error('response budget exceeded')); else chunks.push(chunk); });
        res.on('error', reject);
        res.on('end', () => {
          try {
            if (res.statusCode !== (testCase.expected ?? 200)) {
              timing.receivedAtMonoMs=performance.now(); throw new Error(`${testCase.name}: HTTP ${res.statusCode}`);
            }
            const receivedAt = performance.now();
            timing.receivedAtMonoMs=receivedAt;
            void verifier.verify(Buffer.concat(chunks), { status: res.statusCode ?? 0,
              expectedStatus: testCase.expected ?? 200, populated: testCase.populated ?? false })
              .then(evidence => resolve({ ms: receivedAt - started, bytes, status: res.statusCode ?? 0,
                ...timing, receivedAtMonoMs: receivedAt, ...evidence }), reject);
          } catch (error) { reject(error); }
        });
      });
      req.once('socket', socket => {
        timing.socketAtMonoMs = performance.now();
        assignedResolve?.(timing.socketAtMonoMs);
        if (socket.connecting) socket.once('connect', () => { timing.connectedAtMonoMs = performance.now(); });
        else timing.connectedAtMonoMs = timing.socketAtMonoMs;
      });
      setLookupRequestDeadline(req);
      req.on('error', reject);
      if (assignedReject) req.once('error', assignedReject);
      } catch (error) {
        const failure=error instanceof Error?error:new Error(String(error));
        assignedReject?.(failure);reject(failure);
      }
    });
    return { timing, assigned, done };
  }

  return { start, request: (port: number, definition: LookupHttpCase) => start(port, definition).done,
    clientCount: agents.length, readStatistics: () => verifier.readStatistics(),
    async close() { try { for (const agent of agents) agent.destroy(); } finally { await verifier.close(); } } };
}
