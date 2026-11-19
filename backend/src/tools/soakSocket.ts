import type WebSocket from 'ws';
import type { RawData } from 'ws';
import path from 'node:path';
import { pairedReplayPackets } from './replaySampleMqtt.js';
import { readGzipCsv, rowObject } from './sampleData.js';

export type SoakSocketScope = { network: 'ukmesh'; observer: string | null; ackRows: number };

function soakObserver(receiver: string | undefined, network: string | undefined, observer: string | null): string {
  if (!receiver || !/^[a-f0-9]{64}$/i.test(receiver)) throw new Error('ACK socket scope requires a valid receiver on every row');
  if (network !== 'ukmesh') throw new Error('ACK socket scope requires ukmesh on every row');
  const normalized = receiver.toLowerCase();
  if (observer !== null && observer !== normalized) throw new Error('ACK socket scope requires the same receiver on every row');
  return normalized;
}

/** Validate the whole ACK cohort before publication and retain only its hashes and scope. */
export async function readReplayInput(directory: string, count: number, paired: boolean) {
  if (!Number.isSafeInteger(count) || count < 1) throw new Error('ACK socket scope requires a positive row count');
  const hashes: string[] = [];
  let observer: string | null = null;
  if (paired) {
    for await (const { row } of pairedReplayPackets(directory, count)) {
      hashes.push(row['packet_hash']!);
      observer = soakObserver(row['rx_node_id'], row['network'], observer);
    }
  } else {
    const table = await readGzipCsv(path.join(directory, 'packets.csv.gz'));
    for (const row of table.rows) hashes.push(rowObject(table.headers, row)['packet_hash']!);
  }
  const expected = new Set(hashes);
  if (hashes.length !== count || expected.size !== count) throw new Error('fixture must contain exactly the requested distinct hashes');
  const socketScope: SoakSocketScope = { network: 'ukmesh', observer, ackRows: count };
  return { hashes, expected, socketScope };
}

export function soakSocketUrl(base: URL, scope: SoakSocketScope): URL {
  const socket=new URL('/ws?network='+scope.network,base);socket.protocol='ws:';
  if (scope.observer !== null) socket.searchParams.set('observer',scope.observer);
  return socket;
}

export function socketCloseDetails(code: unknown, reason: unknown) {
  const bytes=Buffer.isBuffer(reason)?reason:typeof reason==='string'?Buffer.from(reason):Buffer.alloc(0);
  return {code:typeof code==='number'&&Number.isSafeInteger(code)?code:null,reason:bytes.subarray(0,256).toString('utf8')};
}

export function socketFailureDetails(error: unknown) {
  if (!error || typeof error!=='object' || !('webSocketClose' in error)) return {};
  const value=error.webSocketClose;
  if (!value || typeof value!=='object' || !('code' in value) || !('reason' in value)) return {};
  return {webSocketClose:socketCloseDetails(value.code,value.reason)};
}

/** Subscription authorization and the initial snapshot finish after WS open. */
export function waitForInitialState(socket: Pick<WebSocket, 'on' | 'off'>, timeoutMs = 15000): Promise<void> {
  return new Promise((resolve, reject) => {
    const finish = (error?: Error) => {
      clearTimeout(timer);
      socket.off('message', message); socket.off('error', failed); socket.off('close', closed);
      if (error) reject(error); else resolve();
    };
    const message = (bytes: RawData) => {
      try {
        if (bytes.toString().split('\n').filter(Boolean).some(line => JSON.parse(line).type === 'initial_state')) finish();
      } catch { finish(new Error('WebSocket initial state was not valid JSON')); }
    };
    const failed = (error: Error) => finish(error);
    const closed = (code: number, reason: Buffer) => {
      const detail=socketCloseDetails(code,reason);
      finish(Object.assign(new Error(`WebSocket closed before its initial state (code ${detail.code ?? 'unknown'}: ${detail.reason})`),
        {webSocketClose:detail}));
    };
    const timer = setTimeout(() => finish(new Error('WebSocket initial state deadline exceeded')), timeoutMs);
    socket.on('message', message); socket.on('error', failed); socket.on('close', closed);
  });
}
