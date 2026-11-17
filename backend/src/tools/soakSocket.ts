import type WebSocket from 'ws';
import type { RawData } from 'ws';

export type SoakSocketScope = { network: 'ukmesh'; observer: string | null; ackRows: number };

/** Full-stack ACK delivery uses the existing observer subscription, never a subset of ACKs. */
export function soakSocketScope(headers: readonly string[], rows: readonly (readonly string[])[],
  count: number, fullStack: boolean): SoakSocketScope {
  if (!fullStack) return {network:'ukmesh',observer:null,ackRows:count};
  if (!Number.isSafeInteger(count) || count < 1 || rows.length !== count) throw new Error('ACK socket scope requires every requested row');
  const rx=headers.indexOf('rx_node_id'),network=headers.indexOf('network');
  if (rx < 0 || network < 0 || headers.lastIndexOf('rx_node_id') !== rx || headers.lastIndexOf('network') !== network) {
    throw new Error('ACK socket scope requires unique receiver and network columns');
  }
  let observer: string | null=null;
  for (const row of rows) {
    const receiver=row[rx];
    if (!receiver || !/^[a-f0-9]{64}$/i.test(receiver)) throw new Error('ACK socket scope requires a valid receiver on every row');
    if (row[network] !== 'ukmesh') throw new Error('ACK socket scope requires ukmesh on every row');
    const normalized=receiver.toLowerCase();
    if (observer !== null && observer !== normalized) throw new Error('ACK socket scope requires the same receiver on every row');
    observer=normalized;
  }
  return {network:'ukmesh',observer,ackRows:rows.length};
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
