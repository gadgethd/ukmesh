import type WebSocket from 'ws';
import type { RawData } from 'ws';

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
    const closed = () => finish(new Error('WebSocket closed before its initial state'));
    const timer = setTimeout(() => finish(new Error('WebSocket initial state deadline exceeded')), timeoutMs);
    socket.on('message', message); socket.on('error', failed); socket.on('close', closed);
  });
}
