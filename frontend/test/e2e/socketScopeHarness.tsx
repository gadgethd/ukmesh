import { useLayoutEffect, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { flushSync } from 'react-dom';
import { useWebSocket } from '../../src/hooks/useWebSocket.js';

const NativeWebSocket = window.WebSocket;
const sockets: FixtureSocket[] = [];
let root: Root | undefined;
let container: HTMLDivElement | undefined;
let select: (scope: string) => void = () => { throw new Error('Harness is not mounted'); };
let closedAtBoundary = false;
class FixtureSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  readyState = FixtureSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor(readonly url: string, protocols?: string | string[]) {
    if (!new URL(url).searchParams.get('network')?.startsWith('socket-fixture-')) {
      return new NativeWebSocket(url, protocols) as unknown as FixtureSocket;
    }
    sockets.push(this);
  }
  open() { this.readyState = FixtureSocket.OPEN; this.onopen?.(); }
  emit(data: string) { this.onmessage?.(new MessageEvent('message', { data: JSON.stringify({ type: 'packet', data, ts: 1 }) })); }
  close() { this.readyState = FixtureSocket.CLOSED; this.onclose?.(); }
}
function Probe() {
  const [scope, setScope] = useState('A');
  const [messages, setMessages] = useState<string[]>([]);
  const connection = useWebSocket(message => setMessages(previous => [...previous, `${scope}:${message.scopeEpoch}:${message.data}`]),
    { network: `socket-fixture-${scope}` }, scope === 'A' ? 1 : 2);
  useLayoutEffect(() => { select = setScope; }, []);
  useLayoutEffect(() => {
    if (scope !== 'B') return;
    // Run before passive effects: native events can still target the previous socket here.
    closedAtBoundary = sockets[0].readyState === FixtureSocket.CLOSED;
    sockets[0].emit('late-A');
    sockets[0].onopen?.();
    sockets[0].onclose?.();
    sockets[0].onerror?.();
  }, [scope]);
  return <section aria-label="Socket scope commit fixture">
    <output data-testid="fixture-socket-messages">{messages.join(',')}</output>
    <output data-testid="fixture-socket-status">{connection.readyState}</output>
  </section>;
}
export function mount() {
  sockets.length = 0;
  closedAtBoundary = false;
  window.WebSocket = FixtureSocket as unknown as typeof WebSocket;
  container = document.createElement('div');
  container.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:white;color:black;overflow:auto';
  document.body.append(container);
  root = createRoot(container);
  flushSync(() => root!.render(<Probe />));
}
export function emitCurrent(data: string) { const socket = sockets.at(-1)!; socket.open(); socket.emit(data); }
export function commitB() { flushSync(() => select('B')); return closedAtBoundary; }
export function snapshot() { return sockets.map(socket => ({ url: socket.url, readyState: socket.readyState })); }
export function unmount() {
  root?.unmount(); container?.remove(); root = undefined; container = undefined;
  window.WebSocket = NativeWebSocket;
}
