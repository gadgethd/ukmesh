import type { WSReadyState, WSConnectionState, WSMessage } from '../features/realtime/protocol.js';
import { useEffect, useLayoutEffect, useMemo, useState } from 'react';
import type { ApiScope } from '../utils/api.js';
import { createMessageTimestampPublisher } from '../utils/messageTimestamp.js';
import { POLLING } from '../config/polling.js';
import { useCommittedRef } from './useCommittedRef.js';

type MessageHandler = (msg: WSMessage) => void;
type ConnectionOwner = {
  active: boolean;
  socket: WebSocket | null;
  timer: ReturnType<typeof setTimeout> | null;
  timestamps: ReturnType<typeof createMessageTimestampPublisher> | null;
  retryDelay: number;
  scope: ApiScope;
  scopeEpoch: number;
  enabled: boolean;
};

function stopConnection(owner: ConnectionOwner) {
  if (owner.timer !== null) clearTimeout(owner.timer);
  owner.timer = null;
  owner.timestamps?.stop();
  owner.timestamps = null;
  const socket = owner.socket;
  owner.socket = null;
  socket?.close();
}

export function useWebSocket(onMessage: MessageHandler, scope: ApiScope = {}, scopeEpoch = 0, enabled = true) {
  const [readyState, setReadyState] = useState<WSReadyState>('connecting');
  const [lastMessageAt, setLastMessageAt] = useState<number | null>(null);
  const handlerRef = useCommittedRef(onMessage);
  const owner = useMemo<ConnectionOwner>(() => ({
    active: false,
    socket: null,
    timer: null,
    timestamps: null,
    retryDelay: 3000,
    scope: { network: scope.network, observer: scope.observer, region: scope.region },
    scopeEpoch,
    enabled,
  }), [scope.network, scope.observer, scope.region, scopeEpoch, enabled]);

  useLayoutEffect(() => {
    owner.active = true;
    return () => {
      owner.active = false;
      stopConnection(owner);
    };
  }, [owner]);

  useEffect(() => {
    setLastMessageAt(null);
    function connect() {
      if (!owner.enabled || !owner.active) return;
      if (owner.timer !== null) clearTimeout(owner.timer);
      owner.timer = null;
      if (owner.socket?.readyState === WebSocket.OPEN || owner.socket?.readyState === WebSocket.CONNECTING) return;

      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const params = new URLSearchParams();
      if (owner.scope.network) params.set('network', owner.scope.network);
      if (owner.scope.observer) params.set('observer', owner.scope.observer);
      if (owner.scope.region) params.set('region', owner.scope.region);
      const query = params.toString();
      const ws = new WebSocket(`${protocol}//${window.location.host}/ws${query ? `?${query}` : ''}`);
      const isCurrent = () => owner.active && owner.socket === ws;
      owner.timestamps?.stop();
      const timestamps = createMessageTimestampPublisher(timestamp => {
        if (isCurrent()) setLastMessageAt(timestamp);
      }, { intervalMs: POLLING.socketStatus });
      owner.timestamps = timestamps;
      owner.socket = ws;
      setReadyState('connecting');

      ws.onopen = () => {
        if (!isCurrent()) return;
        setReadyState('connected');
        owner.retryDelay = 3000;
        console.log('[ws] connected');
      };
      ws.onmessage = (event: MessageEvent<string>) => {
        if (!isCurrent()) return;
        try {
          const raw = String(event.data ?? '');
          const lines = raw.includes('\n') ? raw.split('\n').filter(Boolean) : [raw];
          for (const line of lines) {
            if (!isCurrent()) return;
            const msg = { ...(JSON.parse(line) as WSMessage), scopeEpoch: owner.scopeEpoch };
            timestamps.record(Date.now());
            handlerRef.current(msg);
          }
        } catch {
          // Ignore malformed messages.
        }
      };
      ws.onclose = () => {
        if (!isCurrent()) return;
        timestamps.flush();
        timestamps.stop();
        owner.timestamps = null;
        owner.socket = null;
        setReadyState('disconnected');
        const jitter = Math.random() * 1000;
        const delay = owner.retryDelay + jitter;
        owner.retryDelay = Math.min(15000, owner.retryDelay * 1.5);
        console.log(`[ws] disconnected — reconnecting in ${(delay / 1000).toFixed(1)}s (${Math.round(jitter)}ms jitter)`);
        owner.timer = setTimeout(connect, delay);
      };
      ws.onerror = () => { if (isCurrent()) ws.close(); };
    }
    connect();
  }, [owner]);

  return { readyState, lastMessageAt } satisfies WSConnectionState;
}
