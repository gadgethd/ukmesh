import { useEffect, useRef, useCallback, useState } from 'react';
import type { ApiScope } from '../utils/api.js';

export type WSReadyState = 'connecting' | 'connected' | 'disconnected';
export type WSConnectionState = {
  readyState: WSReadyState;
  lastMessageAt: number | null;
};

export interface WSMessage {
  type: 'packet' | 'node_update' | 'node_upsert' | 'initial_state' | 'coverage_update' | 'link_update';
  data: unknown;
  ts: number;
  scopeEpoch?: number;
}

type MessageHandler = (msg: WSMessage) => void;

export function useWebSocket(onMessage: MessageHandler, scope: ApiScope = {}, scopeEpoch = 0) {
  const [readyState, setReadyState] = useState<WSReadyState>('connecting');
  const [lastMessageAt, setLastMessageAt] = useState<number | null>(null);
  const wsRef   = useRef<WebSocket | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const shouldReconnectRef = useRef(true);
  const retryDelayRef = useRef(3000);
  const handlerRef = useRef(onMessage);
  handlerRef.current = onMessage;

  const connect = useCallback(() => {
    if (!shouldReconnectRef.current) return;

    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    if (wsRef.current?.readyState === WebSocket.OPEN || wsRef.current?.readyState === WebSocket.CONNECTING) return;

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const params = new URLSearchParams();
    if (scope.network) params.set('network', scope.network);
    if (scope.observer) params.set('observer', scope.observer);
    const query = params.toString();
    const suffix = query ? `?${query}` : '';
    const url = `${protocol}//${window.location.host}/ws${suffix}`;
    const ws = new WebSocket(url);
    wsRef.current = ws;
    setReadyState('connecting');

    ws.onopen = () => {
      if (wsRef.current !== ws) return;
      setReadyState('connected');
      retryDelayRef.current = 3000;
      console.log('[ws] connected');
    };

    ws.onmessage = (e: MessageEvent<string>) => {
      if (wsRef.current !== ws) return;
      try {
        const raw = String(e.data ?? '');
        const lines = raw.includes('\n') ? raw.split('\n').filter(Boolean) : [raw];
        for (const line of lines) {
          const msg = {
            ...(JSON.parse(line) as WSMessage),
            scopeEpoch,
          };
          setLastMessageAt(Date.now());
          handlerRef.current(msg);
        }
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      if (wsRef.current !== ws || !shouldReconnectRef.current) return;
      wsRef.current = null;
      setReadyState('disconnected');
      const baseDelay = retryDelayRef.current;
      const jitter = Math.random() * 1000;
      const delay = baseDelay + jitter;
      retryDelayRef.current = Math.min(15000, retryDelayRef.current * 1.5);
      console.log(`[ws] disconnected — reconnecting in ${(delay / 1000).toFixed(1)}s (${Math.round(jitter)}ms jitter)`);
      timerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [scope.network, scope.observer, scopeEpoch]);

  useEffect(() => {
    shouldReconnectRef.current = true;
    retryDelayRef.current = 3000;
    setLastMessageAt(null);
    connect();
    return () => {
      shouldReconnectRef.current = false;
      if (timerRef.current) clearTimeout(timerRef.current);
      const ws = wsRef.current;
      wsRef.current = null;
      ws?.close();
    };
  }, [connect]);

  return { readyState, lastMessageAt } satisfies WSConnectionState;
}
