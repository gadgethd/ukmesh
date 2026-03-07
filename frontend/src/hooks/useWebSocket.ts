import { useEffect, useRef, useCallback, useState } from 'react';
import type { ApiScope } from '../utils/api.js';

export type WSReadyState = 'connecting' | 'connected' | 'disconnected';

export interface WSMessage {
  type: 'packet' | 'node_update' | 'node_upsert' | 'initial_state' | 'coverage_update' | 'link_update';
  data: unknown;
  ts: number;
}

type MessageHandler = (msg: WSMessage) => void;

export function useWebSocket(onMessage: MessageHandler, scope: ApiScope = {}) {
  const [readyState, setReadyState] = useState<WSReadyState>('connecting');
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
      setReadyState('connected');
      retryDelayRef.current = 3000;
      console.log('[ws] connected');
    };

    ws.onmessage = (e: MessageEvent<string>) => {
      try {
        const msg = JSON.parse(e.data) as WSMessage;
        handlerRef.current(msg);
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      if (!shouldReconnectRef.current) return;
      setReadyState('disconnected');
      const delay = retryDelayRef.current;
      retryDelayRef.current = Math.min(15000, retryDelayRef.current * 1.5);
      console.log(`[ws] disconnected — reconnecting in ${Math.round(delay / 1000)}s`);
      timerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [scope.network, scope.observer]);

  useEffect(() => {
    shouldReconnectRef.current = true;
    retryDelayRef.current = 3000;
    connect();
    return () => {
      shouldReconnectRef.current = false;
      if (timerRef.current) clearTimeout(timerRef.current);
      wsRef.current?.close();
    };
  }, [connect]);

  return readyState;
}
