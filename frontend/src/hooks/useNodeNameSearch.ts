import { useEffect, useState } from 'react';
import { fetchJson, withScopeParams } from '../utils/api.js';
import { scopeIsReady, scopeKey, type PublicScope } from '../utils/scope.js';
import type { MeshNode } from '../types/domain.js';

const emptyNodes: MeshNode[] = [];

/** Resolve former names without adding history fields to the public node DTO. */
export function useNodeNameSearch(query: string, scope: PublicScope): MeshNode[] {
  return useNodeLookup(query, scope).nodes;
}

export function useNodeLookup(query: string, scope: PublicScope, attempt = 0) {
  const { network, observer, region } = scope;
  const q = query.trim();
  const key = JSON.stringify([q, scopeKey(scope), attempt]);
  const enabled = scopeIsReady(scope) && Boolean(q) && q.length <= 160;
  const [result, setResult] = useState<{ key: string; nodes: MeshNode[]; status: 'ready' | 'error' } | null>(null);

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      void fetchJson<{ nodes: MeshNode[] }>(
        withScopeParams(`/api/nodes/map?q=${encodeURIComponent(q)}&limit=10`, { network, observer, region }),
        { signal: controller.signal, cache: 'no-store' },
        { timeoutMs: 8_000, maxBytes: 256 * 1024, validate: (value): value is { nodes: MeshNode[] } =>
          Boolean(value && typeof value === 'object' && 'nodes' in value && Array.isArray(value.nodes)
            && value.nodes.length <= 10 && value.nodes.every(node => node && typeof node.node_id === 'string')) },
      ).then((page) => {
        if (!controller.signal.aborted) setResult({
          key, nodes: page.nodes.map(node => ({ ...node, public_key: node.node_id })),
          status: 'ready',
        });
      }).catch(() => {
        if (!controller.signal.aborted) setResult({ key, nodes: [], status: 'error' });
      });
    }, 200);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [enabled, key, q, network, observer, region]);

  // A previous query/scope must never flash while the next request is pending.
  if (result?.key === key && enabled) return result;
  return { nodes: emptyNodes, status: !q ? 'idle' : q.length > 160 ? 'invalid' : enabled ? 'loading' : 'error' } as const;
}
