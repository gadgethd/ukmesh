import { useEffect, useState } from 'react';
import { getCurrentSite } from '../config/site.js';
import { useRuntimeFeatures } from '../config/runtimeFeatures.js';
import { fetchJson, withScopeParams } from '../utils/api.js';
import type { MeshNode } from './useNodes.js';

/** Resolve former names without adding history fields to the public node DTO. */
export function useNodeNameSearch(query: string): MeshNode[] {
  const site = getCurrentSite();
  const network = site.networkFilter ?? site.network;
  const observer = site.observerId;
  const { privacyGeneration } = useRuntimeFeatures();
  const q = query.trim();
  const key = JSON.stringify([q, network, observer, privacyGeneration]);
  const [result, setResult] = useState<{ key: string; nodes: MeshNode[] } | null>(null);

  useEffect(() => {
    if (!q || q.length > 160) return;
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      void fetchJson<{ nodes: MeshNode[] }>(
        withScopeParams(`/api/nodes/map?q=${encodeURIComponent(q)}&limit=10`, { network, observer }),
        { signal: controller.signal, cache: 'no-store' },
      ).then((page) => {
        if (!controller.signal.aborted) setResult({
          key, nodes: page.nodes.map(node => ({ ...node, public_key: node.node_id })),
        });
      }).catch(() => {
        if (!controller.signal.aborted) setResult({ key, nodes: [] });
      });
    }, 200);
    return () => { window.clearTimeout(timer); controller.abort(); };
  }, [key, q, network, observer]);

  // A previous query/scope must never flash while the next request is pending.
  return result?.key === key ? result.nodes : [];
}
