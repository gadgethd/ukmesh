import React, { useMemo } from 'react';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import type { MeshNode } from '../../hooks/useNodes.js';
import { PathMap, type LazyPath, type LazyPathNode, type LazyPathResult } from './PacketDetailPanel.js';
import type { PathTreeStatus } from './feedState.js';
import type { FeedPacket, PathTreeBranchNode } from './feedModel.js';

export const FeedMapPanel: React.FC<{
  packet: FeedPacket | null;
  nodeMap: Map<string, MeshNode>;
  cachedLazyPath: LazyPathResult | null;
  isLoading?: boolean;
}> = ({ packet, nodeMap, cachedLazyPath, isLoading = false }) => {
  const observerPositions = useMemo((): [number, number][] => {
    if (!packet) return [];
    const candidates = packet.observer_node_ids?.length
      ? packet.observer_node_ids
      : (packet.rx_node_id ? [packet.rx_node_id] : []);
    const positions: [number, number][] = [];
    const seen = new Set<string>();
    for (const id of candidates) {
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const node = nodeMap.get(id);
      if (node?.lat != null && node?.lon != null) positions.push([node.lat, node.lon]);
    }
    return positions;
  }, [
    packet,
    nodeMap,
  ]);

  if (!packet) {
    return <div className="uk-feed-map-placeholder">Select a packet to see its path</div>;
  }
  return (
    <PathMap
      results={[]}
      observerPositions={observerPositions}
      lazyPaths={(cachedLazyPath?.paths ?? []) as LazyPath[]}
      nodeMap={nodeMap}
      isLoading={isLoading}
    />
  );
};

function lazyPathNodeKey(node: LazyPathNode): string {
  const identity = node.nodeId ?? node.hash;
  return `${node.position}:${identity}:${node.isObserver ? 'observer' : 'hop'}`;
}

function buildLazyPathTree(paths: LazyPath[]): PathTreeBranchNode[] {
  const roots: PathTreeBranchNode[] = [];
  paths.forEach((path, branchIndex) => {
    let siblings = roots;
    for (const step of path.canonicalPath) {
      const treeKey = lazyPathNodeKey(step);
      let node = siblings.find((candidate) => candidate.treeKey === treeKey);
      if (!node) {
        node = {
          ...step,
          treeKey,
          branchIndexes: new Set<number>(),
          children: [],
        };
        siblings.push(node);
      }
      node.branchIndexes.add(branchIndex);
      siblings = node.children;
    }
  });
  return roots;
}

const PathTreeNodeView: React.FC<{
  node: PathTreeBranchNode;
  nodeMap: Map<string, MeshNode>;
  totalBranches: number;
}> = ({ node, nodeMap, totalBranches }) => {
  const mapNode = node.nodeId ? nodeMap.get(node.nodeId) : undefined;
  const iata = mapNode?.iata?.trim().toUpperCase() ?? null;
  const branchIndexes = Array.from(node.branchIndexes).sort((a, b) => a - b);
  const branchLabel = totalBranches > 1
    ? branchIndexes.length === totalBranches
      ? 'all branches'
      : `branch ${branchIndexes.map((index) => index + 1).join(', ')}`
    : null;
  const kind = node.isObserver
    ? 'observer'
    : node.ambiguous
      ? 'ambiguous'
      : node.nodeId
        ? 'matched'
        : 'unmatched';
  const nodeLabel = node.isObserver
    ? (node.name ?? 'Observer')
    : node.name
      ?? mapNode?.name
      ?? (node.nodeId ? node.nodeId.slice(0, 10) : null)
      ?? 'Unknown hop';
  const seenLabel = !node.isObserver && node.totalObservations > 0
    ? `${node.appearances}/${node.totalObservations} seen`
    : null;
  const meta = [
    kind === 'ambiguous' ? 'ambiguous' : null,
    node.isObserver ? 'observer' : kind === 'unmatched' ? `hop ${node.hash}` : null,
    node.isObserver ? node.hash.slice(0, 8) : iata,
    seenLabel,
  ].filter((value): value is string => Boolean(value));
  const tooltip = node.nodeId ?? (node.isObserver ? node.hash : null);

  return (
    <li className="uk-feed-path-tree__node">
      <div className="uk-feed-path-tree__step">
        <span
          className={`uk-feed-path-tree__dot uk-feed-path-tree__dot--${kind}`}
          title={tooltip ?? undefined}
        >
          {node.isObserver ? 'RX' : node.position + 1}
        </span>
        <div className="uk-feed-path-tree__body">
          <div className="uk-feed-path-tree__title-row">
            <span className="uk-feed-path-tree__name" title={tooltip ?? undefined}>{nodeLabel}</span>
            {branchLabel && <span className="uk-feed-path-tree__branch-label">{branchLabel}</span>}
          </div>
          <div className="uk-feed-path-tree__meta">
            {meta.map((item, index) => <span key={`${item}-${index}`}>{item}</span>)}
          </div>
        </div>
      </div>
      {node.children.length > 0 && (
        <ol className="uk-feed-path-tree__children">
          {node.children.map((child) => (
            <PathTreeNodeView
              key={child.treeKey}
              node={child}
              nodeMap={nodeMap}
              totalBranches={totalBranches}
            />
          ))}
        </ol>
      )}
    </li>
  );
};

export const PacketPathTree: React.FC<{
  lazyPath: LazyPathResult | null;
  nodeMap: Map<string, MeshNode>;
  status: PathTreeStatus;
  onRetry: () => void;
}> = ({ lazyPath, nodeMap, status, onRetry }) => {
  const paths = useMemo(
    () => lazyPath?.paths.filter((path) => path.canonicalPath.length > 0) ?? [],
    [lazyPath],
  );
  const tree = useMemo(() => buildLazyPathTree(paths), [paths]);
  const matchedHops = paths.reduce((sum, path) => sum + path.matchedHops, 0);
  const totalHops = paths.reduce((sum, path) => sum + path.totalHops, 0);
  const ambiguousCount = useMemo(() => {
    const seen = new Set<string>();
    for (const path of paths) {
      for (const step of path.canonicalPath) {
        if (step.ambiguous) seen.add(lazyPathNodeKey(step));
      }
    }
    return seen.size;
  }, [paths]);

  if (status === 'loading' && !lazyPath) {
    return (
      <div className="uk-feed-path-tree uk-feed-path-tree--message">
        <LoadingIndicator label="Resolving predicted repeaters..." variant="inline" />
      </div>
    );
  }
  if (status === 'settling' && !lazyPath) {
    return (
      <div className="uk-feed-path-tree uk-feed-path-tree--message">
        <LoadingIndicator label="Waiting briefly for all observers..." variant="inline" />
      </div>
    );
  }
  if (status === 'unavailable' && !lazyPath) {
    return (
      <div className="uk-feed-path-tree uk-feed-path-tree--message">
        <span className="uk-feed-path-tree__status">No trace hashes were captured for this packet.</span>
      </div>
    );
  }
  if (status === 'error' && !lazyPath) {
    return (
      <div className="uk-feed-path-tree uk-feed-path-tree--message">
        <span className="uk-feed-path-tree__status">Route lookup failed.</span>
        <button className="uk-feed-path-tree__retry" onClick={onRetry}>Try again</button>
      </div>
    );
  }
  if (status === 'idle' && !lazyPath) {
    return (
      <div className="uk-feed-path-tree uk-feed-path-tree--message">
        <span className="uk-feed-path-tree__status">Select “Try again” to resolve this packet path.</span>
        <button className="uk-feed-path-tree__retry" onClick={onRetry}>Resolve path</button>
      </div>
    );
  }
  if (!lazyPath || tree.length === 0) {
    return (
      <div className="uk-feed-path-tree uk-feed-path-tree--message">
        <span className="uk-feed-path-tree__status">No predicted repeater path is available yet.</span>
      </div>
    );
  }

  return (
    <div className="uk-feed-path-tree">
      <div className="uk-feed-path-tree__header">
        <span>Predicted repeaters</span>
        {totalHops > 0 && <span>{totalHops} hops</span>}
        {totalHops > 0 && <span>{matchedHops} matched</span>}
        {ambiguousCount > 0 && <span>{ambiguousCount} ambiguous</span>}
        {paths.length > 1 && <span>{paths.length} branches</span>}
      </div>
      <ol className="uk-feed-path-tree__list">
        {tree.map((node) => (
          <PathTreeNodeView
            key={node.treeKey}
            node={node}
            nodeMap={nodeMap}
            totalBranches={paths.length}
          />
        ))}
      </ol>
    </div>
  );
};
