import { Suspense, startTransition, useLayoutEffect, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { flushSync } from 'react-dom';
import { useOwnerSharing } from '../../src/features/owner/useOwnerSharing.js';

const first = 'A'.repeat(64), second = 'B'.repeat(64);
const suspended = new Promise<void>(() => {});
let root: Root | undefined;
let container: HTMLDivElement | undefined;
let select: (id: string) => void = () => { throw new Error('Harness is not mounted'); };
let observedB = false;

function Probe({ nodeId }: { nodeId: string }) {
  const resource = useOwnerSharing({ hasDashboard: true, selectedNodeId: nodeId,
    ownerSessionKey: nodeId === first ? 'fixture-owner' : 'uncommitted-owner',
    ownerSessionGeneration: 1, privacyGeneration: 1, clearOwnerSession: () => {} });
  if (nodeId === second) { observedB = true; throw suspended; }
  return <section aria-label="Owner scope concurrency fixture">
    <output data-testid="fixture-owner">{resource.packetSharing?.nodeId ?? 'loading'}</output>
    <output data-testid="fixture-destinations">{resource.packetSharingDestinationIds.join(',')}</output>
    <button onClick={() => resource.changePacketSharingDestination('relaylens', true)}>Enable fixture sharing</button>
  </section>;
}
function Harness() {
  const [nodeId, setNodeId] = useState(first);
  useLayoutEffect(() => { select = setNodeId; }, []);
  return <Suspense fallback={<p>Fixture suspended</p>}><Probe nodeId={nodeId} /></Suspense>;
}
export function mount() {
  observedB = false;
  container = document.createElement('div');
  container.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:white;color:black;overflow:auto';
  document.body.append(container);
  root = createRoot(container);
  flushSync(() => root!.render(<Harness />));
}
export function transitionToB() { startTransition(() => select(second)); }
export function bWasRendered() { return observedB; }
export function discardB() { flushSync(() => select(first)); }
export function unmount() { root?.unmount(); container?.remove(); root = undefined; container = undefined; }
