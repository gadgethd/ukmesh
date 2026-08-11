import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildAerialPathSegments,
  registerAerialPaths,
  type PathRegistryEntry,
} from './AnimatedPathOverlay.js';
import { buildResolvedAerialPaths } from './LiveOverlayController.js';
import {
  aggregateCanonicalPath,
  type MultiObserverBetaResponse,
} from '../../hooks/packetPathOverlayUtils.js';

function response(packetHash: string, observerLon: number): MultiObserverBetaResponse {
  return {
    packetHash,
    network: 'uk',
    canonicalPath: [],
    observers: [{ observerId: `rx-${observerLon}` }],
    confidence: 0.8,
    results: [{
      ok: true,
      packetHash,
      network: 'uk',
      mode: 'resolved',
      canonicalPath: [],
      observers: [{ observerId: `rx-${observerLon}` }],
      confidence: 0.8,
      purplePath: [[51, -2], [52, -1], [53, observerLon]],
    }],
  };
}

function pathsFor(dto: MultiObserverBetaResponse) {
  const prediction = aggregateCanonicalPath(dto)!;
  return buildResolvedAerialPaths(prediction.packetHash, prediction.routes, new Map());
}

test('a new unrelated packet cannot rescope and restart the previous packet mid-animation', () => {
  const registry = new Map<string, PathRegistryEntry>();
  const packetAPaths = pathsFor(response('packet-a', 0));
  const packetAKeys = buildAerialPathSegments(packetAPaths).map((segment) => segment.id);

  registerAerialPaths(registry, packetAPaths, 100);
  // Packet B is now active, but its response has not arrived. React can still
  // render the last committed A prediction once; it must remain scoped to A.
  registerAerialPaths(registry, packetAPaths, 250);
  const packetBPaths = pathsFor(response('packet-b', 1));
  registerAerialPaths(registry, packetBPaths, 250);

  assert.equal(registry.get(packetAKeys[0]!)?.startedAt, 100);
  assert.equal(registry.get(packetAKeys[1]!)?.startedAt, 500);
  assert.equal(registry.size, 4, 'packet A and B own two independent directed segments each');
  assert(packetBPaths.every((path) => path.id.includes('PACKET-B')));
});

test('a same-packet observer update keeps the trunk clock and diverts at the split', () => {
  const registry = new Map<string, PathRegistryEntry>();
  const initialDto = response('packet-a', 0);
  const initialPaths = pathsFor(initialDto);
  registerAerialPaths(registry, initialPaths, 100);

  const updatedDto: MultiObserverBetaResponse = {
    ...initialDto,
    observers: [...initialDto.observers, { observerId: 'rx-1' }],
    results: [...initialDto.results!, ...response('packet-a', 1).results!],
  };
  const updatedPaths = pathsFor(updatedDto);
  const [trunkKey, originalBranchKey] = buildAerialPathSegments(initialPaths).map((segment) => segment.id);
  const newBranchKey = buildAerialPathSegments([updatedPaths[1]!])[1]!.id;
  registerAerialPaths(registry, updatedPaths, 300);

  assert.equal(registry.get(trunkKey!)?.startedAt, 100);
  assert.equal(registry.get(originalBranchKey!)?.startedAt, 500);
  assert.equal(registry.get(newBranchKey)?.startedAt, 500);
});
