import assert from 'node:assert/strict';
import test from 'node:test';
import { nodeLosProfiles } from './los.js';
import type { MeshNode } from '../../types/domain.js';
import type { NodeLink } from './model.js';

test('LOS keeps valid peers, physical exaggerated antenna heights and the path-loss boundary', () => {
  const source = { node_id: 'source', lat: 54, lon: -1, elevation_m: 20 } as MeshNode;
  const peer = { node_id: 'peer', lat: 55, lon: -2, elevation_m: 30 } as MeshNode;
  const links = [{ peer_id: 'peer', peer_name: 'Peer', itm_path_loss_db: 129.5 },
    { peer_id: 'missing', itm_path_loss_db: 100 }] as NodeLink[];
  const nodes = new Map([['peer', peer]]);
  const result = nodeLosProfiles(source, links, nodes, 2, 129.5);
  assert.equal(result.length, 1);
  assert.equal(result[0]?.itm_viable, true);
  assert.deepEqual(result[0]?.profile, [[-1, 54, 60], [-2, 55, 80]]);
  assert.equal(nodeLosProfiles(source, [{ ...links[0]!, itm_path_loss_db: 129.6 }], nodes, 2, 129.5)[0]?.itm_viable, false);
  assert.deepEqual(nodeLosProfiles(undefined, links, nodes, 2, 129.5), []);
});
