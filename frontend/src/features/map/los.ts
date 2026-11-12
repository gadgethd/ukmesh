import type { MeshNode } from '../../types/domain.js';
import { hasCoords } from '../../utils/pathing.js';
import type { LosProfile, NodeLink } from './model.js';

export function nodeLosProfiles(source: MeshNode | undefined, links: NodeLink[],
  nodes: ReadonlyMap<string, MeshNode>, exaggeration: number, amberThreshold: number): LosProfile[] {
  if (!hasCoords(source)) return [];
  const profiles: LosProfile[] = [];
  for (const link of links) {
    const peer = nodes.get(link.peer_id);
    if (!hasCoords(peer)) continue;
    profiles.push({
      peer_id: link.peer_id, peer_name: link.peer_name,
      itm_path_loss_db: link.itm_path_loss_db,
      itm_viable: link.itm_path_loss_db != null && link.itm_path_loss_db <= amberThreshold,
      profile: [[source.lon, source.lat, ((source.elevation_m ?? 0) + 10) * exaggeration],
        [peer.lon, peer.lat, ((peer.elevation_m ?? 0) + 10) * exaggeration]],
    });
  }
  return profiles;
}
