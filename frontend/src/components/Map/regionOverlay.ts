import {
  confidenceFor,
  regionColor,
  regionRing,
  unattributedRing,
  type RegionRepeater,
  type RegionSummary,
  type UnattributedCluster,
} from '../../pages/regionsModel.js';

type RegionFeatureProperties = {
  [key: string]: string | number | boolean | null;
};

export type RegionRepeaterFilters = {
  roleFilter: 'repeaters' | 'all';
  statusFilter: 'all' | 'green' | 'red';
  regionFilter: string | null;
  memberIds: ReadonlySet<string> | null;
  selectedNodeId: string | null;
};

function hasCoordinates(repeater: RegionRepeater): repeater is RegionRepeater & { lat: number; lon: number } {
  const { lat, lon } = repeater;
  return lat != null && lon != null
    && Number.isFinite(lat) && Number.isFinite(lon)
    && lat >= -90 && lat <= 90
    && lon >= -180 && lon <= 180
    && !(Math.abs(lat) < 1e-6 && Math.abs(lon) < 1e-6);
}

export function buildRegionAreasGeoJSON(
  regions: readonly RegionSummary[],
  selectedRegion: string | null,
): GeoJSON.FeatureCollection<GeoJSON.Polygon, RegionFeatureProperties> {
  // Selecting a region hides every other region's shape so the chosen
  // footprint is unambiguous; clearing the selection restores all shapes.
  const visibleRegions = selectedRegion
    ? regions.filter((region) => region.name === selectedRegion)
    : regions;
  return {
    type: 'FeatureCollection',
    features: visibleRegions.flatMap((region) => {
      const ring = regionRing(region);
      if (!ring) return [];
      const selected = region.name === selectedRegion;
      return [{
        type: 'Feature' as const,
        geometry: { type: 'Polygon' as const, coordinates: [ring] },
        properties: {
          region: region.name,
          color: regionColor(region.name),
          selected,
          fill_opacity: selected ? 0.3 : 0.13,
          line_opacity: selected ? 1 : 0.55,
          line_width: selected ? 2.4 : 1.3,
          packets: region.packetsTotal,
          members: region.memberCount,
        },
      }];
    }),
  };
}

/**
 * Footprints for traffic that no public region name explains. Deliberately
 * labelled "unnamed": the shape is where the packets were heard, not a guessed
 * region name.
 */
export function buildUnattributedGeoJSON(
  clusters: readonly UnattributedCluster[],
): GeoJSON.FeatureCollection<GeoJSON.Polygon, RegionFeatureProperties> {
  return {
    type: 'FeatureCollection',
    features: clusters.map((cluster) => ({
      type: 'Feature' as const,
      geometry: { type: 'Polygon' as const, coordinates: [unattributedRing(cluster)] },
      properties: {
        cluster_id: cluster.id,
        packets: cluster.packets,
        nodes: cluster.nodeCount,
        label: `Unnamed · ${new Intl.NumberFormat('en-GB').format(cluster.packets)} pkts`,
      },
    })),
  };
}

export function buildRegionRepeatersGeoJSON(
  repeaters: readonly RegionRepeater[],
  filters: RegionRepeaterFilters,
): GeoJSON.FeatureCollection<GeoJSON.Point, RegionFeatureProperties> {
  return {
    type: 'FeatureCollection',
    features: repeaters
      .filter(hasCoordinates)
      .filter((repeater) => {
        if (filters.roleFilter === 'repeaters' && repeater.role !== 2) return false;
        if (filters.statusFilter !== 'all' && repeater.status !== filters.statusFilter) return false;
        if (filters.regionFilter) {
          if (filters.memberIds) {
            if (!filters.memberIds.has(repeater.nodeId)) return false;
          } else if (!((repeater.regions[filters.regionFilter] ?? 0) > 0)) {
            return false;
          }
        }
        return true;
      })
      .map((repeater) => ({
        type: 'Feature' as const,
        geometry: { type: 'Point' as const, coordinates: [repeater.lon, repeater.lat] },
        properties: {
          node_id: repeater.nodeId,
          name: repeater.name,
          label: repeater.name ?? repeater.nodeId.slice(0, 8),
          status: repeater.status,
          tier: repeater.status === 'green' ? confidenceFor(repeater.scopedPackets) : 'red',
          scoped_packets: repeater.scopedPackets,
          unscoped_packets_7d: repeater.unscopedPackets7d,
          region_count: Object.keys(repeater.regions).length,
          selected: repeater.nodeId === filters.selectedNodeId,
        },
      })),
  };
}
