export interface SpamZone {
  lat: number;
  lon: number;
  radiusKm: number;
}

export interface SpamMapIncident {
  id: string;
  network: string;
  status: 'active' | 'closed';
  origin: {
    confidence: number;
    zone: SpamZone | null;
  };
}

export interface SpamOriginGroup {
  id: string;
  network: string;
  incidentIds: string[];
  active: boolean;
  zone: SpamZone;
}

// Public centres are rounded to a ~0.1° privacy grid. Allow neighbouring
// cells, but never use the uncertainty radius as a licence to join a region.
const MAX_CENTRE_DISTANCE_KM = 15;

function distanceKm(a: SpamZone, b: SpamZone): number {
  const radians = Math.PI / 180;
  const dLat = (b.lat - a.lat) * radians;
  const dLon = (b.lon - a.lon) * radians;
  const h = Math.sin(dLat / 2) ** 2
    + Math.cos(a.lat * radians) * Math.cos(b.lat * radians) * Math.sin(dLon / 2) ** 2;
  return 6371 * 2 * Math.asin(Math.sqrt(Math.min(1, h)));
}

/** Group similar coarse origin estimates for display; this is not sender attribution. */
export function groupSpamOrigins(incidents: readonly SpamMapIncident[]): SpamOriginGroup[] {
  const located = incidents
    .filter((incident): incident is SpamMapIncident & { origin: { confidence: number; zone: SpamZone } } => {
      const zone = incident.origin.zone;
      return zone !== null && Number.isFinite(zone.lat) && Math.abs(zone.lat) <= 90
        && Number.isFinite(zone.lon) && Math.abs(zone.lon) <= 180
        && Number.isFinite(zone.radiusKm) && zone.radiusKm > 0;
    })
    // Anchor on the strongest estimate. Stable tie-breaking prevents API order
    // or active/closed transitions from moving circles around.
    .sort((a, b) => b.origin.confidence - a.origin.confidence || a.id.localeCompare(b.id));
  const groups: { group: SpamOriginGroup; centres: SpamZone[] }[] = [];

  for (const incident of located) {
    const zone = incident.origin.zone;
    const match = groups.find(({ group, centres }) => group.network === incident.network
      // Compare every member to prevent a chain of neighbours spanning the UK.
      && centres.every((centre) => distanceKm(centre, zone) <= Math.min(
        MAX_CENTRE_DISTANCE_KM, centre.radiusKm, zone.radiusKm,
      )));
    if (match) {
      match.group.incidentIds.push(incident.id);
      match.group.active ||= incident.status === 'active';
      // Keep the strongest existing coarse centre, and enclose every member's
      // full uncertainty circle. Grouping must not imply a more precise origin.
      match.group.zone.radiusKm = Math.max(
        match.group.zone.radiusKm,
        Math.ceil(distanceKm(match.group.zone, zone) + zone.radiusKm),
      );
      match.centres.push(zone);
    } else {
      groups.push({
        group: {
          id: incident.id,
          network: incident.network,
          incidentIds: [incident.id],
          active: incident.status === 'active',
          zone: { ...zone },
        },
        centres: [zone],
      });
    }
  }
  return groups.map(({ group }) => group);
}

/** Build an approximate circle polygon (lon/lat ring) for a coarse heat zone. */
export function circlePolygon(lat: number, lon: number, radiusKm: number, steps = 48): GeoJSON.Position[] {
  const ring: GeoJSON.Position[] = [];
  const dLat = radiusKm / 111.32;
  const dLon = radiusKm / (111.32 * Math.cos((lat * Math.PI) / 180) || 1);
  for (let i = 0; i <= steps; i++) {
    const theta = (i / steps) * 2 * Math.PI;
    ring.push([lon + dLon * Math.cos(theta), lat + dLat * Math.sin(theta)]);
  }
  return ring;
}

export function buildZonesGeoJSON(groups: readonly SpamOriginGroup[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: groups.flatMap((group): GeoJSON.Feature[] => {
      const properties = { id: group.id, active: group.active, incidentCount: group.incidentIds.length };
      return [
        {
          type: 'Feature',
          properties,
          geometry: { type: 'Polygon', coordinates: [circlePolygon(group.zone.lat, group.zone.lon, group.zone.radiusKm)] },
        },
        {
          type: 'Feature',
          properties,
          geometry: { type: 'Point', coordinates: [group.zone.lon, group.zone.lat] },
        },
      ];
    }),
  };
}
