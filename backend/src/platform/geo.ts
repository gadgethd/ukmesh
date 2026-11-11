/** Great-circle distance in kilometres (mean Earth radius). */
const EARTH_RADIUS_KM = 6371;

export function haversineKm(aLat: number, aLon: number, bLat: number, bLon: number): number {
  const toRadians = Math.PI / 180;
  const dLat = (bLat - aLat) * toRadians;
  const dLon = (bLon - aLon) * toRadians;
  const h = Math.sin(dLat / 2) ** 2
    + Math.cos(aLat * toRadians) * Math.cos(bLat * toRadians) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1, Math.sqrt(h)));
}
