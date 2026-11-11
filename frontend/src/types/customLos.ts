export interface CustomLosPoint {
  lat: number;
  lon: number;
  elevation_m: number;
}

export interface CustomLosSegment {
  path: [number, number, number][];
  obstructed: boolean;
}
