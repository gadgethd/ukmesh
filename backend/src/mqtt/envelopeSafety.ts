export function validPacketIntegers(values: Record<string, number | undefined>): boolean {
  const bounds: Record<string, number> = { packetType: 255, routeType: 3, hopCount: 255, pathHashSizeBytes: 3, advertCount: 2_147_483_647 };
  return Object.entries(values).every(([key, value]) => value == null
    || (Number.isSafeInteger(value) && value >= 0 && value <= (bounds[key] ?? 2_147_483_647)));
}

export function isPermanentDataError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error
    && typeof error.code === 'string' && /^22[0-9A-Z]{3}$/.test(error.code);
}
