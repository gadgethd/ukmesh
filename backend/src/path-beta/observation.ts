import { pathingConfig } from '../platform/config/pathing.js';

export type PathObservation = {
  packetHash: string;
  network: string;
  slow: boolean;
  warm: boolean;
};

export function makePathObservation({
  finalHash, network, decodedPathHashSizeBytes, path, resolvedPacketType,
}: {
  finalHash: string;
  network: string;
  decodedPathHashSizeBytes: number | undefined;
  path: string[] | undefined;
  resolvedPacketType: number | undefined;
}): PathObservation {
  return {
    packetHash: finalHash,
    network,
    slow: (decodedPathHashSizeBytes ?? 0) >= 2 && (path?.length ?? 0) >= pathingConfig.slowModeMinPathHops,
    warm: resolvedPacketType === 4 || resolvedPacketType === 5,
  };
}

export function isPathObservation(value: unknown): value is PathObservation {
  if (!value || typeof value !== 'object') return false;
  const data = value as Record<string, unknown>;
  return typeof data['packetHash'] === 'string' && /^[a-fA-F0-9]{16,128}$/.test(data['packetHash'])
    && typeof data['network'] === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(data['network'])
    && typeof data['slow'] === 'boolean' && typeof data['warm'] === 'boolean';
}
