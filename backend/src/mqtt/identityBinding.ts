function normalizeNodeId(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const nodeId = value.trim().toUpperCase();
  return /^[0-9A-F]{64}$/.test(nodeId) ? nodeId : undefined;
}

/**
 * Status metadata is authoritative only for the observer named by the MQTT
 * topic. A missing origin_id is compatible with older clients, but a present
 * malformed or different identity rejects the entire envelope.
 */
export function statusEnvelopeTargetsObserver(
  observerKey: string,
  envelope: Record<string, unknown>,
): boolean {
  if (!Object.prototype.hasOwnProperty.call(envelope, 'origin_id')) return true;
  const value = envelope['origin_id'];
  if (value == null || String(value).trim() === '') return true;
  return normalizeNodeId(value) === normalizeNodeId(observerKey);
}

/**
 * TX advert envelope fields are not authenticated source evidence. Only a
 * successfully decoded advert containing its own valid public key may bind an
 * advert to a source node.
 */
export function shouldDiscardUnverifiedTxAdvert(input: {
  direction: string | undefined;
  packetType: number | undefined;
  decodedAdvertPayload: boolean;
}): boolean {
  return input.direction === 'tx'
    && input.packetType === 4
    && !input.decodedAdvertPayload;
}
