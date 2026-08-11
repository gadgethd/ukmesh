export type MqttTopicParts = {
  iata: string;
  observerKey: string;
  suffix: 'packets' | 'status' | 'neighbors' | 'neighbours';
  network: 'ukmesh' | 'test';
};

/**
 * Validate the mctomqtt topic envelope before it reaches persistence. Public
 * prefixes share the UKMesh scope; every other configured prefix is an
 * isolated test/development feed.
 */
export function parseMqttTopic(
  topic: string,
  acceptedPrefixes: ReadonlySet<string>,
  blockedIatas: ReadonlySet<string>,
): MqttTopicParts | null {
  const parts = topic.split('/');
  if (parts.length !== 4) return null;

  const prefix = parts[0]?.trim().toLowerCase();
  if (!prefix || !acceptedPrefixes.has(prefix)) return null;

  const iata = (parts[1] ?? '').trim().toUpperCase();
  // Test-marker IATAs are not dropped at ingest: they persist under the
  // isolated 'test' network scope so they never surface on public scopes.
  const isTestMarker = blockedIatas.has(iata);
  if (!/^[A-Z0-9]{2,8}$/.test(iata)) return null;

  // Database node IDs and decoded MeshCore public keys are canonical uppercase.
  // Keep that form at the persistence boundary; WebSocket/API layers normalize
  // independently for browser filtering.
  const observerKey = (parts[2] ?? '').trim().toUpperCase();
  if (!/^[0-9A-F]{64}$/.test(observerKey)) return null;

  const suffix = (parts[3] ?? '').trim().toLowerCase();
  if (suffix !== 'packets' && suffix !== 'status' && suffix !== 'neighbors' && suffix !== 'neighbours') return null;

  return {
    iata,
    observerKey,
    suffix,
    network: isTestMarker ? 'test' : prefix === 'meshcore' || prefix === 'ukmesh' ? 'ukmesh' : 'test',
  };
}
