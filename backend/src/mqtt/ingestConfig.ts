/** Persistent shared sessions need a proven broker restart/resize recovery path. */
export function assertSupportedIngestConnections(connections: number): void {
  if (connections !== 1) {
    throw new Error(`MQTT_INGEST_CONNECTIONS_UNSUPPORTED:${connections}; use 1 until shared-session recovery is qualified`);
  }
}
