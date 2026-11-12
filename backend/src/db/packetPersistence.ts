import { buildPacketBatchInput, type InsertPacketInput } from './packetInput.js';
import { configurePacketBatch, enqueuePacket, type PacketBatchResult } from './packetBatch.js';
import { ingestQuery, withIngestTransaction } from './runtime.js';

/** Called by the application before either durable intake or RAM admission starts. */
export function initializePacketPersistence(): void {
  configurePacketBatch(ingestQuery, withIngestTransaction);
}

export async function insertPacket(p: InsertPacketInput): Promise<PacketBatchResult> {
  return enqueuePacket(buildPacketBatchInput(p));
}
