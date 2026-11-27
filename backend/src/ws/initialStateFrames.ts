import { encodeInitialStateColumns, type InitialState } from '../contracts/initialStateColumns.generated.js';

export type InitialStateFrame = { prefix: string; bytes: number };

export function prepareInitialStateFrame(data: InitialState['data'], columns: boolean): InitialStateFrame {
  const envelope = { type: 'initial_state', data, ts: 0 } satisfies InitialState;
  // Both wire formats place the timestamp last. Retain the immutable prefix
  // so reconnecting clients need only write their own send timestamp.
  const prefix = JSON.stringify(columns ? encodeInitialStateColumns(envelope) : envelope).slice(0, -2);
  return { prefix, bytes: Buffer.byteLength(prefix) + 1 };
}

export function renderInitialStateFrame(frame: InitialStateFrame, timestamp: number, maxBytes: number): string | undefined {
  const text = String(timestamp);
  return frame.bytes + text.length <= maxBytes ? `${frame.prefix}${text}}` : undefined;
}
