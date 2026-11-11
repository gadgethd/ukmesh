import { randomUUID } from 'node:crypto';
import { query } from '../db/index.js';
import { loadCurrentPublicPrivacyPolicy } from '../db/packetBatch.js';
import { observeWorkerOutcome } from '../metrics.js';
import type { LivePacket } from '../types/index.js';

type Intent = { observation_id: string; payload: { packet: LivePacket; upstreamEnvelope: Record<string, unknown> } };
type Enqueue = (packet: LivePacket, envelope: Record<string, unknown>) => Promise<void>;

export function createForwardingOutbox(enqueue: Enqueue, deps: {
  execute?: typeof query;
  loadPolicy?: typeof loadCurrentPublicPrivacyPolicy;
} = {}) {
  const execute = deps.execute ?? query;
  const loadPolicy = deps.loadPolicy ?? loadCurrentPublicPrivacyPolicy;
  let running: Promise<void> | null = null;
  let timer: ReturnType<typeof setInterval> | undefined;
  let stopped = false;
  async function processDue(): Promise<void> {
    if (stopped) return;
    if (running) return running;
    running = (async () => {
      const token = randomUUID();
      const result = await execute<Intent>(`WITH due AS (
        SELECT observation_id FROM packet_forwarding_outbox
        WHERE next_attempt_at <= NOW()
          AND (claim_expires_at IS NULL OR claim_expires_at <= NOW())
        ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
      ) UPDATE packet_forwarding_outbox o SET claim_token = $1,
        claim_expires_at = NOW() + INTERVAL '2 minutes', attempts = attempts + 1
        FROM due WHERE o.observation_id = due.observation_id
        RETURNING o.observation_id::text, o.payload`, [token]);
      if (!result.rows.length) return;
      const policy = await loadPolicy();
      if (!policy) throw new Error('FORWARDING_PRIVACY_POLICY_UNAVAILABLE');
      await Promise.all(result.rows.map(async (row) => {
        try {
          const { packet, upstreamEnvelope } = row.payload;
          const visibility = policy.classify({ network: packet.network ?? '', rxNodeId: packet.rxNodeId ?? null,
            srcNodeId: packet.srcNodeId ?? null, pathHashes: packet.path ?? null,
            pathHashSizeBytes: packet.pathHashSizeBytes ?? null });
          if (visibility.visibilityOk) {
            await enqueue({ ...packet, isPrivate: false, visibilityOk: true }, upstreamEnvelope);
          }
          await execute('DELETE FROM packet_forwarding_outbox WHERE observation_id = $1 AND claim_token = $2', [row.observation_id, token]);
          observeWorkerOutcome('forwarding_outbox', 'handoff', 'success');
        } catch (error) {
          await execute(`UPDATE packet_forwarding_outbox SET claim_token = NULL, claim_expires_at = NULL,
            next_attempt_at = NOW() + INTERVAL '30 seconds', last_error = $3
            WHERE observation_id = $1 AND claim_token = $2`, [row.observation_id, token,
            (error instanceof Error ? error.message : String(error)).slice(0, 500)]);
          observeWorkerOutcome('forwarding_outbox', 'handoff', 'failure');
        }
      }));
    })().catch((error: unknown) => {
      observeWorkerOutcome('forwarding_outbox', 'poll', 'failure');
      console.error('[forwarding-outbox]', error instanceof Error ? error.message : error);
    }).finally(() => { running = null; });
    return running;
  }
  return {
    processDue,
    start() {
      if (timer) return;
      stopped = false;
      timer = setInterval(() => void processDue(), 1_000);
      timer.unref();
      void processDue();
    },
    async stop() {
      stopped = true;
      if (timer) clearInterval(timer);
      timer = undefined;
      await running;
    },
  };
}
