import { deliverWebhook } from '../security/outboundWebhook.js';

const MAX_ATTEMPTS = 2;
const RETRY_DELAY_MS = 1_000;

export type ObserverRegistrationNotice = {
  requestId: string;
  publicKey: string;
  iata: string;
  name: string | null;
  contact: string;
};

function webhookUrl(): string | null {
  const value = (process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'] ?? '').trim();
  return value || null;
}

export function observerRegistrationPayload(
  input: ObserverRegistrationNotice,
  submittedAt = new Date(),
): Record<string, unknown> {
  return {
    username: 'UKMesh observer registrations',
    // Never let a registrant's display name or contact turn into a ping.
    allowed_mentions: { parse: [] },
    embeds: [{
      title: `New observer registration #${input.requestId}`,
      color: 0x00C8FF,
      timestamp: submittedAt.toISOString(),
      fields: [
        { name: 'Public key', value: `\`${input.publicKey}\``, inline: false },
        { name: 'IATA region', value: input.iata, inline: true },
        { name: 'Display name', value: input.name ?? '—', inline: true },
        { name: 'Contact', value: input.contact, inline: true },
        { name: 'Status', value: 'pending', inline: true },
      ],
      footer: { text: 'Review in the operator page: /observer-registrations' },
    }],
  };
}

async function post(url: string, payload: Record<string, unknown>): Promise<void> {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    try {
      await deliverWebhook(url, payload);
      return;
    } catch (error) {
      if (attempt === MAX_ATTEMPTS) {
        console.error(
          '[observer-registration] webhook delivery failed:',
          (error as Error).message,
        );
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}

/**
 * Fire-and-forget Discord notification for a newly accepted observer
 * registration. Never throws: a notification failure must not fail the
 * public registration request.
 */
export function notifyObserverRegistration(input: ObserverRegistrationNotice): void {
  const url = webhookUrl();
  if (!url) return;
  void post(url, observerRegistrationPayload(input));
}
