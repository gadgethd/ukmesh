import { timingSafeEqual } from 'node:crypto';
import { Router } from 'express';
import { query } from '../db/runtime.js';
import { beaconPublicationRegistry } from '../mqtt/channelRegistry.js';
import { readPublicResponseGeneration } from '../privacy/responseGeneration.js';

export function createBeaconPublicAuthority(options: {
  token?: string;
  secrets?: string;
  allowlist?: string;
  streamToken?: string;
  readGeneration?: () => Promise<number>;
} = {
  token: process.env['BEACON_PUBLIC_READ_TOKEN'],
  secrets: process.env['MESHCORE_CHANNEL_SECRETS'],
  allowlist: process.env['PUBLIC_DECRYPT_CHANNELS'],
  streamToken: process.env['MQTT_RAM_PUBLIC_READ_TOKEN'],
}): Router {
  const router = Router();
  const { token, streamToken } = options;
  if (!token || token.length < 32 || token === streamToken || /\s/.test(token)) return router;
  const authorization = Buffer.from(`Bearer ${token}`);
  const readGeneration = options.readGeneration ?? (() => readPublicResponseGeneration(query));
  let registry: ReturnType<typeof beaconPublicationRegistry> | undefined;
  try {
    registry = beaconPublicationRegistry(options.secrets, options.allowlist);
  } catch {
    // The core backend can continue serving while an invalid optional registry is corrected.
  }

  router.get('/authority', async (request, response) => {
    response.set('Cache-Control', 'no-store');
    const actual = Buffer.from(request.get('authorization') ?? '');
    if (actual.length !== authorization.length || !timingSafeEqual(actual, authorization)) {
      response.sendStatus(401);
      return;
    }
    try {
      if (!registry) throw new Error('BEACON_PUBLIC_REGISTRY_UNAVAILABLE');
      const generation = await readGeneration();
      if (!Number.isSafeInteger(generation) || generation < 1) throw new Error('BEACON_PUBLIC_GENERATION_UNAVAILABLE');
      response.json({ version: 1, network: 'ukmesh', generation, ...registry });
    } catch {
      response.sendStatus(503);
    }
  });
  return router;
}
