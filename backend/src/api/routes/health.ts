import { Router } from 'express';
import { HEALTH_CACHE_TTL_MS, healthSnapshot } from '../bootstrap/caches.js';
import {
  hasOperatorAuthorization,
  isLocalClientRequest,
} from '../utils/localOnly.js';
import { registerHealthRoutes } from './healthRoutes.js';

const router = Router();
const refreshTimer = setInterval(() => {
  void healthSnapshot.refresh();
}, HEALTH_CACHE_TTL_MS);
refreshTimer.unref();
setImmediate(() => {
  void healthSnapshot.refresh();
});

registerHealthRoutes(router, {
  readSnapshot: () => healthSnapshot.read(),
  isLocalClient: isLocalClientRequest,
  hasOperatorAuthorization,
});

export default router;
