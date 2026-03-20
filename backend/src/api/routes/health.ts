import { Router } from 'express';
import { getWorkerHealthOverview } from '../../health/status.js';

const router = Router();

router.get('/health', async (_req, res) => {
  try {
    const data = await getWorkerHealthOverview();
    res.json(data);
  } catch (err) {
    console.error('[api] GET /health', (err as Error).message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
