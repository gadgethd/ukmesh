import { Router } from 'express';

const router = Router();
const RADIO_BOT_URL = process.env['RADIO_BOT_URL'] ?? 'http://meshcore-radio-bot:3011';

router.get('/radio-history', async (req, res) => {
  const target = String(req.query['target'] ?? '').trim();
  const limit = Math.min(Number(req.query['limit'] ?? 168), 500);
  if (!target) {
    res.status(400).json({ error: 'target required' });
    return;
  }

  try {
    const upstream = await fetch(`${RADIO_BOT_URL}/history`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ target, limit }),
    });
    if (!upstream.ok) {
      res.status(502).json({ error: 'radio bot unavailable' });
      return;
    }
    res.json(await upstream.json());
  } catch {
    res.status(503).json({ error: 'radio bot unreachable' });
  }
});

router.get('/radio-stats', async (_req, res) => {
  try {
    const upstream = await fetch(`${RADIO_BOT_URL}/state`);
    if (!upstream.ok) {
      res.status(502).json({ error: 'radio bot unavailable' });
      return;
    }
    const data = await upstream.json();
    res.json(data);
  } catch {
    res.status(503).json({ error: 'radio bot unreachable' });
  }
});

export default router;
