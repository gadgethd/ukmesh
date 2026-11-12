/** Checks persistence and live delivery after a bounded, nonduplicate replay. */
import pg from 'pg';
import WebSocket from 'ws';
import { pathToFileURL } from 'node:url';
import { assertLocalDatabaseUrl, option, requiredOption } from './sampleData.js';
import { replaySampleMqtt } from './replaySampleMqtt.js';

export async function smokeSampleReplay(args: string[]): Promise<void> {
  const url = process.env['DATABASE_URL'] ?? '';
  assertLocalDatabaseUrl(url);
  const limit = Number(option(args, 'limit') ?? 20);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error('smoke --limit must be 1..1000');
  const directory = requiredOption(args, 'data-dir');
  const pool = new pg.Pool({ connectionString: url, max: 1 });
  const ws = new WebSocket('ws://127.0.0.1:3100/ws?network=ukmesh', {
    origin: 'http://127.0.0.1:5173',
  });
  const seen = new Set<string>();
  ws.on('message', (bytes) => {
    for (const line of bytes.toString().split('\n').filter(Boolean)) {
      const msg = JSON.parse(line);
      if (msg.type === 'packet' && typeof msg.data?.packetHash === 'string') seen.add(msg.data.packetHash);
    }
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('local WebSocket did not open')), 15_000);
      ws.once('open', () => { clearTimeout(timer); resolve(); });
      ws.once('error', (error) => { clearTimeout(timer); reject(error); });
    });
    const counts = async () => (await pool.query<{ packets: number; status: number }>(
      `SELECT (SELECT COUNT(*)::int FROM mesh.packets) AS packets,
        (SELECT COUNT(*)::int FROM core.node_status_samples) AS status`,
    )).rows[0]!;
    const before = await counts();
    await replaySampleMqtt(['--data-dir', directory, '--limit', String(limit), '--speed', '0']);
    await replaySampleMqtt(['--data-dir', directory, '--limit', '1', '--speed', '0', '--status']);
    const deadline = Date.now() + 45_000;
    let after = before;
    while (Date.now() < deadline) {
      after = await counts();
      if (after.packets - before.packets === limit && after.status > before.status && seen.size > 0) break;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    if (after.packets - before.packets !== limit || after.status <= before.status || seen.size === 0) {
      throw new Error(`pipeline smoke failed: persisted=${after.packets - before.packets}/${limit}, status=${after.status - before.status}, live=${seen.size}`);
    }
    const response = await fetch('http://127.0.0.1:3100/api/packets/recent?network=ukmesh&limit=20', { signal: AbortSignal.timeout(30_000) });
    const packets: unknown = await response.json();
    if (!response.ok || !Array.isArray(packets) || packets.length === 0) throw new Error('recent API returned no replayed data');
    console.log(JSON.stringify({ passed: true, persisted: after.packets - before.packets,
      statusPersisted: after.status - before.status, livePackets: seen.size, recent: packets.length }));
  } finally { ws.terminate(); await pool.end(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  smokeSampleReplay(process.argv.slice(2)).catch((error: unknown) => {
    console.error('[sample:smoke]', error instanceof Error ? error.message : 'failed');
    process.exitCode = 1;
  });
}
