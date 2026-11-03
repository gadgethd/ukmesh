import pg from 'pg';
import { getRecentMessages, getRecentPackets } from '../db/index.js';
import type { QueryFn } from '../db/packetBatch.js';

const databaseUrl = process.env['DATABASE_URL'];
if (!databaseUrl) throw new Error('DATABASE_URL is required');

const pool = new pg.Pool({
  connectionString: databaseUrl,
  max: 2,
  application_name: 'meshcore-p2-10-reader-differential',
});

const scenarios = [
  { limit: 50, network: undefined, fields: 'full' as const },
  { limit: 50, network: 'ukmesh', fields: 'full' as const },
  { limit: 50, network: 'ukmesh', fields: 'slim' as const },
];

const normalize = (value: unknown) => JSON.stringify(value);
const client = await pool.connect();
const previous = process.env['PACKET_IDENTITY_READ_AUTHORITY'];
let failed = false;
try {
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  const query: QueryFn = (sql, params) => client.query(sql, params);
  for (const scenario of scenarios) {
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'hash';
    const oldPackets = await getRecentPackets(scenario.limit, scenario.network, undefined, scenario.fields, undefined, query);
    const oldMessages = await getRecentMessages(scenario.limit, scenario.network, undefined, undefined, query);
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'uid';
    const newPackets = await getRecentPackets(scenario.limit, scenario.network, undefined, scenario.fields, undefined, query);
    const newMessages = await getRecentMessages(scenario.limit, scenario.network, undefined, undefined, query);
    const packetDiff = normalize(oldPackets) !== normalize(newPackets);
    const messageDiff = normalize(oldMessages) !== normalize(newMessages);
    console.log(JSON.stringify({
      scenario,
      packets: { hash: oldPackets.length, uid: newPackets.length, different: packetDiff },
      messages: { hash: oldMessages.length, uid: newMessages.length, different: messageDiff },
    }));
    if (packetDiff || messageDiff) failed = true;
  }
  await client.query('COMMIT');
} catch (error) {
  await client.query('ROLLBACK').catch(() => {});
  throw error;
} finally {
  if (previous === undefined) delete process.env['PACKET_IDENTITY_READ_AUTHORITY'];
  else process.env['PACKET_IDENTITY_READ_AUTHORITY'] = previous;
  client.release();
  await pool.end();
}
if (failed) process.exitCode = 1;
