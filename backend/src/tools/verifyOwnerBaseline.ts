/** Read-only live input capture; all comparison code comes from this checkout.
 * Run: cd backend && node --import tsx src/tools/verifyOwnerBaseline.ts
 * --all-baselines also explains historical drift; no accounts/ACLs are printed.
 */
import { execFileSync } from 'node:child_process';
import {
  buildOwnerInventoryBaseline,
  describeOwnerInventoryDrift,
  parseOwnerInventoryBaseline,
  validateOwnerInventoryBaseline,
} from '../owner/ownerInventoryBaseline.js';

const capture = `
import fs from 'node:fs';
import path from 'node:path';
import { getOwnerAuthorizationInventory, getOwnerAclReadiness, closeOwnerAuthDb } from './dist/db/ownerAuth.js';
import { readAclFile } from './dist/mqtt/aclManager.js';
import { parseOwnerGrantConfig } from './dist/owner/ownerGrantConfig.js';
try {
  const currentPath = process.env.OWNER_AUTH_INVENTORY_BASELINE_PATH;
  if (!currentPath) throw new Error('No inventory baseline configured');
  const directory = path.dirname(currentPath);
  const names = process.env.AUDIT_ALL_BASELINES === '1'
    ? fs.readdirSync(directory).filter(name => name.endsWith('.json'))
    : [path.basename(currentPath)];
  const database = await getOwnerAuthorizationInventory();
  const aclState = await getOwnerAclReadiness();
  console.log(JSON.stringify({
    input: { accounts: database.accounts, configuredGrants: parseOwnerGrantConfig(process.env.OWNER_MQTT_USERNAME_MAP ?? ''), aclContent: readAclFile(), aclState },
    baselines: names.map(name => ({ name, active: name === path.basename(currentPath), content: fs.readFileSync(path.join(directory, name), 'utf8') }))
  }));
} finally { await closeOwnerAuthDb(); }
`;
try {
  const snapshot = JSON.parse(execFileSync('docker', [
    'exec', '-i', '-e', 'PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=15000',
    '-e', `AUDIT_ALL_BASELINES=${process.argv.includes('--all-baselines') ? '1' : '0'}`,
    process.env['HEALTH_AUDIT_BACKEND_CONTAINER'] ?? 'meshcore-analytics-backend-1',
    'node', '--input-type=module',
  ], { input: capture, encoding: 'utf8', timeout: 30_000, maxBuffer: 4 * 1024 * 1024, stdio: ['pipe', 'pipe', 'pipe'] })) as {
    input: Parameters<typeof buildOwnerInventoryBaseline>[0];
    baselines: Array<{ name: string; active: boolean; content: string }>;
  };
  const current = buildOwnerInventoryBaseline(snapshot.input);
  const reports = snapshot.baselines.map(({ name, active, content }) => {
    const baseline = parseOwnerInventoryBaseline(content);
    const validation = validateOwnerInventoryBaseline(baseline, current);
    return { name, active, ...validation, drift: validation.ok ? null : describeOwnerInventoryDrift(baseline, current) };
  });
  console.log(JSON.stringify({ capturedAt: current.generatedAt, counts: current.counts, reports }, null, 2));
  if (reports.some(report => report.active && !report.ok)) process.exitCode = 2;
} catch {
  // execFile errors include child output; do not dump the private snapshot.
  console.error('Read-only owner baseline verification failed; check container access, DB availability and baseline checksum.');
  process.exitCode = 1;
}
