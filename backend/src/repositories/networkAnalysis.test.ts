import assert from 'node:assert/strict';
import test from 'node:test';
import type { QueryResultRow } from 'pg';
import { networkFilters } from '../api/utils/networkFilters.js';
import { rfValidationRows, queryRfValidation, rfValidationPool, RF_VALIDATION_DB_TIMEOUT_MS } from './networkAnalysis.js';

test('RF validation scopes endpoints without joining both identity-node projections', async () => {
  const statements: string[] = [];
  const controller = new AbortController();
  const query = async <T extends QueryResultRow = QueryResultRow>(
    sql: string,
    _params?: unknown[],
    signal?: AbortSignal,
  ): Promise<{ rows: T[] }> => {
    assert.equal(signal, controller.signal);
    statements.push(sql);
    if (sql.includes('FROM node_identity_nodes')) {
      return { rows: [
        { node_id: 'A', name: 'Alpha' },
        { node_id: 'B', name: 'Bravo' },
        { node_id: 'C', name: 'Charlie' },
      ] as unknown as T[] };
    }
    return { rows: [
      {
        node_a_id: 'A', node_b_id: 'OUTSIDE', observed_count: '99',
        multibyte_observed_count: '5', itm_path_loss_db: 100,
        itm_viable: false, force_viable: false,
        last_observed: '2026-08-20 10:00:00+00', classification: 'observed_unexpected',
      },
      {
        node_a_id: 'A', node_b_id: 'B', observed_count: '20',
        multibyte_observed_count: '2', itm_path_loss_db: 110,
        itm_viable: false, force_viable: false,
        last_observed: '2026-08-20 09:00:00+00', classification: 'observed_unexpected',
      },
      {
        node_a_id: 'B', node_b_id: 'C', observed_count: '10',
        multibyte_observed_count: '1', itm_path_loss_db: 120,
        itm_viable: true, force_viable: false,
        last_observed: '2026-08-20 08:00:00+00', classification: 'match',
      },
    ] as unknown as T[] };
  };

  const result = await rfValidationRows(query, networkFilters('ukmesh'), 1, controller.signal);
  assert.equal(statements.length, 2);
  assert.doesNotMatch(statements[1]!, /JOIN\s+node_identity_nodes/i);
  assert.deepEqual(result.rows, [{
    node_a_id: 'A', node_b_id: 'B', observed_count: '20',
    multibyte_observed_count: '2', itm_path_loss_db: 110,
    itm_viable: false, force_viable: false,
    last_observed: '2026-08-20 09:00:00+00', classification: 'observed_unexpected',
    name_a: 'Alpha', name_b: 'Bravo',
  }]);
});

test('empty RF scope does not run a link scan', async () => {
  let calls = 0;
  assert.deepEqual(await rfValidationRows(async () => { calls += 1; return { rows: [] }; }, networkFilters('test'), 100), { rows: [] });
  assert.equal(calls, 1);
});

test('RF pool has an independent server timeout and read-only sessions', () => {
  assert.equal(rfValidationPool.options.statement_timeout, RF_VALIDATION_DB_TIMEOUT_MS);
  assert.equal(RF_VALIDATION_DB_TIMEOUT_MS, 3_500);
  assert.equal(rfValidationPool.options.max, 2);
  assert.match(rfValidationPool.options.options!, /default_transaction_read_only=on/);
});

test('RF cancellation destroys only its checked-out connection; successful reads release normally', async (t) => {
  const released: Array<boolean | undefined> = [];
  let pending = false;
  t.mock.method(rfValidationPool, 'connect', async () => ({
    release: (destroy?: boolean) => released.push(destroy),
    query: async () => pending ? new Promise(() => {}) : { rows: [{ ok: true }] },
  }));
  assert.deepEqual(await queryRfValidation('SELECT true AS ok'), { rows: [{ ok: true }] });
  const controller = new AbortController();
  pending = true;
  const result = queryRfValidation('SELECT true AS ok', [], controller.signal);
  await new Promise<void>((resolve) => setImmediate(resolve));
  controller.abort(new Error('disconnected'));
  await assert.rejects(result, /disconnected/);
  assert.deepEqual(released, [false, true]);
});
