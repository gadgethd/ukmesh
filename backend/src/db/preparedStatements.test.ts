import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import type pg from 'pg';
import {
  BOUNDED_READ_PREPARATION,
  FIXED_PREPARED_STATEMENTS,
  preparedStatementForClient,
  type StatementHint,
} from './preparedStatements.js';

const client = () => ({} as pg.PoolClient);

test('bounded reads use the full SQL digest with fresh values and no caller-supplied name', () => {
  const connection = client();
  const sql = 'SELECT $1::text AS "café"';
  const firstValues = ['first'], secondValues = ['second'];
  const first = preparedStatementForClient(connection, BOUNDED_READ_PREPARATION, sql, firstValues);
  const second = preparedStatementForClient(connection, BOUNDED_READ_PREPARATION, sql, secondValues);
  assert.equal(first.name, `read_v1_${createHash('sha256').update(sql, 'utf8').digest('base64url')}`);
  assert.match(first.name!, /^[A-Za-z0-9_-]+$/);
  assert.ok(Buffer.byteLength(first.name!) <= 63);
  assert.equal(second.name, first.name);
  assert.equal(first.values, firstValues);
  assert.equal(second.values, secondValues);
  assert.equal(first.text, sql);
  assert.notEqual(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION,
    sql + ' /* another shape */', secondValues).name, first.name);
  assert.equal(Object.isFrozen(BOUNDED_READ_PREPARATION), true);
});

test('a connection admits only32read shapes, retains hot shapes and leaves overflow unnamed', () => {
  const connection = client();
  const initial = preparedStatementForClient(connection, BOUNDED_READ_PREPARATION, 'SELECT $1::int', [0]);
  for (let index = 1; index < 100; index++) {
    assert.equal(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION,
      'SELECT $1::int', [index]).name, initial.name, 'values do not consume shape capacity');
  }
  const names = new Set([initial.name]);
  for (let index = 1; index < 32; index++) {
    names.add(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION,
      `SELECT $1::int /* ${index} */`, [index]).name);
  }
  assert.equal(names.size, 32);
  assert.equal(names.has(undefined), false);
  const values = ['overflow'];
  const overflowSql = 'SELECT $1::text /* overflow */';
  assert.deepEqual(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION, overflowSql, values),
    { text: overflowSql, values });
  assert.equal(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION,
    'SELECT $1::int', [999]).name, initial.name, 'overflow evicts nothing');
  assert.ok(preparedStatementForClient(client(), BOUNDED_READ_PREPARATION, overflowSql, values).name,
    'a different physical client has independent capacity');
});

test('fixed statements stay outside the read capacity and malformed hints cannot allocate names', () => {
  const connection = client();
  for (const name of Object.values(FIXED_PREPARED_STATEMENTS)) {
    assert.deepEqual(preparedStatementForClient(connection, name, 'SELECT $1::int', [1]),
      { name, text: 'SELECT $1::int', values: [1] });
  }
  for (const hint of [null, {}, { kind: 'other' }, { kind: 'bounded-read', name: 'unbounded' }, 'unregistered']) {
    assert.throws(() => preparedStatementForClient(connection, hint as StatementHint, 'SELECT 1'),
      /INVALID_(?:FIXED_PREPARED_STATEMENT|STATEMENT_HINT)/);
  }
  for (let index = 0; index < 32; index++) {
    assert.ok(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION, `SELECT ${index}`).name);
  }
  assert.equal(preparedStatementForClient(connection, BOUNDED_READ_PREPARATION, 'SELECT 33').name, undefined);
});
