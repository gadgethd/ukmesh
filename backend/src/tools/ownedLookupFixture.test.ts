import assert from 'node:assert/strict';
import test from 'node:test';
import { validateLeanFixtureIdentity, validateLeanFixtureSelection } from './ownedLookupFixture.js';

const token = '20261114010000_abcd';
const database = 'lean_10x_' + token;
const endpoint = 'postgresql://meshcore:fixture-password@127.0.0.1:5433/' + database;
const selection = { token, targets: { ukmesh: database } };
const fixture = { token, ready: true, databases: [{ label: 'ukmesh', database, cluster: '1234',
  container: 'ukmesh-sample-db', restored: true }] };

test('only the currently selected, fully restored local fixture is accepted', () => {
  assert.deepEqual(validateLeanFixtureSelection(endpoint, selection, fixture), {
    database, cluster: '1234', ownership: 'ukmesh-owned-fixture:' + token,
  });
});

test('a lean prefix does not authorize an original, unselected or remote database', () => {
  for (const value of [endpoint.replace(database, 'meshcore'), endpoint.replace(database, 'lean_10x_other'),
    endpoint.replace('127.0.0.1', 'database.example'), endpoint.replace(':5433', ':5432'),
    endpoint + '?options=-csearch_path%3Dpublic']) {
    assert.throws(() => validateLeanFixtureSelection(value, selection, fixture));
  }
  assert.throws(() => validateLeanFixtureSelection(endpoint, { token, targets: 'original' }, fixture));
});

test('incomplete, mismatched and forged fixture ownership is rejected', () => {
  for (const value of [{ ...fixture, ready: false }, { ...fixture, token: 'different' },
    { ...fixture, databases: [] },
    { ...fixture, databases: [{ ...fixture.databases[0]!, container: 'other-server' }] },
    { ...fixture, databases: [{ ...fixture.databases[0]!, restored: false }] }]) {
    assert.throws(() => validateLeanFixtureSelection(endpoint, selection, value));
  }
  assert.throws(() => validateLeanFixtureSelection(endpoint, { ...selection, token: '../other' }, fixture));
});

test('a matching name on another cluster or without its ownership comment is rejected', () => {
  const expected = { database, cluster: '1234', ownership: 'ukmesh-owned-fixture:' + token };
  assert.doesNotThrow(() => validateLeanFixtureIdentity(expected, expected));
  assert.throws(() => validateLeanFixtureIdentity(expected, { ...expected, cluster: '5678' }));
  assert.throws(() => validateLeanFixtureIdentity(expected, { ...expected, ownership: null }));
});
