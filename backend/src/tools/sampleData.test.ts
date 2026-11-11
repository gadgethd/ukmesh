import assert from 'node:assert/strict';
import test from 'node:test';
import { csvBoolean, csvInteger, csvJson, parseCsv, rowObject } from './sampleData.js';

test('parseCsv handles quoted commas, quotes and newlines', () => {
  const table = parseCsv(
    'a,b,c\n'
      + '1,"two, three","say ""hi"""\n'
      + '4,"line\nfive",6\n',
  );
  assert.deepEqual(table.headers, ['a', 'b', 'c']);
  assert.deepEqual(table.rows, [
    ['1', 'two, three', 'say "hi"'],
    ['4', 'line\nfive', '6'],
  ]);
});

test('parseCsv ignores a trailing blank line', () => {
  const table = parseCsv('a,b\n1,2\n');
  assert.deepEqual(table.rows, [['1', '2']]);
});

test('rowObject maps headers to values', () => {
  assert.deepEqual(rowObject(['a', 'b'], ['1', '2']), { a: '1', b: '2' });
});

test('sample scalar parsers reject empty and malformed values', () => {
  assert.equal(csvInteger('42'), 42);
  assert.equal(csvInteger(''), null);
  assert.equal(csvInteger('4.5'), 4);
  assert.equal(csvBoolean('t'), true);
  assert.equal(csvBoolean('0'), false);
  assert.equal(csvBoolean('yes'), null);
  assert.deepEqual(csvJson('{"a":1}'), { a: 1 });
  assert.equal(csvJson('not-json'), null);
});
