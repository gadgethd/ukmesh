import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { assertLocalDatabaseUrl, csvBoolean, csvInteger, csvJson, inspectGzipCsv, parseCsv, readGzipCsv, rowObject } from './sampleData.js';

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

test('sample database writes reject remote hosts and DSN host overrides', () => {
  for (const value of ['postgresql://user:pass@127.0.0.1:5433/fixture', 'postgres://localhost/fixture']) {
    assert.doesNotThrow(() => assertLocalDatabaseUrl(value));
  }
  for (const value of ['postgresql://db.example/meshcore', 'postgresql://localhost/db?host=db.example',
    'postgresql://localhost/db?hostaddr=192.0.2.1', 'https://localhost/db', 'postgresql://localhost']) {
    assert.throws(() => assertLocalDatabaseUrl(value));
  }
});

test('streamed CSV inspection preserves quoted UTF-8 records across gzip chunks', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-csv-'));
  try {
    const file = path.join(directory, 'fixture.csv.gz');
    const value = 'é'.repeat(32_766) + '""quoted\nvalue';
    await writeFile(file, gzipSync(`time,summary\n2026-11-12T00:00:00Z,"${value}"\n`));
    const table = await readGzipCsv(file);
    assert.equal(table.rows.length, 1);
    assert.equal(table.rows[0][1], value.replace('""', '"'));
    const scan = await inspectGzipCsv(file);
    assert.equal(scan.count, 1);
    assert.equal(scan.lastTime, Date.parse('2026-11-12T00:00:00Z'));
    await writeFile(file, gzipSync('a,b\n1,"unterminated'));
    await assert.rejects(inspectGzipCsv(file), /unterminated/);
    await writeFile(file, gzipSync('a,b\n1,2,3\n'));
    await assert.rejects(inspectGzipCsv(file), /width mismatch/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
