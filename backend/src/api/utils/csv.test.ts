import assert from 'node:assert/strict';
import test from 'node:test';
import { csvCell, spreadsheetSafeText } from './csv.js';

test('spreadsheet formula prefixes are forced to literal text before CSV quoting', () => {
  for (const value of ['=1+1', '+SUM(A1)', '-2+3', '@cmd', ' =1', '\t=1', '\r+1', '\n-1']) {
    const encoded = csvCell(value);
    const decoded = encoded.startsWith('"')
      ? encoded.slice(1, -1).replace(/""/g, '"')
      : encoded;
    assert.ok(decoded.startsWith("'"), value);
  }
  assert.equal(csvCell(-12), '-12');
  assert.equal(spreadsheetSafeText("'=safe"), "'=safe");
});
