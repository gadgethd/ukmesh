import assert from 'node:assert/strict';
import test from 'node:test';
import {
  nodeEffectiveLastSeenSql,
  nodeEffectiveOnlineSql,
} from './presence.js';

test('effective node presence gives fresh path evidence online precedence', () => {
  const lastSeenSql = nodeEffectiveLastSeenSql('node');
  const onlineSql = nodeEffectiveOnlineSql('node', '$4::timestamptz');

  assert.match(lastSeenSql, /node\.last_seen/);
  assert.match(lastSeenSql, /node\.last_rx_at/);
  assert.match(lastSeenSql, /node\.last_status_at/);
  assert.match(lastSeenSql, /node\.last_path_evidence_at/);
  assert.match(
    onlineSql,
    /node\.last_path_evidence_at > \$4::timestamptz - INTERVAL '60 minutes'[\s\S]*THEN TRUE/,
  );
  assert.ok(
    onlineSql.indexOf('last_path_evidence_at') < onlineSql.indexOf('last_rx_at'),
    'fresh path evidence must be evaluated before older direct observations',
  );
  assert.match(onlineSql, /ELSE node\.is_online/);
  assert.throws(() => nodeEffectiveOnlineSql('node; DROP TABLE nodes'));
});
