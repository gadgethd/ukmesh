import assert from 'node:assert/strict';
import test from 'node:test';
import {
  mergeFeedPacketObservations,
  createFeedTimestampFormatter,
  formatFeedTimestamp,
  packetMatchesMessageScope,
  type FeedPacket,
} from './feedModel.js';

test('feed timestamps retain native locale formatting across date and daylight-saving boundaries', () => {
  const formatter = createFeedTimestampFormatter();
  for (const value of ['2026-03-29T00:59:59Z', '2026-03-29T01:00:00Z', '2026-10-25T00:59:59Z', '2026-10-25T01:00:00Z', '2027-01-01T00:00:00Z']) {
    assert.equal(formatFeedTimestamp(value, formatter), new Date(value).toLocaleString(undefined, {
      day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit',
    }));
  }
  assert.equal(formatFeedTimestamp('invalid'), '—');
  assert.equal(formatFeedTimestamp(''), '—');
});

test('feed timestamp formatting accepts a changed locale and timezone without changing the stored date', () => {
  const value = '2026-07-01T23:30:00Z';
  for (const [locale, timeZone] of [['en-GB', 'Europe/London'], ['ja-JP', 'Asia/Tokyo']]) {
    const formatter = new Intl.DateTimeFormat(locale, { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone });
    assert.equal(formatFeedTimestamp(value, formatter), formatter.format(new Date(value)));
  }
});

const retainedMessage: FeedPacket = {
  time: '2026-08-01T12:00:00.000Z',
  first_seen_time: '2026-08-01T11:59:55.000Z',
  packet_hash: 'ABCD',
  rx_node_id: 'AABBCCDD',
  observer_node_ids: ['AABBCCDD', '11223344'],
  iata: 'MME',
  observer_iatas: ['MME', 'NCL'],
  rx_count: 4,
  tx_count: 0,
  summary: 'retained message',
};

test('duplicate feed rows retain the richest observer and IATA aggregate', () => {
  const recentCopy: FeedPacket = {
    ...retainedMessage,
    time: '2026-08-01T12:00:01.000Z',
    first_seen_time: '2026-08-01T12:00:00.000Z',
    rx_node_id: '55667788',
    observer_node_ids: ['55667788'],
    iata: 'LBA',
    observer_iatas: ['LBA'],
    rx_count: 1,
    summary: 'newest decoded message',
  };

  assert.deepEqual(mergeFeedPacketObservations(retainedMessage, recentCopy), {
    ...recentCopy,
    first_seen_time: '2026-08-01T11:59:55.000Z',
    observer_node_ids: ['AABBCCDD', '11223344', '55667788'],
    observer_iatas: ['MME', 'NCL', 'LBA'],
    iata: 'LBA',
    rx_count: 4,
    tx_count: 0,
  });
});

test('channel scopes match the bracketed label case-insensitively', () => {
  assert.equal(packetMatchesMessageScope({ ...retainedMessage, summary: '[bot] historic text' }, 'bot'), true);
  assert.equal(packetMatchesMessageScope({ ...retainedMessage, summary: '[BOT] historic text' }, 'bot'), true);
  assert.equal(packetMatchesMessageScope({ ...retainedMessage, summary: '[bot-extra] historic text' }, 'bot'), false);
});
