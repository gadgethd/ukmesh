"""Select the public broadcast and telemetry subset of a local radio archive."""

import csv
from datetime import datetime
import gzip
import hashlib
import hmac
import json
import re
from heapq import merge

PUBLIC_KEYS = tuple(bytes.fromhex(key) for key in (
    '8b3387e9c5cdea6ac9e5edbaa115cd72',  # MeshCore Public
    '9cd8fcf22a47333b591d96a2b848b73f',  # DEV #test
))
CHANNELS = {hashlib.sha256(key).digest()[0]: key.ljust(32, b'\0') for key in PUBLIC_KEYS}
PUBLIC_TYPES = {'3', '4', '5'}
STATUS_FIELDS = ('battery_mv', 'uptime_secs', 'tx_air_secs', 'rx_air_secs', 'channel_utilization', 'air_util_tx')
IATA = re.compile(r'[A-Z]{3}\Z')
KEY = re.compile(r'[a-fA-F0-9]{64}\Z')


def rows(archive, table):
    with gzip.open(archive / (table + '.csv.gz'), 'rt', newline='') as source:
        yield from csv.DictReader(source)


def identity(row):
    return row['time'], row['packet_hash'], row['rx_node_id']


def observer(key):
    return hashlib.sha256(f'ukmesh-dev-archive:{key.upper()}'.encode()).hexdigest().upper()


def epoch(value):
    return round(datetime.fromisoformat(value).timestamp() * 1000)


def allowed_wire(value, kind):
    try:
        raw = bytes.fromhex(value)
    except ValueError:
        return False
    if len(raw) < 3 or raw[0] & 3 != 1 or (raw[0] >> 2) & 15 != kind:
        return False
    if kind != 5:
        return True
    body = raw[2 + (raw[1] & 63) * (1 + (raw[1] >> 6)):]
    if len(body) < 19 or body[0] not in CHANNELS:
        return False
    return hmac.compare_digest(body[1:3], hmac.digest(CHANNELS[body[0]], body[3:], 'sha256')[:2])


def packet_events(archive):
    raw_frames = {identity(row): row['raw_hex'] for row in rows(archive, 'raw_packets')
                  if row['packet_type'] in PUBLIC_TYPES and row['route_type'] == '1'}
    for row in rows(archive, 'packets'):
        raw = raw_frames.get(identity(row))
        if (not raw or row['network'] != 'ukmesh' or not IATA.fullmatch(row['iata'])
                or not KEY.fullmatch(row['rx_node_id']) or not allowed_wire(raw, int(row['packet_type']))):
            continue
        key = observer(row['rx_node_id'])
        payload = {'type': 'PACKET', 'direction': 'rx', 'raw': raw, 'origin_id': key}
        for field, name in (('rssi', 'RSSI'), ('snr', 'SNR')):
            if row[field]:
                payload[name] = float(row[field])
        yield epoch(row['time']), f'meshcore/{row["iata"]}/{key}/packets', payload, row['packet_type']


def status_events(archive):
    areas = {row['node_id'].upper(): row['iata'] for row in rows(archive, 'nodes')
             if IATA.fullmatch(row['iata'])}
    for row in rows(archive, 'node_status_samples'):
        key = row['node_id']
        area = areas.get(key.upper())
        if row['network'] != 'ukmesh' or not area or not KEY.fullmatch(key):
            continue
        stats = {field: float(row[field]) for field in STATUS_FIELDS if row[field]}
        alias = observer(key)
        payload = {'status': row['status'] or 'online', 'stats': stats,
                   'name': f'Archive observer {area} {alias[:4]}', 'origin_id': alias}
        yield epoch(row['time']), f'meshcore/{area}/{alias}/status', payload, 'status'


def prepare(archive, output):
    counts = {}
    with gzip.open(output, 'wt', encoding='utf-8') as destination:
        packets = sorted(packet_events(archive), key=lambda event: event[0])
        status = sorted(status_events(archive), key=lambda event: event[0])
        for at, topic, payload, kind in merge(packets, status, key=lambda event: event[0]):
            counts[kind] = counts.get(kind, 0) + 1
            destination.write(json.dumps([at, topic, payload], separators=(',', ':')) + '\n')
    if not all(counts.get(kind) for kind in (*PUBLIC_TYPES, 'status')):
        raise ValueError('archive lacks supported packet or status inputs')
    return counts
