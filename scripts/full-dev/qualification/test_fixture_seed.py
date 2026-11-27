import csv
import gzip
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import fixture_seed
from fixture_seed import FILES, OBSERVER, TABLES, TOPIC, classify_window, seed_rows
from common import digest


class FixtureSeedTests(unittest.TestCase):
    def synthetic_source(self, source: Path) -> dict:
        payload = bytes.fromhex('00000011')
        hash_ = hashlib.sha256(b'\x0c' + payload).hexdigest().upper()
        base = {'time': '2026-11-01T00:00:00Z', 'packet_hash': hash_, 'rx_node_id': OBSERVER}
        rows = (
            {**base, 'network': 'ukmesh', 'packet_type': '3', 'topic': TOPIC},
            {**base, 'raw_hex': '0D42AABBCCDD' + payload.hex()},
        )
        for name, row in zip(FILES, rows, strict=True):
            with gzip.open(source / name, 'wt', newline='') as stream:
                writer = csv.DictWriter(stream, list(row))
                writer.writeheader()
                writer.writerow(row)
        manifest = {'synthetic': True, 'seed': 'sequential-ack-v1', 'sequenceStart': 17, 'count': 1,
                    'checksums': {name: digest(source / name) for name in FILES}}
        (source / 'fixture.json').write_text(json.dumps(manifest))
        return manifest

    def test_seed_matches_both_wire_identity_schemes_and_verifies_input_files(self):
        with TemporaryDirectory() as directory:
            source = Path(directory)
            manifest = self.synthetic_source(source)
            [(ukmesh, radio, data)] = seed_rows(source, manifest)
            self.assertNotEqual(radio, ukmesh[:16])
            self.assertEqual(radio, hashlib.sha256(bytes.fromhex('0300000011')).hexdigest()[:16])
            self.assertEqual(data['row']['packet_hash'], ukmesh.upper())
            with patch.object(fixture_seed, 'EVIDENCE', source):
                self.assertEqual(fixture_seed.seed_source(source)[1], manifest)
                with (source / FILES[0]).open('ab') as stream:
                    stream.write(b'changed')
                with self.assertRaisesRegex(ValueError, 'checksums'):
                    fixture_seed.seed_source(source)

    def test_resume_never_treats_partial_or_duplicate_consumption_as_missing(self):
        identities = {'first': 'a', 'second': 'b', 'third': 'c', 'fourth': 'd'}
        counts = {table: {
            ('first' if table.startswith('ukmesh') else 'a'): 1,
            ('fourth' if table.startswith('ukmesh') else 'd'): 1,
        } for table in TABLES}
        counts['beacon_packets']['b'] = 1
        counts['ukmesh_ledger']['fourth'] = 2
        self.assertEqual(classify_window(identities, counts), (['third'], ['second', 'fourth']))

    def test_checkpoint_follows_persistence_instead_of_broker_ack(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()
            self.synthetic_source(source)

            def observe(fixture, identities, *, until_complete):
                return (list(identities), [])

            with patch.object(fixture_seed, 'EVIDENCE', root), \
                    patch.object(fixture_seed, 'observe', side_effect=observe), \
                    patch.object(fixture_seed.shutil, 'disk_usage', return_value=type('Disk', (), {'free': 12 * 2**30})()), \
                    patch.object(fixture_seed, 'publish') as publish:
                with self.assertRaisesRegex(RuntimeError, 'unreconciled persistence'):
                    fixture_seed.replay_seed(root, source, {'token': 'fixture'})
                publish.assert_called_once()
                self.assertFalse((root / 'seed-replay.json').exists())


if __name__ == '__main__':
    unittest.main()
