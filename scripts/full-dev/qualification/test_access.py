"""Reader write proofs must establish identity, absent ACLs and permission denial."""

import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import access


def metadata():
    return {'current_user': 'corescope_ro', 'session_user': 'corescope_ro', 'database': 'lean_10x_owned',
            'relations': [
                {'oid': 118683, 'schema': 'corescope_public', 'name': 'packets', 'relkind': 'v',
                 'table_insert': False, 'any_column_insert': False},
                {'oid': 35630, 'schema': 'mesh', 'name': 'packets', 'relkind': 'r',
                 'table_insert': False, 'any_column_insert': False}]}


class ReaderWriteAccess(unittest.TestCase):
    def checks(self, value=None, *, state='42501', status=3):
        value = metadata() if value is None else value
        completed = subprocess.CompletedProcess([], status, json.dumps(value).encode(),
                                                ('ERROR: ' + state + '\n').encode())
        with patch.object(access, 'docker', return_value=completed) as called:
            records = access.reader_write_checks('owned-db', 'lean_10x_owned', 'corescope_ro',
                                                'corescope_public', host='172.18.0.2')
        return records, called.call_args

    def test_correct_identity_absent_effective_acls_and_exact_denial_pass(self):
        records, invocation = self.checks()
        self.assertTrue(all(row['passed'] for row in records))
        self.assertEqual(records[1]['sqlstate'], '42501')
        sql = invocation.kwargs['data'].decode()
        self.assertIn('BEGIN;', sql)
        self.assertIn('INSERT INTO corescope_public.packets DEFAULT VALUES', sql)
        self.assertIn('ROLLBACK;', sql)
        self.assertNotIn('INSERT INTO corescope_public.packets SELECT *', sql)
        self.assertEqual(records[0]['metadata']['relations'][0]['oid'], 118683)

    def test_table_or_column_insert_grant_fails_even_when_write_reports_permission_denial(self):
        for relation in (0, 1):
            for privilege in ('table_insert', 'any_column_insert'):
                value = metadata()
                value['relations'][relation][privilege] = True
                with self.subTest(relation=relation, privilege=privilege):
                    records, _ = self.checks(value)
                    self.assertTrue(all(not row['passed'] for row in records))

    def test_unsupported_operation_auth_failure_and_success_cannot_prove_write_denial(self):
        for state, status in [('0A000', 3), ('28P01', 2), ('23502', 3), ('42501', 0)]:
            with self.subTest(state=state, status=status):
                records, _ = self.checks(state=state, status=status)
                self.assertFalse(records[1]['passed'])

    def test_wrong_session_database_relation_or_oid_fails_both_proofs(self):
        mutations = [lambda m: m.update(current_user='meshcore'),
                     lambda m: m.update(session_user='meshcore'),
                     lambda m: m.update(database='another_store'),
                     lambda m: m['relations'][0].update(schema='public'),
                     lambda m: m['relations'][1].update(name='another_table'),
                     lambda m: m['relations'][0].update(oid=0),
                     lambda m: m['relations'][0].update(oid=m['relations'][1]['oid']),
                     lambda m: m['relations'][0].update(relkind='r')]
        for mutate in mutations:
            value = metadata()
            mutate(value)
            records, _ = self.checks(value)
            self.assertTrue(all(not row['passed'] for row in records))

    def test_missing_relations_malformed_identity_and_nonboolean_acl_fail_closed(self):
        for mutate in [lambda m: m['relations'].pop(),
                       lambda m: m['relations'][0].update(schema=[]),
                       lambda m: m['relations'][0].pop('any_column_insert'),
                       lambda m: m['relations'][0].update(table_insert=0)]:
            value = metadata()
            mutate(value)
            records, _ = self.checks(value)
            self.assertTrue(all(not row['passed'] for row in records))

    def test_malformed_metadata_is_retained_as_failure_without_raw_output(self):
        completed = subprocess.CompletedProcess([], 3, b'private malformed payload', b'ERROR: 42501\n')
        with patch.object(access, 'docker', return_value=completed):
            records = access.reader_write_checks('owned-db', 'lean_10x_owned', 'corescope_ro',
                                                'corescope_public', host='172.18.0.2')
        self.assertTrue(all(not row['passed'] for row in records))
        self.assertNotIn('private malformed payload', json.dumps(records))


if __name__ == '__main__':
    unittest.main()
