"""Failures from all application languages remain visible without publishing logs."""
from pathlib import Path
import datetime as dt
import tempfile
import unittest
from common import command
from runtime_logs import failure_line, inspect_log, validate_window


class RuntimeErrors(unittest.TestCase):
    def test_quoted_messages_and_structured_failure_flags_cannot_hide_errors(self):
        for line in ('{"failed":true}', 'level=info msg="failed to persist the batch"',
                     'level=warn err="database offline"', 'level=warn error="connection lost"',
                     '[ERROR] connection terminated unexpectedly',
                     '{"event":"packet_write_failure","reason":"database offline"}',
                     '2026-11-14T01:00:00Z {"level":"info","msg":"batch failed"}',
                     '{"error":{"message":"database offline"}}'):
            with self.subTest(line=line):
                self.assertTrue(failure_line(line))
        for line in ('{"failed":false,"error":null}', 'level=info failed=0',
                     'level=info err="" error=null', 'level=info err=<nil> error=false',
                     '{"error":"","level":"info","msg":"committed"}'):
            with self.subTest(line=line):
                self.assertFalse(failure_line(line))

    def test_capture_requires_an_aware_ordered_completed_window(self):
        now = dt.datetime.now(dt.timezone.utc)
        validate_window((now-dt.timedelta(seconds=2)).isoformat(), (now-dt.timedelta(seconds=1)).isoformat())
        for start,end in (('', ''), ('2026-01-01', '2026-01-02'),
                          (now.isoformat(), (now-dt.timedelta(seconds=1)).isoformat()),
                          (now.isoformat(), (now+dt.timedelta(days=1)).isoformat())):
            with self.subTest(start=start):
                with self.assertRaises((ValueError,TypeError)):
                    validate_window(start,end)

    def test_both_process_streams_are_retained_directly_in_the_private_file(self):
        with tempfile.TemporaryFile() as stream:
            result = command(['python3','-c','import sys; print("stdout"); print("stderr",file=sys.stderr); sys.exit(7)'],
                             output_stream=stream, merge_stderr=True, check=False)
            self.assertEqual(result.returncode,7)
            self.assertIsNone(result.stderr)
            stream.seek(0)
            self.assertEqual(set(stream.read().splitlines()), {b'stdout',b'stderr'})

    def test_structured_and_plain_failures_are_detected_with_private_line_references(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'worker.log'
            log.write_text('level=INFO msg="request complete" status=200\n'
                           '{"error":null,"failed":0}\n'
                           'failed=0\n'
                           'level=ERROR msg="worker stopped"\n'
                           'UnhandledPromiseRejection: database offline\n'
                           'ERROR: permission denied for table packets\n'
                           '[link-worker] job failed: database offline\n'
                           'timeout exceeded when trying to connect\n')
            result = inspect_log(log)
            self.assertEqual(result['failure_lines'], 5)
            self.assertEqual(result['example_line_numbers'], [4,5,6,7,8])
            self.assertNotIn('database offline', str(result))

    def test_tab_indented_application_errors_remain_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'backend.log'
            log.write_text('2026-11-23T12:00:00Z \tERROR: database offline\n')
            self.assertEqual(inspect_log(log)['example_line_numbers'], [1])

    def test_postgresql_multiline_sql_dumps_are_data_not_new_failures(self):
        def record(severity, message):
            return ('2026-11-19T09:06:39.785605000Z 2026-11-19 09:06:39.771 UTC [835438] '
                    f'{severity}:  {message}')
        dump = [record('STATEMENT', 'WITH logical_messages AS ('),
                "\t       SELECT DISTINCT ON (p.packet_hash) p.packet_hash",
                "\t        WHERE status = ANY ('{pending,processing,failed}')",
                "\t          AND last_error IS NOT NULL -- exception panic failure",
                "\t      LIMIT $6"]
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'sample-db.log'
            # The severity record stays the single failure; the echoed SQL and
            # its continuation lines never add failures of their own.
            log.write_text('\n'.join([record('FATAL', 'connection to client lost')] + dump) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [1])
            # ERROR with DETAIL bind values and a STATEMENT echo counts once.
            log.write_text('\n'.join([
                record('ERROR', 'duplicate key value violates unique constraint "packets_pkey"'),
                record('DETAIL', 'Key (packet_hash)=(failed) already exists.'),
                record('STATEMENT', "INSERT INTO mesh.packets VALUES ('exception', 'panic')"),
            ]) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [1])
            # A dump without any severity record is pure echo: zero failures.
            log.write_text('\n'.join(dump) + '\n')
            self.assertEqual(inspect_log(log, postgres=True)['failure_lines'], 0)
            # A later genuine record ends the dump and is counted again.
            log.write_text('\n'.join(dump + [record('ERROR', 'permission denied for table packets')]) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [6])
            # Non-dump PostgreSQL records keep the ordinary message scan.
            log.write_text(record('LOG', 'background worker failed to start') + '\n')
            self.assertEqual(inspect_log(log, postgres=True)['failure_lines'], 1)

    def test_dump_boundaries_keep_neighbouring_failures_visible(self):
        def record(severity, message, gap='  '):
            return ('2026-11-19T09:06:39.785605000Z 2026-11-19 09:06:39.771 UTC [835438] '
                    f'{severity}:{gap}{message}')
        dump = [record('STATEMENT', "SELECT status FROM jobs WHERE status = 'failed'"),
                "\t       AND last_error IS NOT NULL -- exception panic failure"]
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'sample-db.log'
            # A non-PostgreSQL failure right after a dump is NOT swallowed.
            log.write_text('\n'.join(dump + ['[link-worker] job failed: database offline']) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [3])
            # Structured application failures after a dump stay visible too.
            log.write_text('\n'.join(dump + ['{"level":"error","msg":"commit failed"}']) + '\n')
            self.assertEqual(inspect_log(log, postgres=True)['failure_lines'], 1)
            # A window opened mid-dump: orphaned tab continuations are echo
            # data even without their owning record; the later FATAL counts.
            log.write_text('\n'.join(["\t WHERE status = 'failed' -- panic exception",
                                      record('FATAL', 'connection to client lost')]) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [2])
            # A tab-indented continuation that looks like a record is still
            # echo data, not a new failure.
            log.write_text('\n'.join([record('ERROR', 'permission denied for table packets')] + dump
                                     + ["\tERROR:  fake failure inside echoed SQL"]) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [1])
            # Single-space severity gaps and records without the docker prefix
            # keep the same classification.
            log.write_text(record('FATAL', 'connection to client lost', gap=' ') + '\n')
            self.assertEqual(inspect_log(log, postgres=True)['failure_lines'], 1)
            bare = '2026-11-19 09:06:39.771 UTC [835438] '
            log.write_text(bare + 'STATEMENT:  SELECT 1\n' + bare + 'FATAL:  connection to client lost\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [2])
            # HINT/CONTEXT/LOCATION echoes are data; WARNING messages keep the
            # ordinary scan (pre-existing behavior).
            log.write_text('\n'.join([
                record('ERROR', 'duplicate key value violates unique constraint'),
                record('HINT', 'failed insert may be retried'),
                record('CONTEXT', 'SQL statement "SELECT failed"'),
                record('LOCATION', 'exec_stmt, pl_exec.c:1'),
            ]) + '\n')
            result = inspect_log(log, postgres=True)
            self.assertEqual(result['failure_lines'], 1)
            self.assertEqual(result['example_line_numbers'], [1])
            log.write_text(record('WARNING', 'background worker failed to start') + '\n')
            self.assertEqual(inspect_log(log, postgres=True)['failure_lines'], 1)

    def test_saved_soak_lost_client_input_counts_only_the_six_fatal_records(self):
        saved = Path(__file__).parent / 'fixtures' / 'runtime-logs' / 'soak-spam-lost-clients.log.txt'
        result = inspect_log(saved, postgres=True)
        # Exact excerpt of the retained 10x soak sample-db log: six genuine
        # FATAL "connection to client lost" records, each followed by the
        # multiline STATEMENT dump whose SQL literals contain 'failed'.
        self.assertEqual(result['failure_lines'], 6)
        self.assertEqual(result['example_line_numbers'], [1, 87, 173, 259, 345, 431])
        # Skipped dump lines still contribute to the retained-file digest.
        import hashlib
        self.assertEqual(result['sha256'], hashlib.sha256(saved.read_bytes()).hexdigest())


if __name__ == '__main__':
    unittest.main()
