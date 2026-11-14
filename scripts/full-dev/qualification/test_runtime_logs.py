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


if __name__ == '__main__':
    unittest.main()
