"""Offline coverage for the interrupted-fixture entry point."""

from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import fixture_resume
import fixture_session
from common import read_json


class InterruptedFixture(unittest.TestCase):
    def prepare(self, directory, *, foreign_volume=False, publisher=False, network=''):
        chosen = {'token': 'owned-10x', 'targets': {'ukmesh': 'owned'}}
        values = {'UKMESH_DATABASE': 'owned'}
        consumer = 'ukmesh-full-backend-1'
        refs = {'ukmesh': 'owned'}
        state = {'selection': chosen, 'values': values}
        intent = {'status': 'applied', 'requested_selection': chosen, 'requested_values': values,
                  'applied_consumers': {consumer: {'refs': refs, 'container_id': 'backend'}}}
        before = {}
        after = {}
        for name, (project, service, target, volume) in fixture_resume.INFRASTRUCTURE.items():
            before[name] = {
                'Id': name, 'Config': {'Labels': {'com.docker.compose.project': project,
                                                 'com.docker.compose.service': service}},
                'HostConfig': {'PortBindings': {}},
                'Mounts': [{'Type': 'volume', 'Destination': target, 'Name': volume}],
                'State': {'Running': False},
            }
            after[name] = {**before[name], 'State': {'Running': True, 'Health': {'Status': 'healthy'}}}
        before[consumer] = {'Id': 'backend', 'State': {'Restarting': True}}
        if foreign_volume:
            before['ukmesh-sample-db']['Mounts'][0]['Name'] = 'foreign-volume'
        before['ukmesh-sample-db']['HostConfig']['NetworkMode'] = network
        stop = ValueError('publisher is still active') if publisher else None
        with patch.object(fixture_resume, 'selection_files', return_value=state), \
             patch.object(fixture_resume, 'read_intent', return_value=intent), \
             patch.object(fixture_resume, 'assert_no_publishers', side_effect=stop), \
             patch.object(fixture_resume, 'describe_consumer', return_value={'refs': refs}), \
             patch.object(fixture_resume, 'inspect_containers', side_effect=[before, after]), \
             patch.object(fixture_resume, 'docker') as docker:
            if foreign_volume or publisher or network:
                with self.assertRaisesRegex(ValueError, 'infrastructure identity|publisher'):
                    fixture_resume.prepare_applied_fixture(directory)
                docker.assert_not_called()
            else:
                self.assertEqual(fixture_resume.prepare_applied_fixture(directory), 'owned-10x')
                self.assertEqual([call.args[0] for call in docker.call_args_list],
                                 [['stop', 'backend'], ['start', *fixture_resume.INFRASTRUCTURE]])
                self.assertEqual(read_json(directory / 'infrastructure-resume.json')['fixture'], 'owned-10x')

    def test_stopped_infrastructure_reaches_health_before_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(Path(directory))

    def test_recovery_refuses_foreign_mounts_and_active_publishers(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(Path(directory), foreign_volume=True)
            self.prepare(Path(directory), publisher=True)
            self.prepare(Path(directory), network='host')
            self.prepare(Path(directory), network='container:foreign')

    def test_failed_fixture_start_is_recorded_without_a_success_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            with patch.object(fixture_session, 'restore_lease'), \
                 patch.object(fixture_resume, 'prepare_applied_fixture', return_value='owned-10x'), \
                 patch.object(fixture_session, 'restore', side_effect=RuntimeError('fixture web OOM')) as restore:
                with self.assertRaisesRegex(RuntimeError, 'fixture web OOM'):
                    fixture_session.restore_current(run)
            restore.assert_called_once_with(run, 'owned-10x')
            self.assertFalse(read_json(run / 'interrupted-recovery.json')['restored'])

    def test_real_cancellation_keeps_the_recovery_parent_until_owned_child_finishes(self):
        code = """
from pathlib import Path
from unittest.mock import patch
import sys
import fixture_resume, fixture_session
from owned_process import run_session
from target_lease import target_lease

root = Path(sys.argv[1])
def restore(run, token):
    print('restoring', flush=True)
    run_session([sys.executable, '-c', 'import time; time.sleep(.4)'], cancellation={'signal': None})
    (root / 'finished').touch()

with patch.object(fixture_session, 'restore_lease', lambda: target_lease(exclusive=True, path=root/'lease')), \
     patch.object(fixture_resume, 'prepare_applied_fixture', return_value='owned-10x'), \
     patch.object(fixture_session, 'restore', side_effect=restore):
    try:
        fixture_session.restore_current(root)
    except InterruptedError:
        sys.exit(130)
"""
        for cancellation in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(cancellation=cancellation), tempfile.TemporaryDirectory() as directory:
                with subprocess.Popen(
                    [sys.executable, '-c', code, directory],
                    cwd=Path(__file__).parent, stdout=subprocess.PIPE, text=True,
                ) as child:
                    self.assertEqual(child.stdout.readline().strip(), 'restoring')
                    child.send_signal(cancellation)
                    self.assertEqual(child.wait(timeout=5), 130)
                root = Path(directory)
                self.assertTrue((root / 'finished').is_file())
                receipt = read_json(root / 'interrupted-recovery.json')
                self.assertTrue(receipt['restored'])
                self.assertEqual(receipt['cancellation'], cancellation.name)
