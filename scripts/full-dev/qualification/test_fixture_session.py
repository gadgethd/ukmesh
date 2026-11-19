"""Real owned process and lease checks on temporary files."""
import json
from pathlib import Path
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from fixture_session import restore_lease
from owned_process import cancellation_scope, run_session
from target_lease import child_context, target_lease
import fixture_session
import read_transport


class FixtureSession(unittest.TestCase):
    def test_disk_interrupt_settles_only_owned_session_and_releases_lease(self):
        # Fast escalation keeps the test focused on real process/FD semantics.
        from owned_process import settle_session
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock, ready = root / 'lease', root / 'ready'
            inner = """import os,signal,sys,time
from pathlib import Path
signal.signal(signal.SIGINT, signal.SIG_IGN)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(30)
"""
            outer = """import os,signal,subprocess,sys,time
signal.signal(signal.SIGINT, signal.SIG_IGN)
fd=int(os.environ['UKMESH_TARGET_LEASE_FD'])
subprocess.Popen([sys.executable,'-c',sys.argv[2],sys.argv[1]],pass_fds=(fd,))
time.sleep(30)
"""
            foreign = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], start_new_session=True)
            try:
                began = time.monotonic()
                def cutoff():
                    if ready.exists() or time.monotonic() - began >= 3:
                        return 'fixture disk reserve reached'
                    return None
                with target_lease(path=lock), cancellation_scope('test') as cancellation:
                    inherited, descriptors = child_context(path=lock)
                    with patch('owned_process.settle_session',
                               side_effect=lambda child,ticks,**_: settle_session(child,ticks,graceful_interrupt=.1)):
                        with self.assertRaisesRegex(InterruptedError, 'disk reserve'):
                            run_session([sys.executable, '-c', outer, str(ready), inner],
                                        cancellation=cancellation, stop_reason=cutoff,
                                        env={**os.environ, **inherited}, pass_fds=descriptors)
                self.assertTrue(ready.is_file())
                with target_lease(exclusive=True, path=lock):
                    self.assertIsNone(foreign.poll())
            finally:
                foreign.kill()
                foreign.wait(timeout=3)

    def test_restore_waits_for_a_real_shared_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / 'lease'
            code = """from pathlib import Path
import sys,time
from target_lease import target_lease
with target_lease(path=Path(sys.argv[1])):
 print('ready',flush=True)
 time.sleep(.45)
"""
            env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parent)}
            with subprocess.Popen([sys.executable, '-c', code, str(lock)], env=env,
                                  stdout=subprocess.PIPE, text=True) as child:
                self.assertEqual(child.stdout.readline().strip(), 'ready')
                first = time.monotonic()
                with patch.object(fixture_session,'target_lease',
                                  side_effect=lambda **kw: target_lease(path=lock, **kw)):
                    with restore_lease():
                        self.assertGreater(time.monotonic() - first, .25)

    def test_reader_recovery_uses_run_labels_and_immutable_container_id(self):
        with tempfile.TemporaryDirectory() as folder:
            run = Path(folder)
            directory = run / 'http-reader'
            directory.mkdir()
            name = 'ukmesh-owned-http-reader-123456abcdef'
            alias = Path(tempfile.gettempdir()) / name
            with (directory / 'owner.json').open('w') as owner:
                json.dump({'name': name, 'started': True}, owner)
            item = {'Id': 'id123456','Name':'/' + name,'Config':{'Labels':{
                'ukmesh.owned-http-reader': name, 'ukmesh.http-reader.alias':str(alias),
                'ukmesh.http-reader.directory':str(directory)}},
                'Mounts':[{'Source':str(directory),'Destination':'/reader'}]}
            present = True
            def docker(args, **_):
                nonlocal present
                if args[0] == 'ps':
                    value = b'id123\n' if present else b''
                elif args[0] == 'inspect':
                    value = json.dumps([item]).encode()
                else:
                    self.assertEqual(args, ['rm','-f','id123456'])
                    present = False
                    value = b''
                return subprocess.CompletedProcess(args,0,value,b'')
            with patch.object(read_transport,'docker',side_effect=docker) as client:
                item['Mounts'][0]['Source'] = '/foreign'
                with self.assertRaisesRegex(ValueError, 'ownership differs'):
                    read_transport.recover_reader(run)
                self.assertTrue(present)
                item['Mounts'][0]['Source'] = str(directory)
                read_transport.recover_reader(run)
                read_transport.recover_reader(run)
                self.assertEqual(sum(call.args[0][0]=='rm' for call in client.mock_calls), 1)

    def test_interruption_defers_until_canonical_start_then_restores(self):
        token = 'owned-10x'
        for failure in ('none', 'transient', 'persistent'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                lock = root / 'lease'
                initial = {'targets': 'original'}
                state = {'selection': initial, 'intent': None}
                actions = []
                fixture_starts = 0

                def select_fixture(received):
                    self.assertEqual(received, token)
                    marker = {'targets': {}, 'token': token}
                    state.update(intent={'status': 'pending', 'requested_selection': marker}, selection=marker)

                def select_original():
                    if state['intent'] and state['intent']['status'] == 'pending':
                        self.fail('mandatory pending fixture operation was skipped')
                    state.update(selection=initial, intent={'status': 'pending', 'requested_selection': initial})

                def launch(script, args, cancellation, **options):
                    nonlocal fixture_starts
                    actions.append(args[0])
                    self.assertIsNone(cancellation['signal'])
                    if args[0] == 'start':
                        self.assertEqual(state['intent']['status'], 'pending')
                        if state['selection'] != initial:
                            fixture_starts += 1
                            if fixture_starts == 1:
                                os.kill(os.getpid(), signal.SIGINT)
                            if failure == 'persistent' or (failure == 'transient' and fixture_starts == 1):
                                return 1
                        state['intent']['status'] = 'applied'
                    return 0

                with patch.multiple(fixture_session, STATE=root), \
                     patch.object(fixture_session, 'read_intent', side_effect=lambda: state['intent']), \
                     patch.object(fixture_session, 'selection_files', side_effect=lambda: {'selection': state['selection']}), \
                     patch.object(fixture_session, 'selection_values', side_effect=lambda: state['selection']), \
                     patch.object(fixture_session, 'original_values', return_value=initial), \
                     patch.object(fixture_session, 'bundle', return_value={'kind': '10x'}), \
                     patch.object(fixture_session, 'switch', side_effect=select_fixture), \
                     patch.object(fixture_session, 'restore_originals', side_effect=select_original), \
                     patch.object(fixture_session, 'run_controller', side_effect=launch), \
                     patch.object(fixture_session, 'target_lease', side_effect=lambda **kw: target_lease(path=lock, **kw)):
                    expected = InterruptedError if failure == 'none' else RuntimeError
                    with self.assertRaises(expected):
                        fixture_session.qualify(token, root, ['reads', '--paired'])
                receipts = list(root.glob('fixture-sessions/*/fixture-session.json'))
                self.assertEqual(len(receipts), 1)
                receipt = json.loads(receipts[0].read_text())
                self.assertEqual(receipt['cancellation'], 'SIGINT')
                if failure == 'persistent':
                    self.assertFalse(receipt['restored'])
                    self.assertIn('could not be resumed', receipt['error'])
                    self.assertEqual(actions, ['start', 'start'])
                else:
                    self.assertTrue(receipt['restored'])
                    self.assertEqual(state['selection'], initial)
                    self.assertEqual(actions, ['start'] * (fixture_starts + 1) + ['inventory'])


if __name__=='__main__':
    unittest.main()
