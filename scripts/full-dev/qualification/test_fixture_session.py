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
from unittest.mock import call, patch

from fixture_session import restore_lease
from owned_process import cancellation_scope, run_session
from target_lease import child_context, target_lease
from target_observation import IntakeNotQuiescentError, IntakeProof
from target_safety import InactiveChangingConsumerError
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

    def test_restore_resumes_the_owned_store_after_an_inactive_consumer_refusal(self):
        token = 'owned-10x'
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            original = {'targets': 'original'}
            selected = {'targets': {}, 'token': token}
            actions = []
            resumed = False

            def select_originals():
                nonlocal selected
                actions.append('select originals')
                if not resumed:
                    raise InactiveChangingConsumerError('inactive fixture consumer')
                selected = original

            def controller(script, args, recovery):
                nonlocal resumed
                self.assertIsNone(recovery['signal'])
                self.assertFalse((evidence / 'resume-fixture.json').exists())
                if args == ['start']:
                    actions.append('start ' + ('originals' if selected == original else 'fixture'))
                    resumed = True
                else:
                    self.assertEqual(selected, original)
                    self.assertEqual(args, ['inventory', '--run', str(evidence / 'restored')])
                    actions.append('inventory')
                return 0

            with patch.object(fixture_session, 'target_lease',
                              side_effect=lambda **kw: target_lease(path=evidence / 'lease', **kw)), \
                 patch.object(fixture_session, 'read_intent', return_value={'status': 'applied'}), \
                 patch.object(fixture_session, 'selection_files', side_effect=lambda: {'selection': selected}), \
                 patch.object(fixture_session, 'selection_values', side_effect=lambda: selected), \
                 patch.object(fixture_session, 'original_values', return_value=original), \
                 patch.object(fixture_session, 'restore_originals', side_effect=select_originals) as select, \
                 patch.object(fixture_session, 'run_controller', side_effect=controller):
                fixture_session.restore(evidence, token)
            self.assertEqual(select.call_args_list, [call(), call()])
            self.assertEqual(actions, ['select originals', 'start fixture', 'select originals', 'start originals', 'inventory'])
            receipt = json.loads((evidence / 'resume-fixture.json').read_text())
            self.assertEqual(receipt['fixture'], token)

    def test_restore_retries_through_recent_intake_then_requires_fresh_proof(self):
        pending = IntakeNotQuiescentError('ukmesh intake is not quiescent')
        with patch.object(fixture_session, 'restore_originals', side_effect=[pending, None]) as restore, \
             patch.object(fixture_session.time, 'monotonic', side_effect=[0, 1, 2]), \
             patch.object(fixture_session.time, 'sleep') as sleep:
            fixture_session.restore_when_quiescent()
        self.assertEqual(restore.call_args_list, [call(), call()])
        sleep.assert_called_once_with(1)

    def test_restore_keeps_deadline_and_refuses_unrelated_failures(self):
        for failure in (IntakeNotQuiescentError('pending'), ValueError('ownership changed')):
            with self.subTest(failure=failure), \
                 patch.object(fixture_session, 'restore_originals', side_effect=failure) as restore, \
                 patch.object(fixture_session.time, 'monotonic', side_effect=[0, 120]), \
                 patch.object(fixture_session.time, 'sleep') as sleep:
                with self.assertRaises(type(failure)) as caught:
                    fixture_session.restore_when_quiescent()
                self.assertIs(caught.exception, failure)
            restore.assert_called_once_with()
            sleep.assert_not_called()

    def test_restore_avoids_a_new_attempt_at_the_deadline(self):
        with patch.object(fixture_session, 'restore_originals', side_effect=IntakeNotQuiescentError) as restore, \
             patch.object(fixture_session.time, 'monotonic', side_effect=[0, 119.5, 120]), \
             patch.object(fixture_session.time, 'sleep') as sleep:
            with self.assertRaises(IntakeNotQuiescentError):
                fixture_session.restore_when_quiescent()
        restore.assert_called_once_with()
        sleep.assert_called_once_with(.5)

    def test_busy_intake_is_recoverable_only_before_proof(self):
        proof = IntakeProof(0, {'ukmesh'})
        with self.assertRaises(IntakeNotQuiescentError):
            proof.add({'ukmesh': {'quiescent': False}}, allow_initial_pending=True)
        with self.assertRaises(ValueError) as malformed:
            proof.add({'ukmesh': {}}, allow_initial_pending=True)
        self.assertIs(type(malformed.exception), ValueError)
        proof.add({'ukmesh': {'quiescent': True, 'probe_last_success': 1}}, allow_initial_pending=True)
        with self.assertRaises(ValueError) as busy:
            proof.add({'ukmesh': {'quiescent': False}}, allow_initial_pending=True)
        self.assertIs(type(busy.exception), ValueError)

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
                     patch.object(fixture_session.shutil, 'disk_usage', return_value=type('Disk', (), {'free': 12 * 2**30})()), \
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
