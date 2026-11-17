import importlib.util
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("mosquitto-reloader.py")
SPEC = importlib.util.spec_from_file_location("mosquitto_reloader", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
reloader = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reloader)


class ReloadAcknowledgementTests(unittest.TestCase):
    def test_waits_for_marker_appended_after_cursor(self) -> None:
        with tempfile.NamedTemporaryFile() as broker_log:
            broker_log.write(b"old Reloading config.\n")
            broker_log.flush()
            cursor = reloader.capture_log_cursor(broker_log.name)

            def append_ack() -> None:
                time.sleep(0.02)
                with open(broker_log.name, "ab") as output:
                    output.write(b"new Reloading config.\n")

            writer = threading.Thread(target=append_ack)
            writer.start()
            self.assertTrue(reloader.wait_for_reload_ack(cursor, 0.5, broker_log.name))
            writer.join()

    def test_rejects_stale_marker(self) -> None:
        with tempfile.NamedTemporaryFile() as broker_log:
            broker_log.write(b"old Reloading config.\n")
            broker_log.flush()
            cursor = reloader.capture_log_cursor(broker_log.name)
            self.assertFalse(reloader.wait_for_reload_ack(cursor, 0.02, broker_log.name))

    def test_default_transport_still_signals_pid_one(self) -> None:
        with tempfile.NamedTemporaryFile() as log, mock.patch.object(reloader, 'RELOAD_FIFO', ''), \
             mock.patch.object(reloader.os, 'kill') as send, \
             mock.patch.object(reloader, 'wait_for_reload_ack', return_value=True):
            self.assertTrue(reloader.signal_and_wait_for_reload(log.name))
            send.assert_called_once_with(1, signal.SIGHUP)


class FifoTransportTests(unittest.TestCase):
    def test_fifo_must_be_private_owned_and_have_a_live_reader(self):
        with tempfile.TemporaryDirectory() as directory:
            fifo = Path(directory) / 'reload'
            os.mkfifo(fifo, 0o600)
            started = time.monotonic()
            with self.assertRaises(OSError):
                reloader.open_reload_fifo(str(fifo))
            self.assertLess(time.monotonic()-started, .5, 'missing broker must not block HTTP threads')
            reader = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)
            try:
                writer = reloader.open_reload_fifo(str(fifo))
                try: os.write(writer, b'reload\n')
                finally: os.close(writer)
                self.assertEqual(os.read(reader, 7), b'reload\n')
                fifo.chmod(0o666)
                with self.assertRaises(RuntimeError): reloader.open_reload_fifo(str(fifo))
                fifo.chmod(0o600)
                with mock.patch.object(reloader.os, 'getuid', return_value=os.getuid()+1):
                    with self.assertRaises(RuntimeError): reloader.open_reload_fifo(str(fifo))
            finally: os.close(reader)

    def test_regular_files_and_symlinked_endpoints_or_directories_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'reload'
            target.write_text('must remain untouched')
            with self.assertRaises(RuntimeError): reloader.open_reload_fifo(str(target))
            target.unlink(); os.mkfifo(target, 0o600)
            alias = root / 'alias'; alias.symlink_to(target)
            with self.assertRaises(RuntimeError): reloader.open_reload_fifo(str(alias))
            parent_alias = root / 'directory'; parent_alias.symlink_to(root, target_is_directory=True)
            with self.assertRaises(RuntimeError): reloader.open_reload_fifo(str(parent_alias / 'reload'))

    def test_full_control_fifo_fails_without_blocking_or_claiming_acknowledgement(self):
        with tempfile.TemporaryDirectory() as directory:
            fifo=Path(directory)/'reload'; os.mkfifo(fifo,0o600)
            reader=os.open(fifo,os.O_RDWR|os.O_NONBLOCK)
            try:
                while True:
                    try: os.write(reader,b'x'*4096)
                    except BlockingIOError: break
                with tempfile.NamedTemporaryFile() as log, mock.patch.object(reloader,'RELOAD_FIFO',str(fifo)), \
                     mock.patch.object(reloader,'wait_for_reload_ack') as acknowledge:
                    started=time.monotonic()
                    with self.assertRaises(BlockingIOError): reloader.signal_and_wait_for_reload(log.name)
                    self.assertLess(time.monotonic()-started,.5)
                    acknowledge.assert_not_called()
            finally: os.close(reader)


class BrokerLifecycleTests(unittest.TestCase):
    """Native child signals and real HTTP acknowledgements, without Docker."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.control = self.root / 'control'; self.control.mkdir(mode=0o700)
        self.log = self.root / 'broker.log'; self.log.touch()
        self.ready = self.root / 'ready'
        self.broker = self.root / 'broker.py'
        self.broker.write_text('''import os, signal, sys, time
from pathlib import Path
log=Path(os.environ['BROKER_TEST_LOG'])
def hup(*_):
    with log.open('a') as output: output.write('Reloading config.\\n')
signal.signal(signal.SIGHUP,hup)
signal.signal(signal.SIGTERM,lambda *_: sys.exit(17))
signal.signal(signal.SIGINT,lambda *_: sys.exit(19))
Path(os.environ['BROKER_TEST_READY']).write_text(str(os.getpid()))
if os.environ.get('BROKER_TEST_EXIT'): time.sleep(.1); sys.exit(int(os.environ['BROKER_TEST_EXIT']))
while True: time.sleep(.05)
''')
        self.wrapper = MODULE_PATH.parent.parent / 'scripts/local-sample/mosquitto-lifecycle.sh'
        self.processes = []
        self.addCleanup(self.stop_all)

    def stop_all(self):
        for process in self.processes:
            if process.poll() is None: process.terminate()
            try: process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid,signal.SIGKILL); process.wait()
                self.fail('broker wrapper failed to reap its children')
            finally:
                try: os.killpg(process.pid,signal.SIGKILL)
                except ProcessLookupError: pass

    def start(self, exit_code=''):
        self.ready.unlink(missing_ok=True)
        environment = {**os.environ, 'MOSQUITTO_RELOAD_FIFO': str(self.control / 'reload'),
            'BROKER_TEST_LOG': str(self.log), 'BROKER_TEST_READY': str(self.ready), 'BROKER_TEST_EXIT': exit_code}
        process = subprocess.Popen(['/bin/sh', str(self.wrapper), sys.executable, str(self.broker)],
                                   env=environment,start_new_session=True)
        self.processes.append(process)
        deadline = time.monotonic()+2
        while not self.ready.exists():
            if process.poll() is not None or time.monotonic()>deadline: self.fail('broker child failed to become ready')
            time.sleep(.01)
        return process, int(self.ready.read_text())

    def test_forwards_term_and_int_and_preserves_broker_exit_code(self):
        for sent, expected in [(signal.SIGTERM,17),(signal.SIGINT,19)]:
            process, child = self.start()
            listing=Path(f'/proc/{process.pid}/task/{process.pid}/children')
            children={child}|({int(value) for value in listing.read_text().split()} if listing.exists() else set())
            process.send_signal(sent)
            self.assertEqual(process.wait(timeout=3), expected)
            for pid in children:
                with self.assertRaises(ProcessLookupError): os.kill(pid,0)
        process, child = self.start('23')
        self.assertEqual(process.wait(timeout=3),23)
        with self.assertRaises(ProcessLookupError): os.kill(child,0)

    def test_same_reloader_serves_real_204_before_and_after_broker_restart(self):
        process, _ = self.start()
        patches = [mock.patch.object(reloader,'LOG_PATH',str(self.log)),
                   mock.patch.object(reloader,'RELOAD_FIFO',str(self.control/'reload')),
                   mock.patch.object(reloader,'TOKEN','n'*32),
                   mock.patch.object(reloader,'ACL_PATH',str(self.root/'absent-acl')),
                   mock.patch.object(reloader,'PASSWD_PATH',str(self.root/'absent-passwd'))]
        # signal_and_wait defaults retain their production defaults; pass the
        # real fixture log explicitly while keeping real transport/ack work.
        original = reloader.signal_and_wait_for_reload
        patches.append(mock.patch.object(reloader,'signal_and_wait_for_reload',lambda:original(str(self.log),.5)))
        for patch in patches: patch.start(); self.addCleanup(patch.stop)
        server = reloader.ThreadingHTTPServer(('127.0.0.1',0),reloader.Handler)
        thread = threading.Thread(target=server.serve_forever); thread.start()
        try:
            def reload():
                request=urllib.request.Request(f'http://127.0.0.1:{server.server_port}/reload',data=b'{}',
                    headers={'Authorization':'Bearer '+'n'*32})
                with urllib.request.urlopen(request,timeout=2) as response: return response.status
            self.assertEqual(reload(),204)
            process.terminate(); self.assertEqual(process.wait(timeout=3),17)
            with self.assertRaises(urllib.error.HTTPError) as offline: reload()
            self.assertEqual(offline.exception.code,503)
            offline.exception.close()
            self.start()
            self.assertEqual(reload(),204)
            self.assertEqual(self.log.read_text().count('Reloading config.'),2)
        finally:
            server.shutdown(); server.server_close(); thread.join()

    def test_only_the_constant_reload_command_can_signal_the_child(self):
        self.start()
        descriptor=reloader.open_reload_fifo(str(self.control/'reload'))
        try:
            os.write(descriptor,b'not-reload\n')
            os.write(descriptor,('touch '+str(self.root/'must-not-exist')+'\n').encode())
        finally: os.close(descriptor)
        time.sleep(.05)
        self.assertEqual(self.log.read_text(),'')
        self.assertFalse((self.root/'must-not-exist').exists())


class ConfigOwnershipTests(unittest.TestCase):
    def test_atomic_rewrite_preserves_complete_bytes_and_restricts_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            acl = Path(directory) / 'acl'
            passwd = Path(directory) / 'passwd'
            content = b'fixture credential bytes\n' * 10000
            for path in (acl, passwd):
                path.write_bytes(content)
                path.chmod(0o660)
            with mock.patch.object(reloader, 'ACL_PATH', str(acl)), \
                 mock.patch.object(reloader, 'PASSWD_PATH', str(passwd)), \
                 mock.patch.object(reloader.os, 'getuid', return_value=os.getuid() + 1):
                reloader.ensure_config_ownership()
            for path in (acl, passwd):
                self.assertEqual(path.read_bytes(), content)
                self.assertEqual(path.stat().st_mode & 0o777, 0o640)
            self.assertEqual(sorted(path.name for path in Path(directory).iterdir()), ['acl', 'passwd'])

    def test_failed_replace_preserves_original_and_removes_temporary_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            acl = Path(directory) / 'acl'
            acl.write_bytes(b'original ACL\n')
            with mock.patch.object(reloader, 'ACL_PATH', str(acl)), \
                 mock.patch.object(reloader, 'PASSWD_PATH', str(Path(directory) / 'missing')), \
                 mock.patch.object(reloader.os, 'getuid', return_value=os.getuid() + 1), \
                 mock.patch.object(reloader.os, 'replace', side_effect=PermissionError('fixture')):
                with self.assertRaises(PermissionError):
                    reloader.ensure_config_ownership()
            self.assertEqual(acl.read_bytes(), b'original ACL\n')
            self.assertEqual(list(Path(directory).iterdir()), [acl])


class ReloadHttpContractTests(unittest.TestCase):
    TOKEN = "t" * 32

    def setUp(self) -> None:
        reloader.TOKEN = self.TOKEN
        self.server = reloader.ThreadingHTTPServer(("127.0.0.1", 0), reloader.Handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.start()
        host, port = self.server.server_address
        self.url = f"http://{host}:{port}/reload"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join()

    def request(self, token: str) -> int:
        request = urllib.request.Request(
            self.url,
            data=b"{}",
            method="POST",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=1) as response:
                return response.status
        except urllib.error.HTTPError as error:
            try:
                return error.code
            finally:
                error.close()

    @mock.patch.object(reloader, "ensure_log_permissions")
    @mock.patch.object(reloader, "signal_and_wait_for_reload", return_value=True)
    def test_returns_204_only_after_acknowledgement(self, signal_reload: mock.Mock, _: mock.Mock) -> None:
        self.assertEqual(self.request(self.TOKEN), 204)
        signal_reload.assert_called_once_with()

    @mock.patch.object(reloader, "ensure_log_permissions")
    @mock.patch.object(reloader, "signal_and_wait_for_reload", return_value=False)
    def test_returns_504_without_acknowledgement(self, signal_reload: mock.Mock, _: mock.Mock) -> None:
        self.assertEqual(self.request(self.TOKEN), 504)
        signal_reload.assert_called_once_with()

    @mock.patch.object(reloader, "signal_and_wait_for_reload")
    def test_rejects_invalid_bearer_without_signalling(self, signal_reload: mock.Mock) -> None:
        self.assertEqual(self.request("x" * 32), 403)
        signal_reload.assert_not_called()

    @mock.patch.object(reloader, 'ensure_config_ownership', side_effect=PermissionError('fixture'))
    @mock.patch.object(reloader, 'signal_and_wait_for_reload')
    def test_failed_ownership_repair_does_not_signal_or_acknowledge(self, signal_reload: mock.Mock, _: mock.Mock) -> None:
        self.assertEqual(self.request(self.TOKEN), 503)
        signal_reload.assert_not_called()


if __name__ == "__main__":
    unittest.main()
