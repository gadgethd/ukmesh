import importlib.util
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


if __name__ == "__main__":
    unittest.main()
