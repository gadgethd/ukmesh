"""Exercise retry recovery/exhaustion and real child process exit propagation."""

from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("ci_retry", Path(__file__).with_name("ci-retry.py"))
RETRY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RETRY)

APT_FAILURE = 'process "/bin/sh -c apt-get update && apt-get install -y curl" did not complete successfully: exit code: 100'


class ClassificationTests(unittest.TestCase):
    def test_registry_transients(self):
        for message in (
            "received unexpected HTTP status: 500 Internal Server Error",
            "unexpected status from HEAD request to https://registry.example/v2/: 502",
            "503 Service Unavailable", "504 Gateway Timeout", "429 Too Many Requests",
            "toomanyrequests: retry later", "read: connection reset by peer",
            "i/o timeout", "TLS handshake timeout", "Temporary failure in name resolution",
        ):
            with self.subTest(message=message):
                self.assertTrue(RETRY.retryable("pull", message))
                self.assertTrue(RETRY.retryable("build", f"ERROR: failed to solve: {message}"))

    def test_apt_mirror_transients(self):
        for message in (
            "E: Failed to fetch http://mirror.example/Packages Hash Sum mismatch",
            "File has unexpected size (123 != 456). Mirror sync in progress?",
            "E: Failed to fetch http://mirror.example/pkg.deb 502 Bad Gateway",
            "E: Temporary failure resolving 'mirror.example'",
        ):
            for prefix in ("ERROR: failed to build: failed to solve: ", "target hopreach: failed to solve: "):
                with self.subTest(message=message, prefix=prefix):
                    self.assertTrue(RETRY.retryable("apt", message))
                    self.assertTrue(RETRY.retryable("build", f"#12 {message}\n{prefix}{APT_FAILURE}"))

    def test_permanent_acquisition_errors(self):
        for message in (
            "401 Unauthorized", "pull access denied", "manifest unknown", "404 Not Found",
            "no matching manifest for linux/amd64", "x509: certificate signed by unknown authority",
            "E: Unable to locate package nonexistent", "E: repository is not signed",
            "NO_PUBKEY ABCDEF", "held broken packages", "unmet dependencies",
        ):
            with self.subTest(message=message):
                self.assertFalse(RETRY.retryable("pull", message))
                self.assertFalse(RETRY.retryable("apt", message))
                self.assertFalse(RETRY.retryable("apt", f"502 Bad Gateway\n{message}"))

    def test_build_failures_ignore_earlier_network_output(self):
        for command in ("npm run build", "go build ./...", "npm audit", "python -m unittest"):
            with self.subTest(command=command):
                output = f'502 Bad Gateway\nERROR: failed to solve: process "/bin/sh -c {command}" did not complete successfully: exit code: 1'
                self.assertFalse(RETRY.retryable("build", output))
        self.assertFalse(RETRY.retryable("build", "502 Bad Gateway\nERROR: failed to solve: failed to compute cache key: missing file"))
        self.assertFalse(RETRY.retryable("build", "502 Bad Gateway\nunknown build error"))
        self.assertFalse(RETRY.retryable("build", "502 Bad Gateway\n" + APT_FAILURE))


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(__file__).resolve().parent.parent / ".ukmesh-tools"
        scratch.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.counter = Path(self.temp.name) / "attempts"
        self.delays = []
        self.output = io.StringIO()

    def invoke(self, outcomes, kind="pull"):
        # A real child emits to stderr, records invocations and returns the
        # chosen status; no Docker daemon or external services are contacted.
        program = """
import json, pathlib, sys
counter = pathlib.Path(sys.argv[1])
attempt = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(attempt + 1))
status, message = json.loads(sys.argv[2])[attempt]
print(message, file=sys.stderr, flush=True)
sys.exit(status)
"""
        command = [sys.executable, "-c", program, str(self.counter), json.dumps(outcomes)]
        with redirect_stdout(self.output), redirect_stderr(self.output):
            status = RETRY.run(kind, command, sleep=self.delays.append)
        return status, int(self.counter.read_text())

    def test_success_does_not_retry_even_with_warning(self):
        self.assertEqual(self.invoke([(0, "502 Bad Gateway warning")]), (0, 1))
        self.assertEqual(self.delays, [])

    def test_recovers_after_two_transient_failures(self):
        outcomes = [(7, "500 Internal Server Error"), (8, "502 Bad Gateway"), (0, "pulled")]
        self.assertEqual(self.invoke(outcomes), (0, 3))
        self.assertEqual(self.delays, [5, 10])
        self.assertIn("pulled", self.output.getvalue())

    def test_exhaustion_returns_last_failure_without_fourth_attempt(self):
        self.assertEqual(self.invoke([(7, "502 Bad Gateway"), (8, "503 Service Unavailable"), (9, "i/o timeout")]), (9, 3))
        self.assertEqual(self.delays, [5, 10])

    def test_permanent_failure_fails_immediately(self):
        self.assertEqual(self.invoke([(42, "manifest unknown")]), (42, 1))
        self.assertEqual(self.delays, [])

    def test_transient_then_permanent_stops_without_third_attempt(self):
        self.assertEqual(self.invoke([(7, "502 Bad Gateway"), (42, "manifest unknown")]), (42, 2))
        self.assertEqual(self.delays, [5])

    def test_apt_build_restarts_failed_layer(self):
        output = f"#12 E: Hash Sum mismatch\nERROR: failed to solve: {APT_FAILURE}"
        self.assertEqual(self.invoke([(1, output), (0, "built")], kind="build"), (0, 2))
        self.assertEqual(self.delays, [5])

    def test_build_error_after_warning_fails_immediately(self):
        output = '502 Bad Gateway\nERROR: failed to solve: process "/bin/sh -c npm run build" failed'
        self.assertEqual(self.invoke([(23, output)], kind="build"), (23, 1))
        self.assertEqual(self.delays, [])

    def test_arguments_are_passed_without_shell_interpretation(self):
        literal = "spaces; $(never-execute) 'quotes'"
        command = [sys.executable, "-c", "import sys; print(sys.argv[1])", literal]
        with redirect_stdout(self.output):
            self.assertEqual(RETRY.run("pull", command, sleep=self.delays.append), 0)
        self.assertEqual(self.output.getvalue().strip(), literal)
        self.assertEqual(self.delays, [])


if __name__ == "__main__":
    unittest.main()
