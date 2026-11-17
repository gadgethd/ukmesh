"""Exercise the configured Alloy health command against a native HTTP fixture."""
import socket
import socketserver
import subprocess
import threading
import unittest

from test_memory_policy import compose


class AlloyReadiness(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        service = compose('docker-compose.yml', 'scripts/full-dev/compose.yml')['alloy']
        check = service['healthcheck']
        cls.command = check['test'][1:]
        # Compose config escapes dollars so its output can be read as Compose
        # again; the container engine receives one dollar per escaped pair.
        cls.command[-1] = cls.command[-1].replace('$$', '$')
        cls.port = int(next(value for value in service['command']
                            if value.startswith('--server.http.listen-addr=')).rsplit(':', 1)[1])

    def run_response(self, response, *, stall=False):
        requests = []
        finish = threading.Event()

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(3)
                request = b''
                while b'\r\n\r\n' not in request:
                    chunk = self.request.recv(4096)
                    if not chunk:
                        break
                    request += chunk
                requests.append(request)
                if stall:
                    finish.wait(3)
                elif response:
                    self.request.sendall(response)

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        with Server(('127.0.0.1', self.port), Handler) as server:
            worker = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
            worker.start()
            try:
                result = subprocess.run(self.command, capture_output=True, timeout=4)
            finally:
                finish.set()
                server.shutdown()
                worker.join(timeout=1)
        self.assertEqual(requests, [b'GET /-/ready HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n'])
        return result.returncode

    def test_ready_200_accepts_both_http_versions(self):
        for version in (b'HTTP/1.0', b'HTTP/1.1'):
            with self.subTest(version=version):
                self.assertEqual(self.run_response(version + b' 200 OK\r\n\r\n'), 0)

    def test_unready_malformed_and_closed_responses_fail(self):
        for response in (b'HTTP/1.1 503 Service Unavailable\r\n\r\n', b'INVALID 200 OK\r\n',
                         b'HTTP/1.1 2000 OK\r\n', b'HTTP/1.1', b''):
            with self.subTest(response=response):
                self.assertNotEqual(self.run_response(response), 0)

    def test_stalled_status_line_is_bounded(self):
        self.assertNotEqual(self.run_response(b'', stall=True), 0)

    def test_connection_refusal_fails_without_new_service_dependencies(self):
        with socket.socket() as temporary:
            temporary.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            temporary.bind(('127.0.0.1', self.port))
            self.assertNotEqual(subprocess.run(self.command, capture_output=True, timeout=4).returncode, 0)


if __name__ == '__main__':
    unittest.main()
