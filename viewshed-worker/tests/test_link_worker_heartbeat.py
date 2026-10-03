import threading
import unittest
from unittest.mock import Mock, call, patch

import link_queue_v3 as queue


class LinkWorkerHeartbeatTests(unittest.TestCase):
    def start_and_join(self, factory, stop):
        thread = queue.start_worker_heartbeat(factory, stop)
        self.addCleanup(thread.join, 2)
        self.addCleanup(stop.set)
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive(), 'heartbeat thread did not stop')
        return thread

    def stop_after_waits(self, count):
        stop = threading.Event()
        waits = []

        def wait(seconds):
            waits.append(seconds)
            if len(waits) == count:
                stop.set()
            return stop.is_set()

        return stop, waits, wait

    def test_publishes_immediately_and_refreshes_unix_seconds_before_expiry(self):
        client = Mock()
        factory = Mock(return_value=client)
        stop, waits, wait = self.stop_after_waits(2)
        with patch.object(stop, 'wait', side_effect=wait), patch.object(
            queue.time, 'time', side_effect=[1_000.9, 1_010.2],
        ):
            thread = self.start_and_join(factory, stop)

        self.assertEqual(thread.name, 'link-worker-heartbeat')
        self.assertEqual(waits, [10, 10])
        self.assertEqual(client.set.call_args_list, [
            call('meshcore:link:v3:worker_heartbeat', '1000', ex=45),
            call('meshcore:link:v3:worker_heartbeat', '1010', ex=45),
        ])
        factory.assert_called_once_with()
        client.close.assert_called_once_with()

    def test_failed_write_closes_old_client_and_reconnects_after_retry_delay(self):
        failed = Mock()
        failed.set.side_effect = ConnectionError('isolated test connection lost')
        recovered = Mock()
        factory = Mock(side_effect=[failed, recovered])
        stop, waits, wait = self.stop_after_waits(2)
        with patch.object(stop, 'wait', side_effect=wait), patch.object(
            queue.time, 'time', side_effect=[1_000.9, 1_002.2],
        ):
            self.start_and_join(factory, stop)

        self.assertEqual(waits, [2, 10])
        self.assertEqual(factory.call_count, 2)
        failed.close.assert_called_once_with()
        recovered.set.assert_called_once_with('meshcore:link:v3:worker_heartbeat', '1002', ex=45)
        recovered.close.assert_called_once_with()

    def test_connection_creation_failure_can_recover_and_publish(self):
        client = Mock()
        factory = Mock(side_effect=[ConnectionError('isolated test unavailable'), client])
        stop, waits, wait = self.stop_after_waits(2)
        with patch.object(stop, 'wait', side_effect=wait), patch.object(queue.time, 'time', return_value=1_002):
            self.start_and_join(factory, stop)

        self.assertEqual(waits, [2, 10])
        self.assertEqual(factory.call_count, 2)
        client.set.assert_called_once_with('meshcore:link:v3:worker_heartbeat', '1002', ex=45)
        client.close.assert_called_once_with()

    def test_already_stopped_worker_does_not_connect_or_publish(self):
        stop = threading.Event()
        stop.set()
        factory = Mock()
        self.start_and_join(factory, stop)
        factory.assert_not_called()

    def test_shutdown_interrupts_real_ten_second_wait(self):
        stop = threading.Event()
        published = threading.Event()
        client = Mock()
        client.set.side_effect = lambda *args, **kwargs: published.set()
        thread = queue.start_worker_heartbeat(lambda: client, stop)
        self.addCleanup(thread.join, 2)
        self.addCleanup(stop.set)
        self.assertTrue(published.wait(timeout=2), 'initial heartbeat was not published')
        stop.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive(), 'shutdown waited for the refresh interval')
        client.set.assert_called_once()
        client.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
