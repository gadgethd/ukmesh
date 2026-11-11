import unittest
from unittest.mock import patch
import worker


class RedisTimeoutTests(unittest.TestCase):
    def test_socket_timeout_exceeds_the_longest_idle_block(self):
        with patch.object(worker.redis.Redis, 'from_url') as connect:
            worker.redis_connection()
        options = connect.call_args.kwargs
        self.assertGreater(options['socket_timeout'], 5)
        self.assertEqual(options['socket_connect_timeout'], 5)


if __name__ == '__main__':
    unittest.main()
