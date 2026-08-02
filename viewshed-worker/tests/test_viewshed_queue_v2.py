import os
import unittest

import redis

import viewshed_queue_v2 as queue


@unittest.skipUnless(os.environ.get('TEST_REDIS_URL'), 'TEST_REDIS_URL is required')
class ViewshedQueueV2RedisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis = redis.Redis.from_url(
            os.environ['TEST_REDIS_URL'],
            password=os.environ.get('TEST_REDIS_PASSWORD') or None,
            decode_responses=True,
        )

    @classmethod
    def tearDownClass(cls):
        cls.redis.close()

    def setUp(self):
        self.redis.flushdb()

    @staticmethod
    def job(node_id='node-1', lat=51.5):
        return {
            'version': 2,
            'node_id': node_id,
            'lat': lat,
            'lon': -1.2,
            'model_version': 5,
        }

    def test_worker_crash_reaps_one_recoverable_retry_without_losing_capacity(self):
        self.assertEqual(queue.admit(self.redis, self.job())[0], 'accepted')
        job_id, old_token, _payload, attempt = queue.claim(self.redis)
        self.assertEqual((job_id, attempt), ('node-1', 1))
        self.redis.zadd(queue.LEASES, {'node-1': 0})

        self.assertEqual(queue.reap(self.redis), 1)
        job_id, new_token, _payload, attempt = queue.claim(self.redis)
        self.assertEqual((job_id, attempt), ('node-1', 2))
        self.assertNotEqual(old_token, new_token)
        self.assertEqual(queue.ack(self.redis, job_id, new_token), 'complete')
        self.assertEqual(
            self.redis.hmget(queue.COUNTERS, 'count', 'bytes'),
            ['0', '0'],
        )

    def test_inflight_position_update_is_processed_as_a_followup_not_lost(self):
        queue.admit(self.redis, self.job(lat=51.5))
        job_id, token, payload, _attempt = queue.claim(self.redis)
        self.assertEqual(payload['lat'], 51.5)
        self.assertEqual(queue.admit(self.redis, self.job(lat=52.0))[0], 'coalesced')

        self.assertEqual(queue.ack(self.redis, job_id, token), 'requeued')
        job_id, token, payload, _attempt = queue.claim(self.redis)
        self.assertEqual(payload['lat'], 52.0)
        self.assertEqual(queue.ack(self.redis, job_id, token), 'complete')

    def test_transient_failures_retry_then_move_to_bounded_purgeable_dlq(self):
        queue.admit(self.redis, self.job())
        transition = None
        for _ in range(queue.MAX_ATTEMPTS):
            job_id, token, _payload, _attempt = queue.claim(self.redis)
            transition = queue.nack(self.redis, job_id, token)
        self.assertEqual(transition, 'dead')
        self.assertEqual(
            self.redis.hmget(queue.COUNTERS, 'count', 'dead_count'),
            ['0', '1'],
        )
        self.assertEqual(
            self.redis.hget(queue.DEAD_REASONS, 'node-1'),
            'attempt_limit_exceeded',
        )
        self.assertTrue(queue.purge_dead(self.redis, 'node-1'))
        self.assertEqual(
            self.redis.hmget(queue.COUNTERS, 'dead_count', 'dead_bytes'),
            ['0', '0'],
        )
        self.assertFalse(self.redis.hexists(queue.PAYLOADS, 'node-1'))
        self.assertFalse(self.redis.sismember(queue.PENDING, 'node-1'))
        self.assertFalse(self.redis.hexists(queue.DEAD_REASONS, 'node-1'))


if __name__ == '__main__':
    unittest.main()
