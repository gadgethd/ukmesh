import os
import random
import unittest

import redis

import link_queue_v3 as queue


@unittest.skipUnless(os.environ.get('TEST_REDIS_URL'), 'TEST_REDIS_URL is required')
class LinkQueueV3RedisTests(unittest.TestCase):
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

    def _admit(self, index: int):
        job_id = f'job-{index}'
        status, accepted_id = queue.admit(self.redis, {
            'version': 3,
            'type': 'physical_pair',
            'job_id': job_id,
            'dedupe_key': f'pair-{index}',
            'node_a_id': f'a-{index}',
            'node_b_id': f'b-{index}',
        })
        self.assertEqual((status, accepted_id), ('accepted', job_id))
        self.assertEqual(self.redis.llen(queue.WAKE), 1)
        return job_id

    def test_final_nack_releases_active_capacity_and_retains_purgeable_dead_payload(self):
        job_id = self._admit(1)
        for attempt in range(queue.MAX_ATTEMPTS):
            claimed = queue.claim(self.redis)
            self.assertIsNotNone(claimed)
            claimed_id, token, _payload, actual_attempt = claimed
            self.assertEqual(claimed_id, job_id)
            self.assertEqual(actual_attempt, attempt + 1)
            transition = queue.nack(self.redis, job_id, token)

        self.assertEqual(transition, 'dead')
        report = queue.audit_invariants(self.redis)
        self.assertEqual(report['actual_count'], 0)
        self.assertEqual(report['actual_dead_count'], 1)
        self.assertGreater(report['actual_dead_bytes'], 0)
        self.assertTrue(report['consistent'])
        self.assertEqual(
            self.redis.hget(queue.DEAD_REASONS, job_id),
            'attempt_limit_exceeded',
        )

        self.assertTrue(queue.purge_dead(self.redis, job_id))
        report = queue.audit_invariants(self.redis)
        self.assertEqual(report['actual_dead_count'], 0)
        self.assertEqual(self.redis.hexists(queue.PAYLOADS, job_id), 0)
        self.assertEqual(self.redis.hexists(queue.DEDUPE_BY_JOB, job_id), 0)
        self.assertEqual(self.redis.hexists(queue.DEAD_REASONS, job_id), 0)

    def test_requeue_dead_moves_accounting_back_under_active_admission_limits(self):
        job_id = self._admit(2)
        for _ in range(queue.MAX_ATTEMPTS):
            claimed_id, token, _payload, _attempt = queue.claim(self.redis)
            self.assertEqual(claimed_id, job_id)
            queue.nack(self.redis, job_id, token)

        self.assertEqual(queue.requeue_dead(self.redis, job_id), 'requeued')
        self.assertEqual(self.redis.hexists(queue.DEAD_REASONS, job_id), 0)
        report = queue.audit_invariants(self.redis)
        self.assertEqual(report['actual_count'], 1)
        self.assertEqual(report['actual_dead_count'], 0)
        self.assertTrue(report['consistent'])

    def test_atomic_repair_reconstructs_corrupt_counters_from_bounded_job_state(self):
        self._admit(3)
        self._admit(4)
        self.redis.hset(queue.COUNTERS, mapping={
            'count': 99,
            'bytes': 1,
            'dead_count': 12,
            'dead_bytes': 34,
        })

        before = queue.audit_invariants(self.redis)
        self.assertFalse(before['consistent'])
        self.assertEqual(before['actual_count'], 2)
        repaired = queue.audit_invariants(self.redis, apply=True)
        self.assertTrue(repaired['applied'])
        self.assertTrue(queue.audit_invariants(self.redis)['consistent'])

    def test_randomized_transitions_preserve_recorded_count_and_byte_invariants(self):
        rng = random.Random(20260729)
        next_job = 0
        for _ in range(500):
            action = rng.randrange(6)
            if action <= 1:
                next_job += 1
                queue.admit(self.redis, {
                    'version': 3,
                    'type': 'physical_pair',
                    'job_id': f'random-{next_job}',
                    'dedupe_key': f'random-pair-{next_job}',
                    'node_a_id': f'a-{next_job}',
                    'node_b_id': f'b-{next_job}',
                })
            elif action in (2, 3):
                claimed = queue.claim(self.redis)
                if claimed:
                    job_id, token, _payload, _attempt = claimed
                    if action == 2:
                        queue.ack(self.redis, job_id, token)
                    else:
                        queue.nack(self.redis, job_id, token)
            elif action == 4:
                dead = self.redis.zrange(queue.DEAD, 0, 0)
                if dead:
                    queue.requeue_dead(self.redis, dead[0])
            else:
                dead = self.redis.zrange(queue.DEAD, 0, 0)
                if dead:
                    queue.purge_dead(self.redis, dead[0])

            report = queue.audit_invariants(self.redis)
            self.assertTrue(report['consistent'], report)


if __name__ == '__main__':
    unittest.main()
