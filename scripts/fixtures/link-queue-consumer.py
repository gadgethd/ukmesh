"""Real Python protocol consumer for the cross-language owned Redis fixture."""
import os
import sys
import redis
import link_queue_v3 as queue

client = redis.Redis.from_url(os.environ['TEST_REDIS_URL'], password=os.environ['TEST_REDIS_PASSWORD'], decode_responses=True)
try:
    phase = sys.argv[1]
    if phase == 'ack-observe':
        job_id, token, payload, attempt = queue.claim(client)
        assert payload['type'] == 'observe' and payload['src_node_id'] == 'fixture-\u00e9'
        assert payload['logical_job_id'] == job_id and attempt == 1
        assert queue.owns_lease(client, job_id, token)
        assert queue.ack(client, job_id, token)
    elif phase == 'admit-physical':
        client.delete(queue.WORKER_HEARTBEAT)
        assert queue.admit_physical(client, 'z', 'a')[0] == 'accepted'
    elif phase == 'ack-physical':
        job_id, token, payload, _ = queue.claim(client)
        assert payload['type'] == 'physical_pair'
        assert (payload['node_a_id'], payload['node_b_id']) == ('a', 'z')
        assert queue.ack(client, job_id, token)
    elif phase == 'ack-rebuild':
        job_id, token, payload, _ = queue.claim(client)
        assert payload['generation'] == 'link_rebuild_0123456789abcdef'
        assert queue.ack(client, job_id, token)
        assert queue.claim(client) is None
        client.delete(queue.REBUILD)
        job_id, token, payload, _ = queue.claim(client)
        assert payload['packet_hash'] == 'frozen' and 'generation' not in payload
        assert queue.ack(client, job_id, token)
        assert queue.audit_invariants(client)['consistent']
    elif phase == 'trim-completed':
        completed = client.zrange(queue.COMPLETED, 0, -1)
        assert len(completed) >= 2
        oldest, newest = completed[0], completed[-1]
        old_dedupe = client.hget(queue.DEDUPE_BY_JOB, oldest)
        new_dedupe = client.hget(queue.DEDUPE_BY_JOB, newest)
        assert old_dedupe and new_dedupe

        status, active_id = queue.admit_physical(client, 'owned-active-a', 'owned-active-z')
        assert status == 'accepted'
        active_job, active_token, _, _ = queue.claim(client)
        assert active_job == active_id
        status, dead_id = queue.admit_physical(client, 'owned-dead-a', 'owned-dead-z')
        assert status == 'accepted'
        dead_job, dead_token, _, _ = queue.claim(client)
        assert dead_job == dead_id
        queue.MAX_ATTEMPTS = 1
        assert queue.nack(client, dead_id, dead_token, 'OwnedFixture') == 'dead'

        queue.COMPLETED_MAX_JOBS = 1
        original_maxmemory = client.config_get('maxmemory')['maxmemory']
        client.config_set('maxmemory', 1)
        try:
            assert queue.cleanup_completed(client, limit=1) == 1
        finally:
            client.config_set('maxmemory', original_maxmemory)
        assert client.zcard(queue.COMPLETED) == len(completed) - 1
        assert client.hget(queue.STATES, oldest) is None
        assert client.hget(queue.DEDUPE, old_dedupe) is None
        assert client.hget(queue.DEDUPE, new_dedupe) == newest
        assert client.hget(queue.STATES, newest) == 'complete'
        assert queue.owns_lease(client, active_id, active_token)
        assert client.hget(queue.STATES, dead_id) == 'dead'
        queue.COMPLETED_MAX_JOBS = len(completed)
        client.zadd(queue.COMPLETED, {newest: 1})
        assert queue.cleanup_completed(client, limit=1) == 1
        assert client.hget(queue.DEDUPE, new_dedupe) is None
        assert queue.audit_invariants(client)['consistent']
    else:
        raise ValueError('unknown contract phase')
    print(f'link-v3 Python {phase} passed')
finally:
    client.close()
