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
    else:
        raise ValueError('unknown contract phase')
    print(f'link-v3 Python {phase} passed')
finally:
    client.close()
