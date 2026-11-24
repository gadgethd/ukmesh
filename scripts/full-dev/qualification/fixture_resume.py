"""Resume the same applied fixture when the VM stopped its infrastructure."""

import time

from common import docker, utc_now, write_json
from consumer_refs import describe_consumer
from fixtures import selection_files
from target_safety import assert_no_publishers, inspect_containers, read_intent


# These are the existing data-plane resources needed to validate the fixture.
INFRASTRUCTURE = {
    'ukmesh-sample-db': ('ukmesh-sample', 'db', '/var/lib/postgresql/data', 'ukmesh-sample_sample-db'),
    'ukmesh-sample-redis': ('ukmesh-sample', 'redis', '/data', None),
    'ukmesh-sample-mqtt': ('ukmesh-sample', 'mqtt', '/mosquitto/data', 'ukmesh-sample_sample-mqtt-data'),
    'ukmesh-companions-beacon-db-1': (
        'ukmesh-companions', 'beacon-db', '/var/lib/postgresql/data', 'ukmesh-companions_beacon-db',
    ),
    'ukmesh-companions-beacon-redis-1': ('ukmesh-companions', 'beacon-redis', '/data', None),
}


def prepare_applied_fixture(evidence):
    """Called under the exclusive target lease, before live store verification."""
    state = selection_files()
    intent = read_intent()
    token = state['selection'].get('token')
    if not token or not intent or intent['status'] != 'applied' or (
            intent['requested_selection'] != state['selection'] or intent['requested_values'] != state['values']):
        raise ValueError('an exact applied owned fixture is required for interrupted recovery')

    assert_no_publishers()
    containers = inspect_containers()
    infrastructure = {}
    for name, (project, service, path, volume) in INFRASTRUCTURE.items():
        item = containers.get(name)
        if not item:
            raise ValueError(f'missing existing fixture infrastructure: {name}')
        labels = item['Config'].get('Labels') or {}
        mounts = [mount for mount in item['Mounts'] if mount['Destination'] == path]
        host = item['HostConfig']
        bindings = host.get('PortBindings') or {}
        network = host.get('NetworkMode', '')
        if (labels.get('com.docker.compose.project'), labels.get('com.docker.compose.service')) != (project, service) \
                or len(mounts) != 1 or mounts[0]['Type'] != 'volume' \
                or (volume is not None and mounts[0].get('Name') != volume) \
                or network == 'host' or network.startswith('container:') or host.get('PublishAllPorts') \
                or any(binding['HostIp'] != '127.0.0.1' for group in bindings.values() for binding in group or ()):
            raise ValueError(f'fixture infrastructure identity differs: {name}')
        infrastructure[name] = item['Id']

    applied = intent['applied_consumers']
    restarting = []
    for name, expected in applied.items():
        item = containers.get(name)
        if not item:
            raise ValueError(f'missing applied fixture consumer: {name}')
        actual = describe_consumer(name, item)
        if actual['refs'] != expected['refs']:
            raise ValueError(f'applied fixture store differs: {name}')
        if item['State']['Restarting']:
            if item['Id'] != expected['container_id']:
                raise ValueError(f'restarting consumer identity differs: {name}')
            restarting.append(name)

    stopped = [name for name in infrastructure if not containers[name]['State']['Running']]
    write_json(evidence / 'infrastructure-resume.json', {
        'at': utc_now(), 'fixture': token, 'infrastructure': stopped, 'restarting_consumers': restarting,
    })
    if restarting:
        docker(['stop', *(containers[name]['Id'] for name in restarting)], timeout=60)
    if stopped:
        docker(['start', *(infrastructure[name] for name in stopped)], timeout=60)

    deadline = time.monotonic() + 60
    while True:
        current = inspect_containers(deadline)
        for name, expected_id in infrastructure.items():
            if current.get(name, {}).get('Id') != expected_id:
                raise ValueError(f'fixture infrastructure changed during recovery: {name}')
        if all(current[name]['State']['Running']
               and current[name]['State'].get('Health', {}).get('Status') == 'healthy'
               for name in infrastructure):
            return token
        if time.monotonic() >= deadline:
            raise TimeoutError('fixture infrastructure did not become healthy')
        time.sleep(min(1, deadline - time.monotonic()))
