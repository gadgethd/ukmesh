#!/usr/bin/env python3
"""Restart only the owned fixture broker and prove the same reloader still ACKs."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0,str(Path(__file__).with_name('qualification')))
from benchmarks import active_fixture
from common import docker, owned_run, safe_error, utc_now, write_json
from context import phase_context

BROKER='ukmesh-sample-mqtt'
RELOADER='ukmesh-full-mosquitto-reloader-1'
CONTROL_VOLUME='ukmesh-sample_sample-mqtt-control'


def inspected():
    values=json.loads(docker(['inspect',BROKER,RELOADER]).stdout)
    for item,project in zip(values,('ukmesh-sample','ukmesh-full')):
        if item['Config']['Labels'].get('com.docker.compose.project')!=project:
            raise ValueError('Broker/reloader local project ownership differs')
        controls=[mount for mount in item['Mounts'] if mount['Destination']=='/mosquitto/control']
        if len(controls)!=1 or controls[0].get('Name')!=CONTROL_VOLUME:
            raise ValueError('Broker/reloader must share their owned control volume')
        if item['HostConfig'].get('Privileged') or any(mount['Destination'].endswith('docker.sock') for mount in item['Mounts']):
            raise ValueError('Broker control must not require privilege or a Docker socket')
    if values[1]['HostConfig']['PidMode'] not in ('','private'):
        raise ValueError('Reloader must own its PID namespace')
    return values


def status(route, token=None):
    request=Request('http://127.0.0.1:3109/'+route,
        data=b'{}' if route=='reload' else None,
        headers={'Authorization':'Bearer '+token} if token is not None else {})
    try:
        with urlopen(request,timeout=5) as response:return response.status
    except HTTPError as error:
        try:return error.code
        finally:error.close()


def prove(run):
    fixture=active_fixture()
    before=inspected()
    if any(not item['State']['Running'] for item in before):
        raise ValueError('Start the reviewed broker/reloader configuration before the native proof')
    environment=dict(value.split('=',1) for value in before[1]['Config']['Env'])
    token=environment['OWNER_ACL_RELOAD_TOKEN']
    if len(token)<32 or environment.get('MOSQUITTO_RELOAD_FIFO')!='/mosquitto/control/reload':
        raise ValueError('Reloader FIFO transport/authentication is not configured')
    context=phase_context(run)
    attempt=Path(tempfile.mkdtemp(prefix='reloader-lifecycle-',dir=run))
    checks=[]
    try:
        assert status('reload','wrong-owned-token')==403
        assert status('reload',token)==204
        checks.append({'name':'authenticated-real-broker-ack-before-restart','passed':True})
        docker(['restart','--time','15',BROKER],timeout=60)
        deadline=time.monotonic()+120
        while True:
            current=inspected()
            ready=all(item['State']['Running'] for item in current) and \
                current[0]['State'].get('Health',{}).get('Status')=='healthy'
            if ready and status('healthz')==200:break
            if time.monotonic()>=deadline:raise RuntimeError('Broker/reloader did not recover within120seconds')
            time.sleep(.25)
        assert status('reload',token)==204
        checks.append({'name':'authenticated-real-broker-ack-after-restart','passed':True})
        after=inspected()
        same=after[1]['Id']==before[1]['Id'] and after[1]['State']['Pid']==before[1]['State']['Pid'] \
            and after[1]['RestartCount']==before[1]['RestartCount']
        checks.append({'name':'same-reloader-process-survives-broker-restart','passed':same})
        checks.append({'name':'fixture-and-profile-preserved','passed':active_fixture()['token']==fixture['token'] and phase_context(run)==context})
    except Exception as error:
        checks.append({'name':'native-reloader-lifecycle','passed':False,'error':safe_error(error)})
    result={'passed':bool(checks) and all(item['passed'] for item in checks),'at':utc_now(),
            'fixture_token':fixture['token'],'checks':checks,'context':context,'attempt':str(attempt)}
    write_json(attempt/'result.json',result)
    write_json(run/'reloader-lifecycle.json',result)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',required=True)
    args=parser.parse_args()
    result=prove(owned_run(args.run))
    print(json.dumps({'passed':result['passed'],'attempt':result['attempt']}))
    raise SystemExit(0 if result['passed'] else 1)
