"""One bounded protection window; application owners define their intake state."""
from concurrent.futures import ThreadPoolExecutor
import json
import hashlib
import datetime as dt
from pathlib import Path
import sys
import uuid
import time

from common import REPO,STATE,command,docker,write_json,utc_now
from loopback_http import http_get
sys.path.insert(0,str(REPO/'scripts/companion-dev'))
from ingest_state import read_core,read_beacon,metric

APPS={'ukmesh':'ukmesh-full-backend-1','corescope':'ukmesh-companions-corescope-ingestor-1','beacon':'ukmesh-companions-beacon-1'}
QUEUES=('meshcore_mqtt_ingest_active','meshcore_mqtt_ingest_queue_depth',
        'meshcore_packet_batch_queue_depth','meshcore_mqtt_post_persist_inflight')
SECONDS=45


def stable_identities(snapshot):
    return {name:{key:row.get(key) for key in ('container_id','started_at','pid','image_id','running','refs')}
            for name,row in snapshot['consumers'].items() if row['refs']}


class IntakeProof:
    def __init__(self,started,required):
        self.started=started;self.required=set(required);self.first={};self.last={};self.core_times=set();self.reads=0

    def add(self,observations):
        if set(observations)!=self.required:raise ValueError('all live intake owners must report; missing is not zero')
        for name,value in observations.items():
            if value.get('quiescent') is not True:raise ValueError(name+' intake is not quiescent')
            old=self.first.setdefault(name,value)
            if name=='corescope':
                if value['process']!=old['process'] or value['state']['generation']!=old['state']['generation'] \
                        or value['state']['droppedTotal']!=old['state']['droppedTotal'] or value['write_errors']!=old['write_errors']:
                    raise ValueError('Core process or intake changed during protection')
                self.core_times.add(value['sampled_at'])
            if name=='beacon' and (value['process_started_at']!=old['process_started_at'] \
                    or value['state']['droppedTotal']!=old['state']['droppedTotal']):
                raise ValueError('Beacon process or intake changed during protection')
            self.last[name]=value
        self.reads+=1

    def ready(self):
        return self.reads>=2 and ('corescope' not in self.required or len(self.core_times)>=2) \
            and ('ukmesh' not in self.required or self.last['ukmesh']['probe_last_success']>=self.started)


def read_apps(snapshot,deadline,*,clock=time.monotonic,wall=time.time):
    def remaining():
        value=deadline-clock()
        if value<=0:raise TimeoutError('protection observation deadline')
        return min(2,value)
    def execute(args,**options):return docker(args,timeout=min(options.get('timeout',2),remaining()))
    result={}
    for app,name in APPS.items():
        record=snapshot['consumers'].get(name)
        if record is None:raise ValueError('intake owner absent; resume previous retained store')
        if not record['running']:continue  # Explicit same-ref resume is validated by the intent owner.
        item=snapshot['containers'][name]
        if app=='corescope':result[app]=read_core(item,execute=execute,now=wall,timeout=remaining())
        elif app=='beacon':result[app]=read_beacon(item,execute=execute,now=wall,timeout=remaining())
        else:
            env=dict(v.split('=',1) for v in item['Config'].get('Env',[]))
            if env.get('MQTT_INGEST_MODE','durable')!='durable':raise ValueError('restore normal durable ingest before changing stores')
            ready=json.loads(http_get('http://127.0.0.1:3100/readyz',timeout=remaining())[0])
            text=http_get('http://127.0.0.1:9091/metrics',timeout=remaining())[0].decode()
            now=wall();stamp=ready.get('ts')
            if type(stamp) not in (int,float) or not 0<=now-stamp/1000<=5 \
                    or ready.get('status')!='ready' or ready.get('checks',{}).get('database') is not True \
                    or ready.get('checks',{}).get('mqtt',{}).get('state')!='connected':
                raise ValueError('backend readiness is unavailable or stale')
            queues={key:metric(text,key) for key in QUEUES}
            healthy=metric(text,'meshcore_mqtt_delivery_probe_healthy')==1 and metric(text,'meshcore_mqtt_delivery_probe_enabled')==1
            success=metric(text,'meshcore_mqtt_delivery_probe_last_success_timestamp_seconds')
            if success>now:raise ValueError('delivery probe timestamp is in the future')
            result[app]={'app':app,'quiescent':healthy and all(v==0 for v in queues.values()),
                         'queues':queues,'probe_last_success':success,'observed_at':now}
    return result


def valid_broker(value):
    cleanup=value.get('cleanup',{})
    return type(value.get('version')) is int and value['version']==1 and value.get('passed') is True \
        and cleanup.get('closed') is True and cleanup.get('forced') is False \
        and cleanup.get('connected') is True and cleanup.get('subscribed') is True


def save_broker_result(value):
    path=STATE/'runtime/target-protection'/('broker-'+uuid.uuid4().hex+'.json')
    write_json(path,value)
    return str(path)


def broker_observation(deadline,*,clock=time.monotonic,save=save_broker_result):
    remaining=deadline-clock()
    if remaining<=0:raise TimeoutError('protection deadline before broker observation')
    result=command(['node',str(REPO/'scripts/local-sample/broker-quiescence.mjs')],timeout=remaining,check=False)
    if len(result.stdout)>8*1024**2:raise ValueError('broker observation output exceeded its bound')
    # The broker owner emits only counters/digests and fixed error codes. Retain
    # its actual stdout even when it refuses, before reporting generic failure.
    evidence={'at':utc_now(),'exit_code':result.returncode,'stdout':result.stdout.decode(errors='replace'),
              'stdout_sha256':hashlib.sha256(result.stdout).hexdigest()}
    path=save(evidence)
    value=json.loads(result.stdout)
    if result.returncode or not valid_broker(value):
        raise ValueError('broker observation refused; retained evidence '+str(path))
    return {**value,'evidence_path':path,'stdout_sha256':evidence['stdout_sha256']}


def observe(snapshot_reader,*,apps=read_apps,broker=broker_observation,clock=time.monotonic,wall=time.time,sleep=time.sleep):
    started=clock();deadline=started+SECONDS;started_wall=wall()
    sources=[Path(__file__),REPO/'scripts/local-sample/broker-quiescence.mjs',REPO/'scripts/companion-dev/ingest_state.py']
    source_hashes=lambda:{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    source_before=source_hashes()
    initial=snapshot_reader(deadline);identities=stable_identities(initial)
    required={app for app,name in APPS.items() if initial['consumers'].get(name,{}).get('running') is True}
    tracker=IntakeProof(started_wall,required)
    # Fail missing/unsupported app state before creating even a clean observer client.
    tracker.add(apps(initial,deadline))
    with ThreadPoolExecutor(max_workers=1) as worker:
        pending=worker.submit(broker,deadline)
        while True:
            if clock()>=deadline:raise TimeoutError('combined protection observation exceeded45seconds')
            tracker.add(apps(initial,deadline))
            if pending.done():
                broker_result=pending.result()
                if not valid_broker(broker_result):
                    raise ValueError('fresh broker observation did not pass and close')
                if tracker.ready():break
            sleep(min(1,max(0,deadline-clock())))
    final=snapshot_reader(deadline)
    if clock()>deadline or identities!=stable_identities(final) or initial['broker']!=final['broker'] or source_before!=source_hashes():
        raise ValueError('protection source identity changed or exceeded its budget')
    return {'passed':True,'scope':'protection only; no timed qualification result changed',
            'started_at':started_wall,'finished_at':wall(),'duration_seconds':clock()-started,
            'max_seconds':SECONDS,'source_sha256':source_before,'broker':broker_result,'applications':tracker.last,'reads':tracker.reads,
            'consumer_identities':identities,'broker_identity':initial['broker']}
