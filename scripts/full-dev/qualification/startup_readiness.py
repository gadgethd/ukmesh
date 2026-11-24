"""One nonauthorizing reconnect preparation; target protection remains separate."""
import datetime as dt
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import uuid

from common import REPO,STATE,clean_environment,write_json,utc_now
from target_lease import child_context
from owned_process import cancellation_scope, spawn_session, settle_session as _settle
from target_observation import APPS,stable_identities
sys.path.insert(0,str(REPO/'scripts/companion-dev'))
from ingest_state import read_core_connection,read_beacon,timestamp

SECONDS=60
LIMIT=8*1024**2
HEALTH='ukmesh-full-mesh-health-check-1'


class Unavailable(RuntimeError):
    """Supported native state is temporarily unavailable, not an empty queue."""


def _cancellation_scope():
    return cancellation_scope('startup readiness')


def _check_cancellation(state):
    if state['signal'] is not None:
        raise InterruptedError('startup readiness canceled')


def _run_owned(argv,deadline,*,cancellation,after_spawn=None,merge_stderr=False,spawn=subprocess.Popen,limit_seconds=2):
    """Own only short read-only probe processes, including local sg descendants."""
    _check_cancellation(cancellation)
    left=deadline-time.monotonic()
    if left<=0:raise TimeoutError('startup readiness60 expired')
    end=time.monotonic()+min(limit_seconds,left)
    inherited,descriptors=child_context();child=None;ticks=None
    with tempfile.TemporaryFile()as out,tempfile.TemporaryFile()as err:
        try:
            child,ticks=spawn_session(argv,spawn=spawn,cwd=REPO,env={**clean_environment(),**inherited},
                pass_fds=descriptors,stdout=out,stderr=subprocess.STDOUT if merge_stderr else err)
            if after_spawn is not None:after_spawn(child)
            _check_cancellation(cancellation)
            while os.waitid(os.P_PID,child.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)is None:
                _check_cancellation(cancellation)
                if os.fstat(out.fileno()).st_size>LIMIT or os.fstat(err.fileno()).st_size>65536:raise ValueError('probe output bound exceeded')
                if time.monotonic()>=end:raise Unavailable('probe-timeout')
                time.sleep(min(.02,max(0,end-time.monotonic())))
            if os.fstat(out.fileno()).st_size>LIMIT or os.fstat(err.fileno()).st_size>65536:raise ValueError('probe output bound exceeded')
            out.seek(0);err.seek(0);stdout,stderr=out.read(),err.read()
        finally:
            if child is not None:
                _settle(child,ticks)
    if time.monotonic()>deadline:raise TimeoutError('startup readiness60 expired')
    return subprocess.CompletedProcess(argv,child.returncode,stdout,stderr)


def _run(argv,deadline,*,after_spawn=None,merge_stderr=False,spawn=subprocess.Popen,limit_seconds=2):
    with _cancellation_scope()as cancellation:
        return _run_owned(argv,deadline,cancellation=cancellation,after_spawn=after_spawn,
                          merge_stderr=merge_stderr,spawn=spawn,limit_seconds=limit_seconds)


def docker_probe(args,deadline,**options):
    result=_run(['sg','docker','-c',shlex.join(['docker','--host','unix:///var/run/docker.sock',*args])],deadline,merge_stderr=options.get('merge_stderr',False))
    if result.returncode:raise RuntimeError('native Docker read failed')
    return result


def _http(url,deadline):
    # Isolate the existing synchronous HTTP owner so slow trickle IO cannot
    # outlive the deadline or leave a background thread. No new endpoint/API.
    code='''import sys
sys.path.insert(0,sys.argv[1])
from loopback_http import http_get
from urllib.error import HTTPError,URLError
try:
 data,_=http_get(sys.argv[2],timeout=float(sys.argv[3]));sys.stdout.buffer.write(data)
except HTTPError as e:sys.exit(75 if e.code in(502,503,504)else 76)
except(URLError,TimeoutError,ConnectionError):sys.exit(75)
'''
    remaining=deadline-time.monotonic()
    if remaining<=0:raise TimeoutError('startup readiness60 expired')
    result=_run([sys.executable,'-c',code,str(REPO/'scripts/full-dev/qualification'),url,str(min(2,remaining))],deadline)
    if result.returncode==75:raise Unavailable('native-http-unavailable')
    if result.returncode:raise ValueError('native HTTP response unsupported')
    return json.loads(result.stdout)


def _env(item):return dict(row.split('=',1)for row in item['Config'].get('Env',[]))


def _beacon_logs(item,deadline,execute):
    container=item['Id'];started=item['State']['StartedAt']
    log_driver=item.get('HostConfig',{}).get('LogConfig',{}).get('Type')
    if log_driver!='journald':
        return execute(['logs','--timestamps','--since',started,container],merge_stderr=True).stdout
    if not re.fullmatch(r'[0-9a-f]{64}',container):raise ValueError('invalid Beacon journal identity')
    # Read the same Docker log records without streaming hours of access logs
    # through the daemon. Keep the enclosing readiness and output limits.
    pattern=r'beacon starting|component=ingest|"component"[[:space:]]*:[[:space:]]*"ingest"'
    result=_run(['journalctl','--no-pager','--quiet','--output=json',
                 '--output-fields=MESSAGE,CONTAINER_ID_FULL','--since',f'@{int(timestamp(started))}',
                 '--grep',pattern,f'CONTAINER_ID_FULL={container}'],deadline,limit_seconds=5)
    if result.returncode and (result.returncode!=1 or result.stdout):
        raise RuntimeError('native Beacon journal read failed')
    if not result.stdout:raise Unavailable('native Beacon journal events unavailable')
    rows=[]
    for line in result.stdout.splitlines():
        row=json.loads(line);message=row.get('MESSAGE');sampled=row.get('__REALTIME_TIMESTAMP')
        if row.get('CONTAINER_ID_FULL')!=container or not isinstance(message,str) or '\n' in message \
                or not isinstance(sampled,str) or not sampled.isascii() or not sampled.isdigit():
            raise ValueError('native Beacon journal event malformed')
        at=dt.datetime.fromtimestamp(int(sampled)/1_000_000,dt.timezone.utc).isoformat()
        rows.append(f'{at} {message}\n')
    return ''.join(rows).encode()


def _beacon_events(raw,item,broker,configured):
    if len(raw)>LIMIT or not raw.endswith(b'\n'):raise ValueError('Beacon log incomplete')
    rows=[]
    for line in raw.decode().splitlines():
        stamp,separator,message=line.partition(' ')
        if not separator:raise ValueError('Beacon log malformed')
        fields=json.loads(message)if message.startswith('{')else dict(v.split('=',1)for v in shlex.split(message)if '='in v)
        rows.append((timestamp(stamp),fields))
    if not any(row.get('msg')=='beacon starting'for _,row in rows):raise ValueError('Beacon process epoch missing')
    events={};epoch=max(timestamp(item['State']['StartedAt']),timestamp(broker['started_at']))
    for name in configured:
        connected=[index for index,(at,row)in enumerate(rows)if row.get('component')=='ingest'and row.get('broker')==name and row.get('msg')=='connected'and at>=epoch]
        if not connected:raise Unavailable('Beacon native OnConnect pending')
        index=connected[-1]
        for _,row in rows[index+1:]:
            if row.get('component')!='ingest'or row.get('broker')!=name:continue
            message=str(row.get('msg','')).lower()
            if 'subscribe'in message and('fail'in message or 'error'in message):raise ValueError('Beacon native subscription failed')
            if any(word in message for word in('connection lost','stopped')):raise Unavailable('Beacon native connection lost')
            if str(row.get('level','')).upper()in('ERROR','FATAL'):raise ValueError('Beacon native connection error')
        events[name]={'native_onconnect_at':rows[index][0],'suback_or_socket_claim':False}
    return events


def _backend_ready(value,now):
    stamp=value['ts'];status=value['status'];database=value['checks']['database'];mqtt=value['checks']['mqtt']['state']
    if type(stamp)not in(int,float)or not 0<=now-stamp/1000<=5:raise ValueError('backend native sample stale')
    if type(status)is not str or status not in('ready','degraded'):raise ValueError('backend native status unsupported')
    if type(database)is not bool:raise ValueError('backend native database status malformed')
    if type(mqtt)is not str or mqtt not in('starting','connected','disconnected','reconnecting','error'):
        raise ValueError('backend native MQTT status unsupported')
    if status!='ready'or not database or mqtt!='connected':raise Unavailable('backend native readiness pending')
    return {'ready':True,'mqtt_connected':True,'quiescence_claim':False}


def probe(app,item,snapshot,deadline):
    execute=lambda args,**kwargs:docker_probe(args,deadline,**kwargs)
    if app=='ukmesh':
        if _env(item).get('MQTT_INGEST_MODE','durable')!='durable':raise ValueError('normal durable intake required')
        return _backend_ready(_http('http://127.0.0.1:3100/readyz',deadline),time.time())
    if app=='corescope':
        value=read_core_connection(item,execute=execute,timeout=min(2,max(0,deadline-time.monotonic())))
        if not value['ready']or value['mqtt_connected']is False:raise Unavailable('Core native source pending')
        return value
    if app=='healthcheck':
        value=_http('http://127.0.0.1:3090/api/bootstrap',deadline)['mqtt']['connected']
        if type(value)is not bool:raise ValueError('Health Check native status malformed')
        if not value:raise Unavailable('Health Check native reconnect pending')
        return {'ready':True,'mqtt_connected':True,'suback_claim':False}
    if app!='beacon':raise ValueError('unsupported native readiness owner')
    native=read_beacon(item,execute=execute,timeout=min(2,max(0,deadline-time.monotonic())))
    rows=_http('http://127.0.0.1:3103/api/v1/brokers/',deadline)
    if not isinstance(rows,list)or len(rows)!=2 or any(set(row)!={'name','connected'}or type(row['connected'])is not bool for row in rows):raise ValueError('Beacon native broker status malformed')
    states={row['name']:row['connected']for row in rows}
    if set(states)!={'mqtt1','mqtt2'}:raise ValueError('Beacon native broker identities differ')
    configured={f'mqtt{i}'for i in(1,2)if _env(item).get(f'MQTT_BROKER_{i}_URL')}
    if any(states[name]is not(name in configured)for name in states):raise Unavailable('Beacon native broker status pending')
    events={}
    if configured:
        raw=_beacon_logs(item,deadline,execute)
        events=_beacon_events(raw,item,snapshot['broker'],configured)
    return {'ready':True,'process_started_at':native['process_started_at'],'native_events':events,
            'api_state_is_only_hint':True,'quiescence_claim':False}


def _identities(snapshot):
    owners={}
    for app,name in {**APPS,'healthcheck':HEALTH}.items():
        item=snapshot['containers'].get(name)
        if item and item['State']['Running']:
            expected={'ukmesh':('ukmesh-full','backend'),'corescope':('ukmesh-companions','corescope-ingestor'),
                      'beacon':('ukmesh-companions','beacon'),'healthcheck':('ukmesh-full','mesh-health-check')}[app]
            labels=item['Config'].get('Labels',{})
            if (labels.get('com.docker.compose.project'),labels.get('com.docker.compose.service'))!=expected:
                raise ValueError('native readiness owner role identity differs')
            owners[app]={'name':name,'id':item['Id'],'image':item['Image'],'pid':item['State']['Pid'],'started_at':item['State']['StartedAt']}
    return {'consumers':stable_identities(snapshot),'broker':snapshot['broker'],'owners':owners}


def _wait_running_consumers(snapshot_reader,initial,*,cancellation,clock=time.monotonic,sleep=time.sleep,probe_fn=probe,save=None):
    started=clock();deadline=started+SECONDS;identity=_identities(initial);reads=0;last={}
    result={'passed':False,'scope':'startup reconnect preparation ONLY, not target protection','max_seconds':SECONDS,
            'started_at':utc_now(),'quiescence_claim':False,'identities':identity,'skipped':{app:'missing-or-stopped-explicit'for app in{**APPS,'healthcheck':HEALTH}if app not in identity['owners']}}
    execute=lambda args,**options:docker_probe(args,deadline,**options)
    path=STATE/'runtime/startup-readiness'/('readiness-'+uuid.uuid4().hex+'.json')
    def emit():
        if save is None:write_json(path,result)
        else:save(result)
    def canceled_receipt():
        result.update(passed=False,cancellation=signal.Signals(cancellation['signal']).name,
                      error=result.get('error','InterruptedError'),duration_seconds=clock()-started)
        emit()
    cancellation['on_cancel']=canceled_receipt
    try:
        while True:
            reads+=1
            _check_cancellation(cancellation)
            if clock()>=deadline:raise TimeoutError('startup readiness60 expired')
            current=snapshot_reader(deadline,execute_fn=execute)
            if _identities(current)!=identity:raise ValueError('startup owner/ref/broker identity changed')
            try:
                last={app:probe_fn(app,current['containers'][row['name']],current,deadline)for app,row in identity['owners'].items()}
                if any(type(row)is not dict or type(row.get('ready'))is not bool for row in last.values()):raise ValueError('native readiness schema malformed')
                if not all(row['ready']for row in last.values()):raise Unavailable('native readiness pending')
            except Unavailable:
                last={};sleep(min(1,max(0,deadline-clock())));continue
            _check_cancellation(cancellation)
            if clock()>deadline:raise TimeoutError('startup readiness60 expired')
            final=snapshot_reader(deadline,execute_fn=execute)
            if _identities(final)!=identity:raise ValueError('startup owner/ref/broker identity changed')
            result.update(passed=True,observations=last);return result
    except BaseException as error:
        result.update(passed=False,error=type(error).__name__);raise
    finally:
        result.update(finished_at=utc_now(),duration_seconds=clock()-started,reads=reads)
        if cancellation['signal']is not None:
            canceled_receipt()
        else:
            if clock()>deadline:result['passed']=False
            emit()
        if clock()>deadline and not result.get('error'):
            result.update(passed=False,error='TimeoutError',duration_seconds=clock()-started)
            emit();raise TimeoutError('startup readiness finalization exceeded60')


def wait_running_consumers(snapshot_reader,initial,*,clock=time.monotonic,sleep=time.sleep,probe_fn=probe,save=None):
    with _cancellation_scope()as cancellation:
        return _wait_running_consumers(snapshot_reader,initial,cancellation=cancellation,
                                       clock=clock,sleep=sleep,probe_fn=probe_fn,save=save)
