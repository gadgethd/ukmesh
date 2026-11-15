"""App-owned intake observations, never a database or public API adapter."""
import datetime as dt
import json
import math
from pathlib import Path,PurePosixPath
import re
import sys
import time
import yaml

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'full-dev/qualification'))
from common import docker

LIMIT=8*1024**2


def timestamp(value):
    if not isinstance(value,str):raise ValueError('app timestamp missing')
    parsed=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('app timestamp is not timezone-aware')
    return parsed.timestamp()


def identity(item,service):
    labels=item['Config'].get('Labels',{});state=item['State']
    if labels.get('com.docker.compose.project')!='ukmesh-companions' or labels.get('com.docker.compose.service')!=service \
            or state.get('Running') is not True or type(state.get('Pid')) is not int or state['Pid']<=0 \
            or not item.get('Id') or not item.get('Image'):
        raise ValueError('companion intake process identity is unavailable')
    timestamp(state['StartedAt'])
    return {'container_id':item['Id'],'image_id':item['Image'],'container_started_at':state['StartedAt']}


def core_evidence(document,process,actual,now):
    if not isinstance(document,dict):raise ValueError('Core stats document is malformed')
    state=document.get('ingest_state')
    if not isinstance(state,dict):raise ValueError('Core ingest state unsupported or missing')
    for key in ('version','generation','pid','startTicks','pending','active','outstanding','droppedTotal'):
        if type(state.get(key)) is not int or state[key]<0:raise ValueError('Core ingest counter missing or malformed')
    if state['version']!=1 or not 1<=state['generation']<2**64 or state['startTicks']>=2**64 \
            or state['pid']<1 or state['startTicks']<1 or state['outstanding']!=state['pending']+state['active']:
        raise ValueError('Core ingest state version or coherence differs')
    if type(state.get('ready')) is not bool or type(state.get('accepting')) is not bool \
            or state.get('drainState') not in ('running','draining','drained','incomplete'):
        raise ValueError('Core intake admission state missing')
    if type(document.get('write_errors')) is not int or document['write_errors']<0:
        raise ValueError('Core write outcome observation missing')
    sampled=timestamp(document.get('sampledAt'))
    # Core's existing IngestorStatsStaleThreshold is five seconds.
    if not 0<=now-sampled<=5 or sampled<timestamp(actual['container_started_at']):
        raise ValueError('Core intake observation is stale or belongs to an old container')
    if process['pid']!=state['pid'] or process['start_ticks']!=state['startTicks'] \
            or not process['argv'] or process['argv'][0]!='/bin/companions/corescope-ingestor':
        raise ValueError('Core stats process generation differs from the live ingestor')
    quiescent=state['ready'] and state['outstanding']==0 and (
        state['accepting'] and state['drainState']=='running' or not state['accepting'] and state['drainState']=='drained')
    return {'app':'corescope',**actual,'sampled_at':document['sampledAt'],'observed_at':now,
            'process':{'pid':state['pid'],'start_ticks':state['startTicks']},'state':{key:state[key] for key in ('version','generation','pid','startTicks','ready','accepting','pending','active','outstanding','drainState','droppedTotal')},
            'write_errors':document.get('write_errors'),'quiescent':bool(quiescent)}


def namespace_process(raw,pid):
    stat,separator,argv=raw.partition(b'\n')
    if not separator:raise ValueError('Core namespace process evidence missing')
    fields=stat[stat.rfind(b')')+2:].split()
    return {'pid':int(stat.split(b' ',1)[0]),'start_ticks':int(fields[19]),'argv':[x.decode() for x in argv.split(b'\0') if x]}


def read_core(item,*,execute=docker,now=time.time,timeout=2):
    actual=identity(item,'corescope-ingestor')
    env=dict(v.split('=',1) for v in item['Config'].get('Env',[]))
    path=env.get('CORESCOPE_INGESTOR_STATS','/tmp/corescope-ingestor-stats.json')
    if not PurePosixPath(path).is_absolute() or '..' in PurePosixPath(path).parts or '\0' in path:
        raise ValueError('Core stats path must be an absolute configured file')
    result=execute(['exec',item['Id'],'sh','-ceu','test -f "$1"; test ! -L "$1"; head -c 8388609 -- "$1"','core-intake',path],timeout=timeout)
    if result.returncode:raise RuntimeError('Core stats transport failed')
    raw=result.stdout
    if len(raw)>LIMIT:raise ValueError('Core stats exceeds the observation bound')
    document=json.loads(raw);state=document.get('ingest_state',{})
    pid=state.get('pid')
    if type(pid) is not int or pid<1:raise ValueError('Core owner PID missing')
    command='head -c 4096 /proc/$1/stat; head -c 4096 /proc/$1/cmdline'
    result=execute(['exec',item['Id'],'sh','-ceu',command,'core-owner-pid',str(pid)],timeout=timeout)
    if result.returncode:raise RuntimeError('Core process transport failed')
    proc=result.stdout
    return core_evidence(document,namespace_process(proc,pid),actual,now())


def metric(text,name):
    values=re.findall(r'^'+re.escape(name)+r' ([^\s]+)[ \t]*$',text,re.M)
    if len(values)!=1:raise ValueError('required companion metric missing or ambiguous')
    value=float(values[0])
    if not math.isfinite(value) or value<0:raise ValueError('companion metric is invalid')
    return value


def beacon_evidence(text,actual,now):
    depth=metric(text,'beacon_ingest_queue_depth');dropped=metric(text,'beacon_ingest_dropped_total')
    start=metric(text,'process_start_time_seconds')
    if not depth.is_integer() or not dropped.is_integer() or not timestamp(actual['container_started_at'])-1<=start<=now:
        raise ValueError('Beacon queue or process observation differs')
    return {'app':'beacon',**actual,'observed_at':now,'process_started_at':start,
            'state':{'outstanding':int(depth),'droppedTotal':int(dropped)},'quiescent':depth==0}


def beacon_url(url):
    match=isinstance(url,str) and re.fullmatch(r'http://127\.0\.0\.1:([1-9][0-9]{0,4})/metrics',url)
    if not match or not 0<int(match[1])<=65535:raise ValueError('Beacon operator endpoint must be valid container loopback')
    return url


def beacon_address(path=None):
    config=yaml.safe_load((path or Path(__file__).with_name('beacon.yaml')).read_text())
    address=config.get('server',{}).get('admin_listen')
    return beacon_url('http://'+str(address)+'/metrics')


def read_beacon(item,*,execute=docker,now=time.time,timeout=2,address=None):
    actual=identity(item,'beacon');url=beacon_url(address) if address is not None else beacon_address()
    # The pinned companion image has Bash. pipefail keeps a late curl failure
    # from being hidden by a successful bounded head with a parseable prefix.
    command='curl --fail --silent --max-time "$2" "$1" | head -c 8388609'
    result=execute(['exec',item['Id'],'bash','-o','pipefail','-ceu',command,'beacon-intake',url,str(timeout)],timeout=timeout)
    if result.returncode:raise RuntimeError('Beacon metrics transport failed')
    raw=result.stdout
    if len(raw)>LIMIT:raise ValueError('Beacon metrics exceeds the observation bound')
    return beacon_evidence(raw.decode(),actual,now())
