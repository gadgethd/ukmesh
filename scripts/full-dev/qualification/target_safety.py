"""Target transition ownership. No application schema or broker storage policy."""
from contextlib import contextmanager
from copy import deepcopy
import json
import os
from pathlib import Path
import stat
import time
import uuid
import tempfile
from functools import wraps

from common import REPO,STATE,docker,service_manifest,write_json,utc_now
from consumer_refs import describe_consumer,requested_refs
from target_lease import target_lease
from target_observation import observe
from transition_protocol import new_intent,assess_intent,complete_intent

def write_intent(path,value):
    """The recovery journal must reach stable storage before either selection file."""
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    fd,temporary=tempfile.mkstemp(prefix='.target-intent-',dir=path.parent)
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(value,stream,sort_keys=True);stream.write('\n');stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)


INTENT=STATE/'runtime/target-intent.json'
VALUES={'UKMESH_DATABASE':'ukmesh','OWNER_DATABASE':'owner','BEACON_DATABASE':'beacon','CORESCOPE_DATA_DIR':'corescope'}


class InactiveChangingConsumerError(ValueError):
    """The previous store must be resumed before publishing a changed target."""


def target_values(values):
    if set(values)!=set(VALUES) or any(not isinstance(v,str) or not v or any(c in v for c in '\n\r\0') for v in values.values()):
        raise ValueError('complete literal target values required')
    return {family:values[key] for key,family in VALUES.items()}


def read_intent(path=INTENT):
    try:fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    except FileNotFoundError:return None
    with os.fdopen(fd,'rb') as source:
        info=os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size>1024**2:raise ValueError('target intent must be a bounded regular file')
        value=json.loads(source.read(1024**2+1))
    if value.get('version')!=1 or value.get('status') not in ('pending','applied'):
        raise ValueError('target intent is malformed')
    return value


def inspect_containers(deadline=None,*,execute_fn=None):
    def execute(args,**options):
        remaining=deadline-time.monotonic() if deadline is not None else 10
        if remaining<=0:raise TimeoutError('target inspection deadline')
        return (execute_fn or docker)(args,timeout=min(10,remaining),**options)
    ids=execute(['ps','-aq']).stdout.decode().split()
    items=json.loads(execute(['inspect',*ids]).stdout) if ids else []
    return {item['Name'].lstrip('/'):item for item in items}


def inspect_snapshot(deadline=None,*,execute_fn=None):
    containers=inspect_containers(deadline,execute_fn=execute_fn)
    finite={s.get('container',s['name']) for s in service_manifest()['services'] if s.get('kind')=='oneshot'}
    records={}
    for name,item in containers.items():
        project=item['Config'].get('Labels',{}).get('com.docker.compose.project')
        if project not in ('ukmesh-full','ukmesh-companions'):continue
        records[name]=describe_consumer(name,item,identity=True,finite=name in finite)
    broker=containers.get('ukmesh-sample-mqtt')
    if broker:
        labels=broker['Config'].get('Labels',{})
        if labels.get('com.docker.compose.project')!='ukmesh-sample' or labels.get('com.docker.compose.service')!='mqtt':
            raise ValueError('local broker identity differs')
        bound=broker['NetworkSettings'].get('Ports',{}).get('1883/tcp')
        if broker['State']['Running'] and bound!=[{'HostIp':'127.0.0.1','HostPort':'1884'}]:raise ValueError('local broker endpoint differs')
        broker_id={'container_id':broker['Id'],'image_id':broker['Image'],'started_at':broker['State']['StartedAt'],
                   'restarts':broker['RestartCount'],'running':broker['State']['Running']}
    else:broker_id={'missing':True}
    return {'consumers':records,'containers':containers,'broker':broker_id}


def assert_no_publishers(*,proc=Path('/proc')):
    entries=('replaySampleMqtt','soakDatabaseLookups','fullStackTraffic','fullStackRecovery')
    scripts={REPO/'backend'/tree/(name+extension) for tree,extension in (('src/tools','.ts'),('dist/tools','.js')) for name in entries}
    scripts|={REPO/'scripts/full-dev/interfaces.mjs',REPO/'scripts/full-dev/qualification/recovery.mjs',REPO/'scripts/full-dev/qualification/fixtures.py'}
    basenames={p.name for p in scripts}
    for entry in proc.iterdir():
        if not entry.name.isdigit():continue
        try:
            argv=[os.fsdecode(value) for value in (entry/'cmdline').read_bytes().split(b'\0') if value]
            candidates=[(i,value) for i,value in enumerate(argv) if Path(value).name in basenames]
            if not candidates:continue
            cwd=(entry/'cwd').resolve(strict=True)
            for index,value in candidates:
                actual=(Path(value) if Path(value).is_absolute() else cwd/value).resolve()
                if actual not in scripts:continue
                if actual.name=='fixtures.py':
                    if argv[index+1:index+2]!=['create']:continue
                raise ValueError('known native publisher or fixture provisioner is still active')
        except (FileNotFoundError,PermissionError):
            # A disappearing process cannot remain a live publisher. An
            # unreadable known candidate is not evidence of quiescence.
            if entry.exists():raise ValueError('publisher process identity unavailable')


def changing(snapshot,targets,scope=None):
    return {name for name,row in snapshot['consumers'].items() if row['refs'] and (scope is None or name in scope)
            and row['refs']!=requested_refs(row,targets)}


def check_start(values,*,scope=None,snapshot=inspect_snapshot,observe_fn=observe,path=INTENT,prepare_fn=None):
    requested=target_values(values);intent=read_intent(path)
    actual=snapshot()
    moved=changing(actual,requested,scope)
    if not moved:
        if intent and intent['status']=='pending':assess_intent(intent,actual['consumers'],requested)
        return {'passed':True,'kind':'same-store-bootstrap','observability_required':False,
                'note':'Missing/inactive intake is not asserted empty; same refs may bootstrap their existing stores.'}
    if not intent or intent['status']!='pending':raise ValueError('changing stores requires the owned pending selection intent')
    assessed=assess_intent(intent,actual['consumers'],requested)
    if not assessed['may_observe']:raise ValueError('changing inactive consumer; resume previous retained store')
    assert_no_publishers()
    if actual['broker'].get('running') is not True:raise ValueError('broker observation unavailable')
    prepared=prepare_fn(snapshot,actual)if prepare_fn is not None else None
    if prepare_fn is not None and(not isinstance(prepared,dict)or prepared.get('passed')is not True):
        raise ValueError('startup readiness preparation refused')
    proof=observe_fn(snapshot)
    if proof.get('passed') is not True:raise ValueError('target protection observation refused')
    return {'passed':True,'kind':'known-intent-protection','intent_token':intent['token'],'assessment':assessed,'protection':proof,
            **({'startup_readiness':prepared}if prepared is not None else {})}


def select(values,previous_values,commit,*,snapshot=inspect_snapshot,observe_fn=observe,path=INTENT,lock_path=None,
           selection_reader=None,requested_selection=None):
    kwargs={} if lock_path is None else {'path':lock_path}
    with target_lease(exclusive=True,**kwargs):
        requested=target_values(values);prior=read_intent(path);actual=snapshot()
        if requested_selection and requested_selection.get('token') and any(
            item['State']['Running']
            and (item['Config'].get('Labels') or {}).get('com.docker.compose.project') == 'ukmesh-lan'
            for item in actual.get('containers', {}).values()
        ):
            raise ValueError('stop the LAN addon before selecting a qualification fixture')
        if prior and prior['status']=='pending':
            intent=prior
            if intent.get('requested_values')!=values or intent.get('requested_selection')!=requested_selection:
                raise ValueError('resume only the exact pending target selection')
            if selection_reader:
                state=selection_reader()
                if state['values'] not in (intent['old_values'],intent['requested_values']) or state['selection'] not in (intent['old_selection'],intent['requested_selection']):
                    raise ValueError('foreign target files cannot be adopted by a pending intent')
            assessed=assess_intent(intent,actual['consumers'],requested)
        else:
            old=previous_values();old_targets=target_values(old)
            old_selection=selection_reader()['selection'] if selection_reader else None
            if old==values and old_selection==requested_selection and not changing(actual,requested):
                return {'passed':True,'services_restarted':False,'transition':'unchanged'}
            intent=new_intent(uuid.uuid4().hex,actual['consumers'],requested,old_targets=old_targets)
            intent.update(old_values=deepcopy(old),requested_values=deepcopy(values),created_at=utc_now(),
                          old_selection=old_selection,requested_selection=deepcopy(requested_selection))
            assessed=assess_intent(intent,actual['consumers'],requested)
        if changing(actual,requested):
            if not assessed['may_observe']:
                raise InactiveChangingConsumerError('stopped changing consumer; resume previous retained store')
            assert_no_publishers()
            proof=observe_fn(snapshot)
            if proof.get('passed') is not True:raise ValueError('target protection observation refused')
            intent['selection_protection']=proof
        intent['selection_complete']=False
        write_intent(path,intent)  # Intent precedes both files; retry can recover a torn settings publication.
        try:commit()
        except BaseException:
            intent['selection_error_at']=utc_now();write_intent(path,intent);raise
        intent['selection_complete']=True;write_intent(path,intent)
        return {'passed':True,'intent_token':intent['token'],'services_restarted':False}


def mark_complete(values,*,snapshot=inspect_snapshot,path=INTENT):
    intent=read_intent(path)
    if not intent or intent['status']!='pending':return {'passed':True,'transition':'none'}
    if intent.get('requested_values')!=values:raise ValueError('completion differs from pending selection')
    current=snapshot();manifest=service_manifest();required={s.get('container',s['name']) for s in manifest['services'] if s.get('required',True)}
    present=set();healthy=True
    for s in manifest['services']:
        if not s.get('required',True):continue
        name=s.get('container',s['name']);item=current['containers'].get(name)
        if item is None:healthy=False;continue
        present.add(name);state=item['State']
        if s.get('kind')=='oneshot':healthy&=state['Status']=='exited' and state['ExitCode']==0
        else:healthy&=state['Running'] and state.get('Health',{}).get('Status') in (None,'healthy') and not state['OOMKilled']
    result=complete_intent(intent,current['consumers'],{'required':required,'present':present,'passed':healthy})
    result['applied_at']=utc_now();write_intent(path,result)
    return {'passed':True,'transition':'applied','intent_token':result['token']}


@contextmanager
def publication_lease(*,path=INTENT,lock_path=None):
    kwargs={} if lock_path is None else {'path':lock_path}
    with target_lease(**kwargs):
        intent=read_intent(path)
        if intent and intent['status']=='pending':raise ValueError('finish the pending canonical target start before publishing')
        yield


def managed_publisher(function):
    @wraps(function)
    def guarded(*args,**kwargs):
        with publication_lease():return function(*args,**kwargs)
    return guarded
