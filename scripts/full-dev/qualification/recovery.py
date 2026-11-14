"""Bounded fault probes with native commit counts and unconditional cleanup."""
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from urllib.request import Request, urlopen
import uuid

from benchmarks import active_fixture, node_tool
from common import REPO, STATE, command, digest, docker, read_json, safe_error, sql, utc_now, write_json
from context import phase_context
from probes import http_get, inventory
from traffic import typed_counts, verify_typed
from fixtures import bundle


REQUIRED_CHECKS={'three-application-native-commits-before-faults','lost-real-api-database-sessions',
    'packet-lock-contention-and-aborted-lock-holder','owned-sqlite-write-contention',
    'duplicate-and-delayed-observer-delivery','local-provider-rate-limit-and-slow-response',
    'local-alert-503-durable-queue-and-restart','overlapping-health-learning-and-ingestion',
    'serial-owned-service-restarts-and-durable-state',
    'native-readonly-lifecycle-inventory-and-owned-compressed-boundary-proof',
    'native-lifecycle-restore-receipt-gates','final-ingest-queue-drain','complete-inventory-after-recovery',
    'source-settings-and-fixture-preserved'}


def maintenance_proof() -> dict:
    provenance=read_json(STATE/'maintenance-152-provenance.json')
    expected={'scripts/full-dev/test-maintenance.mjs',
              'backend/src/db/migrations/151_bounded_hypertable_maintenance.sql',
              'backend/src/db/migrations/152_bound_retention_calendar_intervals.sql'}
    if provenance.get('passed') is not True or set(provenance.get('sources',{}))!=expected \
            or any(digest(REPO/name)!=checksum for name,checksum in provenance['sources'].items()):
        raise ValueError('native maintenance proof source provenance differs')
    at=dt.datetime.fromisoformat(provenance['at'])
    if at.tzinfo is None or not 0<=(dt.datetime.now(dt.timezone.utc)-at).total_seconds()<=7*86400:
        raise ValueError('native maintenance proof timestamp is not current and timezone-aware')
    fixture=bundle(provenance['fixture_token'])
    main=next(row for row in fixture['databases'] if row['label']=='ukmesh')
    if fixture['kind']!='sample' or main['database']!=provenance['target']:
        raise ValueError('positive maintenance proof must retain its owned small sample target')
    definition=sql(main['container'],main['database'],main['user'],
        "SELECT pg_get_functiondef('ops.prune_expired_hypertable(regclass,interval)'::regprocedure)")
    image=json.loads(docker(['inspect','ukmesh-full-backend-1']).stdout)[0]['Image']
    if hashlib.sha256(definition.encode()).hexdigest()!=provenance['installed_function_sha256'] or image!=provenance['backend_image_id']:
        raise ValueError('native maintenance proof installed function or application image changed')
    receipt_path=Path(provenance['receipt']).resolve()
    if receipt_path.parent!=STATE.resolve() or digest(receipt_path)!=provenance['receipt_sha256']:
        raise ValueError('native maintenance receipt ownership or digest differs')
    receipt=read_json(receipt_path)
    required={'compressed-boundary-expired-removed','retained-side-preserved','raw-path-history-preserved',
              'compression-restored','rollback-protected-fixture','seed-cleanup-complete'}
    checks=receipt.get('checks',[])
    if receipt.get('passed') is not True or len(set(checks))!=len(checks) or not required.issubset(checks):
        raise ValueError('native maintenance positive behavior proof is incomplete')
    return {'passed':True,'provenance_sha256':digest(STATE/'maintenance-152-provenance.json'),
            'receipt_sha256':digest(receipt_path),'fixture_token':fixture['token'],'checks':receipt['checks']}


def provider(*, status: int = 0, delay_ms: int = 0) -> None:
    if type(status) is not int or status not in (0,429,503) or type(delay_ms) is not int or not 0<=delay_ms<=2000:
        raise ValueError('bounded local provider fault required')
    body=json.dumps({'status':status,'delayMs':delay_ms}).encode()
    with urlopen(Request('http://127.0.0.1:3107/failure',data=body,headers={'Content-Type':'application/json'}),timeout=5) as response:
        if response.status!=204:
            raise RuntimeError('local provider fault controller rejected the setting')


def wait_for(check, *, seconds: float = 120):
    started=time.monotonic()
    last=None
    while time.monotonic()-started<=seconds:
        try:
            last=check()
            if last:
                return {'passed':True,'elapsed_seconds':time.monotonic()-started}
        except Exception:
            pass
        time.sleep(.5)
    raise RuntimeError('native recovery did not complete within its 120-second deadline')


def queues_drained() -> bool:
    body,_=http_get('http://127.0.0.1:9091/metrics')
    metrics={parts[0]:float(parts[1]) for line in body.decode().splitlines()
             if len(parts:=line.split())==2 and not line.startswith('#')}
    return all(metrics.get(key)==0 for key in ('meshcore_packet_batch_queue_depth','meshcore_mqtt_ingest_queue_depth','meshcore_mqtt_ingest_active'))


def driver(directory: Path, fixture: dict, action: str, *, manifest: Path | None = None, cycles: int = 1) -> tuple[dict,Path]:
    if active_fixture()['token']!=fixture['token']:
        raise ValueError('owned fixture changed during recovery')
    name=action+'-'+uuid.uuid4().hex[:8]
    manifest=manifest or directory/(name+'-traffic.json')
    result=directory/(name+'-result.json')
    request=directory/(name+'-request.json')
    write_json(request,{'action':action,'seed':uuid.uuid4().hex,'manifest':str(manifest),'result':str(result),
                        **({'cycles':cycles} if cycles>1 else {})})
    database=next(row['database'] for row in fixture['databases'] if row['label']=='ukmesh')
    invocation=['env','DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/'+database,
                'node','--max-old-space-size=128','--import',str(REPO/'backend/node_modules/tsx/dist/loader.mjs'),
                str(REPO/'scripts/full-dev/qualification/recovery.mjs'),str(request)]
    with (directory/(name+'.log')).open('xb') as log:
        Path(log.name).chmod(0o600)
        completed=command(invocation,output_stream=log,check=False,timeout=45)
    if completed.returncode:
        # Error text may include a raw local envelope. Keep it in protected evidence.
        error=directory/(name+'.stderr')
        error.write_bytes(completed.stderr);error.chmod(0o600)
        raise RuntimeError(f'owned recovery native driver failed: {action}')
    return read_json(result),manifest


def commit_check(directory: Path, fixture: dict, action: str='publish') -> tuple[dict,dict,Path]:
    native,path=driver(directory,fixture,action)
    plan=read_json(path)
    result=verify_typed(plan,fixture)
    if not native['passed'] or not result['passed']:
        raise RuntimeError('native application commit counts did not recover')
    drain=wait_for(queues_drained)
    return {'passed':True,'native':native,'commits':result,'queue_drain':drain},plan,path


def recovery(run: Path) -> dict:
    fixture=active_fixture()
    if fixture['kind'] not in ('1x','10x') or not inventory(run)['passed']:
        raise ValueError('recovery requires the complete default stack on an owned retained lookup fixture')
    before=phase_context(run)
    directory=run/('recovery-'+uuid.uuid4().hex[:8]);directory.mkdir(mode=0o700)
    checks=[]
    def record(name, operation):
        try:
            value=operation()
            checks.append({'name':name,**value})
        except Exception as error:
            checks.append({'name':name,'passed':False,'error':safe_error(error)})

    try:
        provider()
        committed,plan,path=commit_check(directory,fixture)
        checks.append({'name':'three-application-native-commits-before-faults',**committed})
        def lost_sessions():
            json.loads(http_get('http://127.0.0.1:3100/api/nodes/map?network=ukmesh')[0])
            native,_=driver(directory,fixture,'lost-connections')
            restored=wait_for(lambda: bool(json.loads(http_get('http://127.0.0.1:3100/api/nodes/map?network=ukmesh')[0])))
            return {'passed':native['passed'] and restored['passed'],'native':native,'reconnected':restored}
        record('lost-real-api-database-sessions',lost_sessions)
        record('packet-lock-contention-and-aborted-lock-holder',lambda:commit_check(directory,fixture,'lock')[0])
        def sqlite_contention():
            file=Path(fixture['corescope_data_dir'])/'analyzer.db'
            with sqlite3.connect(file,timeout=5) as writer, ThreadPoolExecutor(max_workers=1) as executor:
                writer.execute('BEGIN IMMEDIATE')
                work=executor.submit(driver,directory,fixture,'publish')
                native,path=work.result(timeout=45)
                time.sleep(1)
                blocked=typed_counts(read_json(path),fixture)
                held=blocked['corescope']['observations']==0 and blocked['ukmesh']['packets']>0
                writer.rollback()
            plan=read_json(path)
            proof=verify_typed(plan,fixture)
            return {'passed':held and native['passed'] and proof['passed'],'held_write_lock_seconds':1,'commits':proof}
        record('owned-sqlite-write-contention',sqlite_contention)
        def redelivery():
            native,_=driver(directory,fixture,'redeliver',manifest=path)
            adjusted=json.loads(json.dumps(plan))
            for application,key in (('ukmesh','packets'),('ukmesh','ledger'),('corescope','observations'),('beacon','observations')):
                adjusted['expected'][application][key]+=1
            proof=verify_typed(adjusted,fixture)
            return {'passed':native['passed'] and proof['passed'],'native':native,'commits':proof}
        record('duplicate-and-delayed-observer-delivery',redelivery)
        def slow_tagger():
            main=next(row for row in fixture['databases'] if row['label']=='ukmesh')
            provider(status=429)
            try:
                _,path=driver(directory,fixture,'publish')
                packet=read_json(path)['channelHashes'][0]
                pending=wait_for(lambda:int(sql(main['container'],main['database'],main['user'],
                    "SELECT count(*) FROM ops.tagger_retry WHERE packet_hash='"+packet+"'"))>0)
                provider(delay_ms=2000)
                tagged=wait_for(lambda:int(sql(main['container'],main['database'],main['user'],
                    "SELECT count(*) FROM mesh.message_tags WHERE packet_hash='"+packet+"' AND latency_ms>=1900"))==1)
                proof=verify_typed(read_json(path),fixture)
                cleared=int(sql(main['container'],main['database'],main['user'],
                    "SELECT count(*) FROM ops.tagger_retry WHERE packet_hash='"+packet+"'"))==0
                return {'passed':pending['passed'] and tagged['passed'] and proof['passed'] and cleared,
                        'durable_retry_observed':pending,'slow_response_recovered':tagged,'retry_cleared':cleared,'commits':proof}
            finally:provider()
        record('local-provider-rate-limit-and-slow-response',slow_tagger)
        def alert_delivery():
            probe=uuid.uuid4().hex
            provider(status=503)
            try:
                body=json.dumps([{'labels':{'alertname':'OwnedFullStackProbe-'+probe,'severity':'info'},
                                  'annotations':{'summary':'Owned local recovery proof'}}]).encode()
                with urlopen(Request('http://127.0.0.1:9095/api/v2/alerts',data=body,
                                     headers={'Content-Type':'application/json'}),timeout=10) as response:
                    if response.status!=200:raise RuntimeError('local Alertmanager rejected the recovery alert')
                queued=wait_for(lambda:json.loads(http_get('http://127.0.0.1:3106/healthz')[0])['pending_count']>0)
                docker(['restart','ukmesh-full-alert-receiver-1'],timeout=30)
                provider()
                delivered=wait_for(lambda:probe in json.loads(http_get('http://127.0.0.1:3107/counts')[0]).get('alertProbes',[]))
                drained=wait_for(lambda:json.loads(http_get('http://127.0.0.1:3106/healthz')[0])['pending_count']==0)
                return {'passed':queued['passed'] and delivered['passed'] and drained['passed'],
                        'durable_queue_observed':queued,'delivery_after_receiver_restart':delivered,'queue_drained':drained}
            finally:provider()
        record('local-alert-503-durable-queue-and-restart',alert_delivery)
        def overlapping_jobs():
            main=next(row for row in fixture['databases'] if row['label']=='ukmesh')
            started=sql(main['container'],main['database'],main['user'],"SELECT now()::text")
            with ThreadPoolExecutor(max_workers=1) as executor:
                publishing=executor.submit(driver,directory,fixture,'publish',cycles=6)
                docker(['restart','ukmesh-full-health-worker-1','ukmesh-full-path-learning-worker-1'],timeout=60)
                native,path=publishing.result(timeout=45)
            commits=verify_typed(read_json(path),fixture)
            def completed():
                return int(sql(main['container'],main['database'],main['user'],
                    "SELECT (EXISTS(SELECT 1 FROM ops.worker_health_snapshots WHERE ts>'"+started+"'::timestamptz) "
                    "AND EXISTS(SELECT 1 FROM analytics.path_model_calibration WHERE updated_at>'"+started+"'::timestamptz))::int"))==1
            jobs=wait_for(completed)
            return {'passed':native['passed'] and commits['passed'] and jobs['passed'],
                    'health_snapshot_and_learning_publication':jobs,'concurrent_typed_commits':commits}
        record('overlapping-health-learning-and-ingestion',overlapping_jobs)
        def restarts():
            preserved=typed_counts(plan,fixture)
            timings=[]
            for name in ('ukmesh-sample-db','ukmesh-companions-beacon-db-1','ukmesh-sample-redis','ukmesh-sample-mqtt',
                         'ukmesh-full-backend-1','ukmesh-full-tagger-worker-1','ukmesh-companions-corescope-ingestor-1','ukmesh-companions-beacon-1'):
                start=time.monotonic()
                docker(['restart',name],timeout=60)
                def healthy():
                    state=json.loads(docker(['inspect',name]).stdout)[0]['State']
                    return state['Running'] and state.get('Health',{}).get('Status','healthy')=='healthy'
                wait_for(healthy)
                timings.append({'container':name,'elapsed_seconds':time.monotonic()-start})
            proof,_,_=commit_check(directory,fixture)
            return {'passed':typed_counts(plan,fixture)==preserved and proof['passed'],
                    'committed_rows_preserved':typed_counts(plan,fixture)==preserved,'restarts':timings,'new_work':proof}
        record('serial-owned-service-restarts-and-durable-state',restarts)
        def lifecycle_inventory():
            database=next(row['database'] for row in fixture['databases'] if row['label']=='ukmesh')
            log=directory/'native-lifecycle-inventory.log'
            code=node_tool('dataLifecycle',[],database,log,timeout=120)
            evidence=maintenance_proof()
            passed=code==0 and '[data-lifecycle] inventory' in log.read_text() and evidence['passed']
            return {'passed':passed,'read_only_inventory':code==0,'positive_cleanup':evidence}
        record('native-readonly-lifecycle-inventory-and-owned-compressed-boundary-proof',lifecycle_inventory)
        record('native-lifecycle-restore-receipt-gates',lambda:driver(directory,fixture,'lifecycle')[0])
        record('final-ingest-queue-drain',lambda:wait_for(queues_drained))
        record('complete-inventory-after-recovery',lambda:inventory(directory))
    finally:
        provider()
    after=phase_context(run)
    # Container IDs/timestamps may change. Code, image IDs, settings and ownership may not.
    stable=before==after
    checks.append({'name':'source-settings-and-fixture-preserved','passed':stable})
    result={'at':utc_now(),'passed':bool(checks) and all(row['passed'] for row in checks),
            'checks':checks,'fixture_token':fixture['token'],'context':after,'attempt':str(directory)}
    write_json(run/'recovery.json',result)
    return result
