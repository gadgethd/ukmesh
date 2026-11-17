"""Owned synthetic readiness/intent boundaries; no service or database calls."""
from copy import deepcopy
import datetime as dt
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import startup_readiness as r
import target_safety as safety
from transition_protocol import new_intent
from test_target_protocol import records
from ingest_state import read_core_connection


class Clock:
    def __init__(self):self.value=0
    def __call__(self):return self.value
    def sleep(self,seconds):self.value+=seconds


def snapshot():
    name=r.APPS['ukmesh'];record={'refs':{'ukmesh':'old'},'container_id':'u','image_id':'i','pid':10,'started_at':'2026-11-15T10:00:00Z','running':True}
    return {'consumers':{name:record},'broker':{'container_id':'b','running':True,'started_at':'2026-11-15T10:00:01Z'},
        'containers':{name:{'Id':'u','Image':'i','Config':{'Labels':{'com.docker.compose.project':'ukmesh-full','com.docker.compose.service':'backend'}},'State':{'Running':True,'Pid':10,'StartedAt':record['started_at']}}}}


class BoundedPreparation(unittest.TestCase):
    def wait(self,actual,probe,clock=None,reader=None):
        clock=clock or Clock();saved=[]
        result=r.wait_running_consumers(reader or(lambda *a,**k:deepcopy(actual)),actual,clock=clock,sleep=clock.sleep,probe_fn=probe,save=saved.append)
        self.assertTrue(saved[-1]['passed']);return result
    def test_transient_then_native_ready_uses_one_decreasing_budget(self):
        clock=Clock();remaining=[]
        def probe(app,item,state,deadline):
            remaining.append(deadline-clock())
            if len(remaining)<3:raise r.Unavailable('503')
            return {'ready':True,'outstanding':8,'generation':12}
        value=self.wait(snapshot(),probe,clock)
        self.assertEqual(remaining,[60,59,58]);self.assertEqual(value['reads'],3)
        self.assertFalse(value['quiescence_claim']);self.assertEqual(value['observations']['ukmesh']['outstanding'],8)
    def test_never_ready_expires_without_a_new_window(self):
        clock=Clock();saved=[];deadlines=[]
        def unavailable(app,item,state,deadline):deadlines.append(deadline);raise r.Unavailable('503')
        with self.assertRaises(TimeoutError):r.wait_running_consumers(lambda *a,**k:snapshot(),snapshot(),clock=clock,sleep=clock.sleep,probe_fn=unavailable,save=saved.append)
        self.assertEqual(set(deadlines),{60});self.assertEqual(clock(),60);self.assertFalse(saved[-1]['passed'])
    def test_schema_error_is_not_polled_as_unavailability(self):
        calls=[]
        def malformed(*args):calls.append(1);raise ValueError('native schema')
        with self.assertRaisesRegex(ValueError,'schema'):self.wait(snapshot(),malformed)
        self.assertEqual(calls,[1])
    def test_owner_ref_broker_and_running_set_drift_refuse(self):
        for change in ('ref','broker','pid','running'):
            initial=snapshot();current=deepcopy(initial);name=r.APPS['ukmesh']
            if change=='ref':current['consumers'][name]['refs']={'ukmesh':'foreign'}
            elif change=='broker':current['broker']['container_id']='replacement'
            elif change=='pid':current['containers'][name]['State']['Pid']=11
            else:current['containers'][name]['State']['Running']=False
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'identity changed'):
                self.wait(initial,lambda *a:{'ready':True},reader=lambda *a,**k:current)
    def test_final_snapshot_and_elapsed_probe_are_checked(self):
        clock=Clock()
        def slow(*args):clock.value=61;return {'ready':True}
        with self.assertRaises(TimeoutError):self.wait(snapshot(),slow,clock)
    def test_final_evidence_io_cannot_extend_the_readiness_deadline(self):
        clock=Clock();saved=[]
        def save(value):saved.append(deepcopy(value));clock.value=61
        with self.assertRaises(TimeoutError):r.wait_running_consumers(lambda *a,**k:snapshot(),snapshot(),clock=clock,sleep=clock.sleep,probe_fn=lambda *a:{'ready':True},save=save)
        self.assertFalse(saved[-1]['passed'])
    def test_first_cancel_during_normal_final_save_persists_failed_then_refuses(self):
        for sig in(signal.SIGTERM,signal.SIGINT):
            saved=[];clock=Clock()
            def save(value):
                saved.append(deepcopy(value))
                if len(saved)==1:
                    os.kill(os.getpid(),sig);os.kill(os.getpid(),sig)
            with self.subTest(signal=sig),self.assertRaises(InterruptedError):
                r.wait_running_consumers(lambda *a,**k:snapshot(),snapshot(),clock=clock,sleep=clock.sleep,probe_fn=lambda *a:{'ready':True},save=save)
            self.assertFalse(saved[-1]['passed']);self.assertEqual(saved[-1]['error'],'InterruptedError')
            self.assertEqual(saved[-1]['cancellation'],signal.Signals(sig).name)
    def test_first_cancel_at_exit_mask_transition_is_not_lost(self):
        original=signal.pthread_sigmask;blocks=0;saved=[];clock=Clock()
        def masking(how,signals):
            nonlocal blocks
            if how==signal.SIG_BLOCK:
                blocks+=1
                if blocks==2:os.kill(os.getpid(),signal.SIGTERM)
            return original(how,signals)
        with patch.object(r.signal,'pthread_sigmask',side_effect=masking),self.assertRaises(InterruptedError):
            r.wait_running_consumers(lambda *a,**k:snapshot(),snapshot(),clock=clock,sleep=clock.sleep,probe_fn=lambda *a:{'ready':True},save=lambda value:saved.append(deepcopy(value)))
        self.assertFalse(saved[-1]['passed']);self.assertEqual(saved[-1]['cancellation'],'SIGTERM')
    def test_cancellation_is_not_caught_as_transient(self):
        def canceled(*args):raise InterruptedError('canceled')
        with self.assertRaises(InterruptedError):self.wait(snapshot(),canceled)


class IntentOrdering(unittest.TestCase):
    OLD={'UKMESH_DATABASE':'old','OWNER_DATABASE':'owner-old','BEACON_DATABASE':'beacon-old','CORESCOPE_DATA_DIR':'/owned/old'}
    NEW={'UKMESH_DATABASE':'new','OWNER_DATABASE':'owner-new','BEACON_DATABASE':'beacon-new','CORESCOPE_DATA_DIR':'/owned/new'}
    def context(self):
        state={'consumers':records(),'broker':{'running':True}}
        intent=new_intent('owned',state['consumers'],safety.target_values(self.NEW),old_targets=safety.target_values(self.OLD))
        return state,intent
    def run_case(self,state,intent,prepare,observe):
        with tempfile.TemporaryDirectory()as directory,patch.object(safety,'assert_no_publishers'):
            path=Path(directory)/'intent.json'
            if intent is not None:safety.write_intent(path,intent)
            return safety.check_start(self.NEW,snapshot=lambda:state,path=path,prepare_fn=prepare,observe_fn=observe)
    def test_preparation_before_exactly_one_unchanged_protection(self):
        state,intent=self.context();calls=[]
        value=self.run_case(state,intent,lambda *a:calls.append('ready')or {'passed':True},lambda *a:calls.append('strict45')or {'passed':True})
        self.assertEqual(calls,['ready','strict45']);self.assertEqual(value['kind'],'known-intent-protection')
    def test_cold_and_same_store_resume_do_not_wait(self):
        state,intent=self.context()
        for row in state['consumers'].values():row['refs']={key:safety.target_values(self.NEW)[key]for key in row['refs']};row['running']=False
        for current,owned in ((state,intent),({'consumers':{},'broker':{}},None)):
            value=self.run_case(current,owned,lambda *a:self.fail('must not prepare'),lambda *a:self.fail('must not observe'))
            self.assertEqual(value['kind'],'same-store-bootstrap')
    def test_stopped_old_refuses_before_readiness_but_requested_partial_resumes(self):
        state,intent=self.context();state['consumers']['core']['running']=False
        with self.assertRaisesRegex(ValueError,'inactive'):self.run_case(state,intent,lambda *a:self.fail('prepare'),lambda *a:self.fail('observe'))
        state['consumers']['core']['refs']=deepcopy(intent['consumers']['core']['requested'])
        self.assertTrue(self.run_case(state,intent,lambda *a:{'passed':True},lambda *a:{'passed':True})['passed'])
    def test_false_or_malformed_preparation_never_reaches_strict_or_apply(self):
        state,intent=self.context()
        for value in ({'passed':False},{},None):
            with self.assertRaisesRegex(ValueError,'preparation refused'):self.run_case(state,intent,lambda *a:value,lambda *a:self.fail('must not observe'))
    def test_readiness_does_not_qualify_backlog_or_swallow_failure(self):
        state,intent=self.context();calls=[]
        def strict(*args):calls.append('strict');raise ValueError('not quiescent')
        with self.assertRaisesRegex(ValueError,'quiescent'):self.run_case(state,intent,lambda *a:{'passed':True,'outstanding':9},strict)
        self.assertEqual(calls,['strict'])


class NativeContracts(unittest.TestCase):
    def logs(self,later=''):
        return ('2026-11-15T10:00:00Z time=x level=INFO msg="beacon starting"\n'
            '2026-11-15T10:00:05Z time=x level=INFO msg=connected component=ingest broker=mqtt1\n'+later).encode()
    def test_backend_malformed_types_and_unknown_enums_are_fatal(self):
        base={'ts':1000,'status':'ready','checks':{'database':True,'mqtt':{'state':'connected'}}}
        cases=[('database',value)for value in(1,'true',None,[])]+[('status',value)for value in('unknown',True,None,[])]+[('mqtt',value)for value in('up',True,None,{})]
        for key,value in cases:
            document=deepcopy(base)
            if key=='database':document['checks']['database']=value
            elif key=='mqtt':document['checks']['mqtt']['state']=value
            else:document['status']=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):r._backend_ready(document,1)
    def test_backend_declared_temporary_states_remain_unavailable(self):
        base={'ts':1000,'status':'ready','checks':{'database':True,'mqtt':{'state':'connected'}}}
        self.assertTrue(r._backend_ready(base,1)['ready'])
        for key,value in[('status','degraded'),('database',False)]+[('mqtt',state)for state in('starting','disconnected','reconnecting','error')]:
            document=deepcopy(base)
            if key=='database':document['checks']['database']=value
            elif key=='mqtt':document['checks']['mqtt']['state']=value
            else:document['status']=value
            with self.subTest(key=key,value=value),self.assertRaises(r.Unavailable):r._backend_ready(document,1)
    def test_beacon_api_hint_needs_onconnect_in_current_broker_epoch(self):
        item={'State':{'StartedAt':'2026-11-15T10:00:00Z'}}
        self.assertTrue(r._beacon_events(self.logs(),item,{'started_at':'2026-11-15T10:00:04Z'},{'mqtt1'}))
        with self.assertRaises(r.Unavailable):r._beacon_events(self.logs(),item,{'started_at':'2026-11-15T10:00:06Z'},{'mqtt1'})
    def test_beacon_lost_is_transient_but_subscribe_error_is_structural(self):
        item={'State':{'StartedAt':'2026-11-15T10:00:00Z'}};broker={'started_at':'2026-11-15T10:00:04Z'}
        for message,error in (('connection lost, will reconnect',r.Unavailable),('subscribe failed',ValueError)):
            raw=self.logs(f'2026-11-15T10:00:06Z time=x level=WARN msg="{message}" component=ingest broker=mqtt1\n')
            with self.assertRaises(error):r._beacon_events(raw,item,broker,{'mqtt1'})
    def test_actual_stderr_only_native_log_stream_is_merged(self):
        raw=self.logs();value=r._run([sys.executable,'-c','import sys;sys.stderr.buffer.write('+repr(raw)+')'],time.monotonic()+5,merge_stderr=True)
        self.assertEqual(value.returncode,0);self.assertEqual(value.stdout,raw);self.assertEqual(value.stderr,b'')
        with patch.object(r,'_run',return_value=value)as run:r.docker_probe(['logs','owned'],time.monotonic()+5,merge_stderr=True)
        self.assertTrue(run.call_args.kwargs['merge_stderr'])
    def test_core_actual_full_document_sources_accept_backlog(self):
        with tempfile.TemporaryDirectory()as directory:
            config=Path(directory)/'config.json';config.write_text(json.dumps({'mqttSources':[{'name':'owned','broker':'tcp://broker:1883'}]}))
            started='2026-11-15T10:00:00+00:00';now=dt.datetime(2026,11,15,10,0,3,tzinfo=dt.timezone.utc).timestamp()
            item={'Id':'core','Image':'image','State':{'Running':True,'Pid':90,'StartedAt':started},'Config':{'Labels':{'com.docker.compose.project':'ukmesh-companions','com.docker.compose.service':'corescope-ingestor'},'Env':[]},'Mounts':[{'Type':'bind','Destination':'/config/config.json','Source':str(config)}]}
            state={'version':1,'generation':2,'pid':77,'startTicks':100,'ready':True,'accepting':True,'pending':5,'active':2,'outstanding':7,'drainState':'running','droppedTotal':3}
            doc={'ingest_state':state,'sampledAt':'2026-11-15T10:00:02+00:00','write_errors':2,'source_statuses':[{'name':'owned','broker':'tcp://broker:1883','connected':True}]}
            proc=('77 (core) '+' '.join(['S']+['0']*18+['100'])+'\n').encode()+b'/bin/companions/corescope-ingestor\0'
            def execute(args,**kwargs):return SimpleNamespace(returncode=0,stdout=json.dumps(doc).encode()if args[-2]=='core-intake'else proc)
            self.assertTrue(read_core_connection(item,execute=execute,now=lambda:now)['mqtt_connected'])
            doc['source_statuses'][0]['connected']=False
            self.assertFalse(read_core_connection(item,execute=execute,now=lambda:now)['mqtt_connected'])
            doc['source_statuses'][0]['broker']='tcp://foreign:1883'
            with self.assertRaisesRegex(ValueError,'identity'):read_core_connection(item,execute=execute,now=lambda:now)

    def test_core_zero_mqtt_omission_preserves_public_modes_only(self):
        omitted=object();source={'name':'owned','broker':'tcp://broker:1883'}
        with tempfile.TemporaryDirectory()as directory:
            config=Path(directory)/'config.json'
            item={'Id':'core','Image':'image','State':{'Running':True,'Pid':90,'StartedAt':'2026-11-15T10:00:00+00:00'},'Config':{'Labels':{'com.docker.compose.project':'ukmesh-companions','com.docker.compose.service':'corescope-ingestor'},'Env':[]},'Mounts':[{'Type':'bind','Destination':'/config/config.json','Source':str(config)}]}
            state={'version':1,'generation':2,'pid':77,'startTicks':100,'ready':True,'accepting':True,'pending':0,'active':0,'outstanding':0,'drainState':'running','droppedTotal':0}
            proc=('77 (core) '+' '.join(['S']+['0']*18+['100'])+'\n').encode()+b'/bin/companions/corescope-ingestor\0'
            now=dt.datetime(2026,11,15,10,0,3,tzinfo=dt.timezone.utc).timestamp()
            cases=[([],omitted,True),([],[],True),([],None,False),([],[{**source,'connected':True}],False),([source],omitted,False),([source],[],False)]
            for configured,statuses,valid in cases:
                config.write_text(json.dumps({'publicProjection':not configured,'mqttSources':configured}))
                doc={'ingest_state':state,'sampledAt':'2026-11-15T10:00:02+00:00','write_errors':0}
                if statuses is not omitted:doc['source_statuses']=statuses
                def execute(args,**kwargs):return SimpleNamespace(returncode=0,stdout=json.dumps(doc).encode()if args[-2]=='core-intake'else proc)
                with self.subTest(configured=len(configured),statuses=statuses):
                    if valid:
                        value=read_core_connection(item,execute=execute,now=lambda:now)
                        self.assertTrue(value['ready']);self.assertIsNone(value['mqtt_connected']);self.assertEqual(value['mqtt_source_count'],0)
                        self.assertNotIn('postgres_connected',value)
                    else:
                        with self.assertRaises(ValueError):read_core_connection(item,execute=execute,now=lambda:now)


class OwnedProbeProcesses(unittest.TestCase):
    def test_postspawn_failure_settles_descendants_without_harming_unrelated_process(self):
        with tempfile.TemporaryDirectory()as directory:
            marker=Path(directory)/'pid';foreign=subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)'],start_new_session=True)
            seen=[]
            code='import subprocess,sys,time;from pathlib import Path;c=subprocess.Popen([sys.executable,"-c","import time;time.sleep(20)"]);Path(sys.argv[1]).write_text(str(c.pid));time.sleep(20)'
            def fail(child):
                seen.append(child);end=time.monotonic()+1
                while not marker.exists()and time.monotonic()<end:time.sleep(.01)
                self.assertTrue(marker.exists());raise OSError('injected evidence write failure')
            try:
                with self.assertRaisesRegex(OSError,'evidence'):r._run([sys.executable,'-c',code,str(marker)],time.monotonic()+5,after_spawn=fail)
                self.assertIsNotNone(seen[0].returncode);self.assertEqual(r._members(seen[0].pid),[]);self.assertIsNone(foreign.poll())
            finally:foreign.kill();foreign.wait()
    def test_first_cancel_during_successful_probe_cleanup_still_reaps_and_fails(self):
        original=r._settle
        for sig in(signal.SIGTERM,signal.SIGINT):
            saved=[];children=[];clock=Clock()
            def settle(child,ticks):
                children.append(child);os.kill(os.getpid(),sig);original(child,ticks)
            def probe(*args):
                r._run([sys.executable,'-c','pass'],time.monotonic()+5)
                return {'ready':True}
            with self.subTest(signal=sig),patch.object(r,'_settle',side_effect=settle),self.assertRaises(InterruptedError):
                r.wait_running_consumers(lambda *a,**k:snapshot(),snapshot(),clock=clock,sleep=clock.sleep,probe_fn=probe,save=lambda value:saved.append(deepcopy(value)))
            self.assertIsNotNone(children[0].returncode);self.assertEqual(r._members(children[0].pid),[])
            self.assertFalse(saved[-1]['passed']);self.assertEqual(saved[-1]['error'],'InterruptedError')
    def test_term_in_spawn_identity_window_is_delivered_after_proof_and_reaps(self):
        children=[]
        def spawn(*args,**kwargs):
            child=subprocess.Popen(*args,**kwargs);children.append(child);os.kill(os.getpid(),signal.SIGTERM);return child
        def interrupted(*args):raise InterruptedError('owned cancellation')
        old=signal.signal(signal.SIGTERM,interrupted)
        try:
            with self.assertRaises(InterruptedError):r._run([sys.executable,'-c','import time;time.sleep(20)'],time.monotonic()+5,spawn=spawn)
            self.assertIsNotNone(children[0].returncode);self.assertEqual(r._members(children[0].pid),[])
        finally:signal.signal(signal.SIGTERM,old)


if __name__=='__main__':unittest.main(verbosity=2)
