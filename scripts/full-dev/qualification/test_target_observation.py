"""One fixed45s window and all live owners; fake transports only."""
from copy import deepcopy
import time
import unittest
from target_observation import APPS,IntakeProof,observe,valid_broker,broker_observation,intake_owners
from unittest.mock import patch
from types import SimpleNamespace
import json


class Clock:
    def __init__(self):self.now=0
    def clock(self):return self.now
    def wall(self):return 1000+self.now
    def sleep(self,value):self.now+=value;time.sleep(0)


def snapshot():
    return {'broker':{'id':'broker'},'consumers':{
        name:{'refs':{'store':name},'container_id':name,'running':True,'image_id':'image','pid':1,'started_at':'start'}
        for name in APPS.values()}}


def apps(clock,*,probe=True):
    return {'ukmesh':{'quiescent':True,'probe_last_success':clock.wall() if probe else 999},
            'corescope':{'quiescent':True,'process':{'pid':7,'start_ticks':10},'state':{'generation':5,'droppedTotal':10},
                         'write_errors':0,'sampled_at':str(clock.now)},
            'beacon':{'quiescent':True,'process_started_at':900,'state':{'droppedTotal':12}}}


class ProtectionObservation(unittest.TestCase):
    def test_handoff_records_only_the_process_that_owns_mqtt(self):
        state=snapshot()
        legacy='ukmesh-full-backend-1'
        state['consumers'][legacy]={'running':True}
        state['containers']={legacy:{'Config':{'Env':['UKMESH_RUNTIME_ROLE=api']}}}
        with self.assertRaisesRegex(ValueError,'must not run together'):
            intake_owners(state)
        state['consumers'][APPS['ukmesh']]['running']=False
        self.assertEqual(intake_owners(state)['ukmesh'],legacy)
        state['containers'][legacy]['Config']['Labels']={'org.ukmesh.runtime-layout':'split-v1'}
        state['consumers'][APPS['ukmesh']]['running']=True
        self.assertEqual(intake_owners(state)['ukmesh'],APPS['ukmesh'])

    def run_observation(self,*,reader=None,broker=None,snap=None):
        clock=Clock();calls=[]
        def read(value,deadline):
            calls.append(clock.now)
            return (reader or (lambda c:apps(c,probe=c.now>=30)))(clock)
        result=observe(snap or (lambda deadline:snapshot()),apps=read,
            broker=broker or (lambda deadline:{'version':1,'passed':True,'cleanup':{'closed':True,'forced':False,'connected':True,'subscribed':True}}),
            clock=clock.clock,wall=clock.wall,sleep=clock.sleep)
        return result,clock,calls

    def test_real_broker_cleanup_shape_and_nonzero_stdout_are_retained(self):
        literal={'version':1,'passed':True,'cleanup':{'closed':True,'connected':True,'forced':False,'subscribed':True}}
        self.assertTrue(valid_broker(literal))
        for key in ('closed','connected','subscribed'):
            bad=deepcopy(literal);bad['cleanup'][key]=False
            self.assertFalse(valid_broker(bad))
        refused={**literal,'passed':False,'errors':['disconnected-durable-sessions']}
        raw=json.dumps(refused).encode();saved=[]
        with patch('target_observation.command',return_value=SimpleNamespace(returncode=1,stdout=raw)):
            with self.assertRaisesRegex(ValueError,'retained evidence'):
                broker_observation(45,clock=lambda:0,save=lambda value:saved.append(value) or 'private.json')
        self.assertEqual(saved[0]['stdout'],raw.decode());self.assertEqual(saved[0]['exit_code'],1)

    def test_completed_broker_does_not_skip_wait_for_normal_probe(self):
        result,clock,calls=self.run_observation()
        self.assertTrue(result['passed']);self.assertGreaterEqual(clock.now,30);self.assertLessEqual(clock.now,45)
        self.assertGreater(len(calls),2);self.assertGreaterEqual(result['applications']['ukmesh']['probe_last_success'],1000)
        self.assertEqual(result['max_seconds'],45)

    def test_missing_fresh_probe_expires_without_extending_window(self):
        with self.assertRaises(TimeoutError):self.run_observation(reader=lambda c:apps(c,probe=False))

    def test_unready_missing_or_active_app_refuses_before_broker_is_started(self):
        for kind in ('missing','active','stale'):
            called=[]
            def read(clock):
                value=apps(clock)
                if kind=='missing':del value['corescope']
                elif kind=='active':value['beacon']['quiescent']=False
                else:raise ValueError('Core observation stale')
                return value
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                self.run_observation(reader=read,broker=lambda deadline:called.append(True))
            self.assertEqual(called,[])

    def test_false_broker_and_changed_actual_consumer_or_broker_identity_refuse(self):
        with self.assertRaises(ValueError):self.run_observation(broker=lambda deadline:{'version':1,'passed':False,'cleanup':{'closed':True,'forced':False,'connected':True,'subscribed':True}})
        for key in ('consumer','broker'):
            count=0
            def snap(deadline):
                nonlocal count
                value=snapshot();count+=1
                if count>1:
                    if key=='consumer':value['consumers'][APPS['ukmesh']]['refs']={'ukmesh':'foreign'}
                    else:value['broker']['id']='replacement'
                return value
            with self.subTest(key=key),self.assertRaises(ValueError):self.run_observation(snap=snap)

    def test_new_core_generation_or_beacon_loss_is_not_hidden_by_zero_queue(self):
        for kind in ('generation','drop'):
            count=0
            def read(clock):
                nonlocal count
                count+=1;value=apps(clock)
                if count>1:
                    if kind=='generation':value['corescope']['state']['generation']+=1
                    else:value['beacon']['state']['droppedTotal']+=1
                return value
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.run_observation(reader=read)


if __name__=='__main__':unittest.main()
