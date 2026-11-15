"""Per-consumer intent and partial retries; no Docker, databases or target writes."""
from copy import deepcopy
from pathlib import Path
import unittest

from consumer_refs import describe_consumer
from transition_protocol import new_intent,assess_intent,complete_intent


def records():
    return {name:{'name':name,'refs':refs,'running':True,'container_id':name+'-old'} for name,refs in {
        'ukmesh':{'ukmesh':'old','owner':'owner-old'},'core':{'corescope':'/owned/old'},'beacon':{'beacon':'beacon-old'}}.items()}


class TransitionProtocol(unittest.TestCase):
    def setUp(self):
        self.old=records();self.targets={'ukmesh':'new','owner':'owner-new','beacon':'beacon-new','corescope':'/owned/new'}
        self.old_targets={'ukmesh':'old','owner':'owner-old','beacon':'beacon-old','corescope':'/owned/old'}
        self.intent=new_intent('owned',self.old,self.targets,old_targets=self.old_targets)

    def requested(self):
        result=deepcopy(self.old)
        for name,row in result.items():row['refs']=deepcopy(self.intent['consumers'][name]['requested']);row['container_id']=name+'-new'
        return result

    def test_failed_partial_start_keeps_known_old_and_requested_refs_distinct(self):
        mixed=deepcopy(self.old);mixed['ukmesh']=self.requested()['ukmesh']
        before=deepcopy(self.intent)
        result=assess_intent(self.intent,mixed,self.targets)
        self.assertTrue(result['mixed']);self.assertTrue(result['may_observe'])
        self.assertEqual(result['categories'],{'ukmesh':'requested','core':'old','beacon':'old'})
        self.assertFalse(result['complete']);self.assertEqual(self.intent,before)
        self.assertEqual(assess_intent(self.intent,self.requested(),self.targets)['categories'],dict.fromkeys(self.old,'requested'))

    def test_foreign_missing_and_changed_intent_refs_refuse(self):
        for name in self.old:
            bad=deepcopy(self.old);bad[name]['refs']={'foreign':'store'}
            with self.assertRaisesRegex(ValueError,'foreign'):assess_intent(self.intent,bad,self.targets)
        bad=deepcopy(self.old);del bad['core']
        with self.assertRaisesRegex(ValueError,'previous'):assess_intent(self.intent,bad,self.targets)
        with self.assertRaisesRegex(ValueError,'previous'):new_intent('foreign',self.requested(),self.targets,old_targets=self.old_targets)
        with self.assertRaisesRegex(ValueError,'exact'):assess_intent(self.intent,self.old,{**self.targets,'beacon':'third'})
        bad=deepcopy(self.old);bad['unowned']={'refs':{'ukmesh':'old'}}
        with self.assertRaisesRegex(ValueError,'unowned'):assess_intent(self.intent,bad,self.targets)

    def test_stopped_requested_resume_is_explicit_and_changing_stopped_refuses(self):
        mixed=self.requested();mixed['core']['running']=False
        result=assess_intent(self.intent,mixed,self.targets)
        self.assertEqual(result['same_store_inactive_resume'],['core']);self.assertTrue(result['may_observe'])
        self.assertNotIn('pending',result);self.assertFalse(result['complete'])
        mixed['core']['refs']=self.old['core']['refs']
        result=assess_intent(self.intent,mixed,self.targets)
        self.assertEqual(result['blocked_changing_inactive'],['core']);self.assertFalse(result['may_observe'])

    def test_full_authoritative_inventory_required_before_applied(self):
        required={f'service-{i}' for i in range(43)}
        inventory={'required':required,'present':required,'passed':True}
        result=complete_intent(self.intent,self.requested(),inventory)
        self.assertEqual(result['status'],'applied');self.assertEqual(self.intent['status'],'pending')
        for bad in ({**inventory,'passed':False},{**inventory,'present':required-{'service-42'}}):
            with self.assertRaises(ValueError):complete_intent(self.intent,self.requested(),bad)
        with self.assertRaises(ValueError):complete_intent(self.intent,self.old,inventory)
        stopped=self.requested();stopped['core']['running']=False
        with self.assertRaises(ValueError):complete_intent(self.intent,stopped,inventory)

    def test_finite_migration_needs_its_requested_store_and_successful_exit(self):
        old=deepcopy(self.old);old['migration']={'refs':{'ukmesh':'old'},'container_id':'job','running':False,'finite':True,'status':'exited','exit_code':0}
        intent=new_intent('owned',old,self.targets,old_targets=self.old_targets)
        self.assertTrue(assess_intent(intent,old,self.targets)['may_observe'])
        current=self.requested();current['migration']={**old['migration'],'refs':{'ukmesh':'new'}}
        inventory={'required':set(current),'present':set(current),'passed':True}
        self.assertEqual(complete_intent(intent,current,inventory)['status'],'applied')
        current['migration']['exit_code']=1
        with self.assertRaises(ValueError):complete_intent(intent,current,inventory)


class ConsumerRefs(unittest.TestCase):
    def test_roles_and_hosts_are_validated_without_emitting_credentials(self):
        env={'DATABASE_URL':'postgresql://meshcore_api:withheld@ukmesh-sample-db:5432/main',
             'MESHCORE_HTTP_DATABASE_URL':'postgresql://meshcore_api:withheld@ukmesh-sample-db:5432/main',
             'MESHCORE_INGEST_DATABASE_URL':'postgresql://meshcore_ingest:withheld@ukmesh-sample-db:5432/main',
             'MESHCORE_WORKER_DATABASE_URL':'postgresql://meshcore_worker:withheld@ukmesh-sample-db:5432/main',
             'OWNER_DATABASE_URL':'postgresql://meshcore_owner:withheld@ukmesh-sample-db:5432/auth'}
        item={'Id':'id','Image':'image','State':{'Running':True,'Pid':123,'StartedAt':'now'},'Config':{'Env':[k+'='+v for k,v in env.items()],
              'Labels':{'com.docker.compose.project':'ukmesh-full'}}}
        result=describe_consumer('ukmesh-full-backend-1',item,identity=True)
        self.assertEqual(result['refs'],{'ukmesh':'main','owner':'auth'});self.assertNotIn('withheld',str(result))
        for invalid in ('postgresql://meshcore:withheld@ukmesh-sample-db:5432/main','postgresql://meshcore_api:withheld@foreign:5432/main',
                        'postgresql://meshcore_api:withheld@ukmesh-sample-db:5432/main?options=unsafe'):
            bad=deepcopy(item);bad['Config']['Env'][0]='DATABASE_URL='+invalid
            with self.assertRaises(ValueError):describe_consumer('ukmesh-full-backend-1',bad,identity=True)
        bad=deepcopy(item);bad['Config']['Env'][1]=bad['Config']['Env'][1].replace('/main','/third')
        with self.assertRaisesRegex(ValueError,'conflicting'):describe_consumer('ukmesh-full-backend-1',bad)


if __name__=='__main__':unittest.main()
