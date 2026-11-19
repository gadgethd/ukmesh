"""Owned mixed/torn selections and controller recovery; no app/DB calls."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import target_safety as safety
from target_lease import target_lease,ENV
from transition_protocol import new_intent
from test_target_protocol import records

OLD={'UKMESH_DATABASE':'old','OWNER_DATABASE':'owner-old','BEACON_DATABASE':'beacon-old','CORESCOPE_DATA_DIR':'/owned/old'}
NEW={'UKMESH_DATABASE':'new','OWNER_DATABASE':'owner-new','BEACON_DATABASE':'beacon-new','CORESCOPE_DATA_DIR':'/owned/new'}


class TargetSafety(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.path=Path(self.temporary.name)/'intent.json';self.lock=Path(self.temporary.name)/'lease'
        self.actual={'consumers':records(),'broker':{'running':True}}
        self.files={'values':deepcopy(OLD),'selection':{'targets':'original'}}
        self.marker={'targets':{'ukmesh':'new','owner':'owner-new','beacon':'beacon-new'},'token':'owned'}

    def select(self,commit,**overrides):
        options=dict(snapshot=lambda:self.actual,observe_fn=lambda reader:{'passed':True},path=self.path,
                     lock_path=self.lock,selection_reader=lambda:deepcopy(self.files),requested_selection=self.marker)
        previous=overrides.pop('previous',lambda:deepcopy(OLD))
        options.update(overrides)
        return safety.select(NEW,previous,commit,**options)

    def test_torn_files_retry_only_exact_owned_request_and_durable_intent_first(self):
        def fail_between_files():
            intent=safety.read_intent(self.path)
            self.assertEqual(intent['status'],'pending');self.assertFalse(intent['selection_complete'])
            self.files['values']=deepcopy(NEW)
            raise RuntimeError('selected marker not yet published')
        with self.assertRaises(RuntimeError):self.select(fail_between_files)
        intent=safety.read_intent(self.path);self.assertFalse(intent['selection_complete'])
        def finish():self.files['selection']=deepcopy(self.marker)
        # The retry never needs already-coherent current_targets().
        with patch.object(safety,'assert_no_publishers'):
            result=self.select(finish,previous=lambda:self.fail('torn current_targets must not be required'))
        self.assertTrue(result['passed']);self.assertTrue(safety.read_intent(self.path)['selection_complete'])
        with self.assertRaisesRegex(ValueError,'exact pending'):
            self.select(finish,requested_selection={**self.marker,'token':'foreign'})
        self.files['values']={**NEW,'BEACON_DATABASE':'foreign'}
        with self.assertRaisesRegex(ValueError,'foreign target files'):self.select(finish)

    def test_partial_known_start_reobserves_both_sides_and_unknown_ref_refuses(self):
        with patch.object(safety,'assert_no_publishers'):self.select(lambda:None)
        self.actual['consumers']['ukmesh']['refs']={'ukmesh':'new','owner':'owner-new'}
        called=[]
        result=safety.check_start(NEW,snapshot=lambda:self.actual,observe_fn=lambda read:called.append(read()) or {'passed':True},path=self.path)
        self.assertTrue(result['assessment']['mixed']);self.assertEqual(len(called),1)
        self.actual['consumers']['core']['refs']={'corescope':'/foreign'}
        with self.assertRaisesRegex(ValueError,'foreign'):safety.check_start(NEW,snapshot=lambda:self.actual,path=self.path)

    def test_same_store_missing_observability_bootstraps_but_changing_stopped_refuses(self):
        # No read of unsupported old stats is necessary for this same-ref resume.
        self.actual['consumers']['core']['running']=False
        result=safety.check_start(OLD,snapshot=lambda:self.actual,observe_fn=lambda _:self.fail('must not observe'),path=self.path)
        self.assertFalse(result['observability_required'])
        intent=new_intent('owned',records(),safety.target_values(NEW),old_targets=safety.target_values(OLD))
        intent['requested_values']=NEW;safety.write_intent(self.path,intent)
        with self.assertRaisesRegex(ValueError,'inactive'):safety.check_start(NEW,snapshot=lambda:self.actual,path=self.path)
        self.actual['consumers']['core']['refs']={'corescope':'/owned/new'}
        result=safety.check_start(NEW,snapshot=lambda:self.actual,observe_fn=lambda _: {'passed':True},path=self.path)
        self.assertIn('core',result['assessment']['same_store_inactive_resume'])

    def test_stopped_requested_backend_can_resume_without_global_empty_claim(self):
        # Full selection is pending; the companion still uses the old store.
        with patch.object(safety,'assert_no_publishers'):self.select(lambda:None)
        self.actual['consumers']['ukmesh'].update(refs={'ukmesh':'new','owner':'owner-new'},running=False)
        result=safety.check_start(NEW,scope={'ukmesh'},snapshot=lambda:self.actual,
            observe_fn=lambda _:self.fail('same-ref backend resume does not move companions'),path=self.path)
        self.assertEqual(result['kind'],'same-store-bootstrap');self.assertFalse(result['observability_required'])
        self.assertEqual(safety.read_intent(self.path)['status'],'pending')
        self.assertNotIn('pending',result)

    def test_pending_publication_refuses_and_fd_lifetime_covers_cleanup(self):
        safety.write_intent(self.path,{'version':1,'status':'pending'})
        with self.assertRaisesRegex(ValueError,'pending'):
            with safety.publication_lease(path=self.path,lock_path=self.lock):self.fail('admitted')
        self.path.unlink()
        with self.assertRaisesRegex(RuntimeError,'publication failed'), safety.publication_lease(path=self.path,lock_path=self.lock):
            self.assertIn(ENV,os.environ)
            try:raise RuntimeError('publication failed')
            finally:
                self.assertIn(ENV,os.environ)  # cleanup remains inside the shared lease
        self.assertNotIn(ENV,os.environ)

    def test_unchanged_selection_does_not_create_pending_adoption(self):
        result=safety.select(OLD,lambda:OLD,lambda:self.fail('unchanged files'),snapshot=lambda:self.actual,
            path=self.path,lock_path=self.lock,selection_reader=lambda:self.files,requested_selection=self.files['selection'])
        self.assertEqual(result['transition'],'unchanged');self.assertFalse(self.path.exists())

    def test_applied_requires_full_inventory_and_requested_refs_after_failed_start(self):
        with patch.object(safety,'assert_no_publishers'):self.select(lambda:None)
        intent=safety.read_intent(self.path)
        manifest={'services':[{'name':f'service-{i}','container':f'service-{i}','kind':'container','required':True} for i in range(43)]}
        containers={s['container']:{'State':{'Running':True,'OOMKilled':False,'Health':{'Status':'healthy'}}} for s in manifest['services']}
        current={'consumers':deepcopy(self.actual['consumers']),'containers':containers}
        for name,row in current['consumers'].items():row['refs']=intent['consumers'][name]['requested']
        with patch.object(safety,'service_manifest',return_value=manifest):
            missing=deepcopy(current);del missing['containers']['service-42']
            with self.assertRaisesRegex(ValueError,'inventory'):safety.mark_complete(NEW,snapshot=lambda:missing,path=self.path)
            self.assertEqual(safety.read_intent(self.path)['status'],'pending')
            failed=deepcopy(current);failed['containers']['service-1']['State']['Health']['Status']='unhealthy'
            with self.assertRaises(ValueError):safety.mark_complete(NEW,snapshot=lambda:failed,path=self.path)
            self.assertEqual(safety.read_intent(self.path)['status'],'pending')
            result=safety.mark_complete(NEW,snapshot=lambda:current,path=self.path)
            self.assertEqual(result['transition'],'applied');self.assertEqual(safety.read_intent(self.path)['status'],'applied')

    def test_known_unmanaged_relative_and_compiled_publishers_refuse(self):
        proc=Path(self.temporary.name)/'proc';entry=proc/'123';entry.mkdir(parents=True)
        cwd=entry/'cwd'
        for directory,argument in ((safety.REPO/'backend','src/tools/replaySampleMqtt.ts'),
                (safety.REPO,'backend/src/tools/soakDatabaseLookups.ts'),
                (safety.REPO/'backend','dist/tools/fullStackTraffic.js')):
            cwd.unlink(missing_ok=True);cwd.symlink_to(directory)
            (entry/'cmdline').write_bytes(b'node\0'+os.fsencode(argument)+b'\0')
            with self.assertRaisesRegex(ValueError,'publisher'):safety.assert_no_publishers(proc=proc)
        (entry/'cmdline').write_bytes(b'node\0unrelated/replaySampleMqtt.ts\0')
        safety.assert_no_publishers(proc=proc)
        (entry/'cmdline').write_bytes(os.fsencode(str(safety.REPO/'scripts/full-dev/qualification/fixtures.py'))+b'\0status\0')
        safety.assert_no_publishers(proc=proc)
        (entry/'cmdline').write_bytes(os.fsencode(str(safety.REPO/'scripts/full-dev/qualification/fixtures.py'))+b'\0create\0')
        with self.assertRaisesRegex(ValueError,'provisioner'):safety.assert_no_publishers(proc=proc)


if __name__=='__main__':unittest.main()
