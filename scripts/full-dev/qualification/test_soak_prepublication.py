"""Confirmed prepublication cleanup never becomes a successful/skipped proof."""
import copy
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
import subprocess
import threading
import unittest
from unittest.mock import patch

import common
import health
import soak


class PrepublicationCleanup(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name)
        self.addCleanup(patch.stopall);patch.object(soak,'REPO',self.root).start()
        self.tool=self.root/'backend/src/tools/soakDatabaseLookups.ts';self.tool.parent.mkdir(parents=True);self.tool.write_text('reviewed native tool')
        self.helpers={key:self.tool.parent/name for key,name in (('publication','soakPublication.ts'),('counts','soakWorksetCounts.ts'))}
        for key,path in self.helpers.items():path.write_text('reviewed '+key+' helper')
        self.source=self.root/'soak-source';self.aux=self.root/'auxiliary-source';self.output=self.root/'native-soak'
        for path in (self.source,self.aux,self.output):path.mkdir()
        common.write_json(self.source/'fixture.json',{'synthetic':True,'count':30})
        self.hashes=[f'{index:064X}' for index in range(30)]
        with gzip.open(self.source/'packets.csv.gz','wt') as stream:stream.write('packet_hash\n'+'\n'.join(self.hashes)+'\n')
        with gzip.open(self.source/'raw_packets.csv.gz','wt') as stream:stream.write('raw_hex\n')
        common.write_json(self.aux/'traffic.json',{'ukmeshHashes':['F'*64]})
        hashes=sorted(self.hashes+['F'*64]);self.invocation='a'*32
        self.fixture={'databases':[{'label':'ukmesh','database':'lean_10x_owned'}]}
        self.receipt={'version':1,'passed':False,'phase':'pre-publication','publicationStarted':False,'guardCompleted':True,
            'ackPublished':0,'auxiliaryPublished':0,'invocationToken':self.invocation,'database':'lean_10x_owned',
            'dataDirectory':str(self.source),'outputDirectory':str(self.output),'settings':{'count':30,'rate':30},
            'toolSha256':common.digest(self.tool),'helperSha256':{key:common.digest(path) for key,path in self.helpers.items()},
            'inputSha256':{key:common.digest(path) for key,path in
                (('fixture',self.source/'fixture.json'),('packets',self.source/'packets.csv.gz'),
                 ('raw',self.source/'raw_packets.csv.gz'),('auxiliary',self.aux/'traffic.json'))},
            'countsScope':{'kind':'owned-packet-derived-observation-workset-v1','hashCount':31,'ackHashCount':30,
                'auxiliaryHashCount':1,'sha256':hashlib.sha256(json.dumps(hashes,separators=(',',':')).encode()).hexdigest()},
            'error':{'code':'57014','message':'setup failure'}}
        self.path=self.output/'failure.json';common.write_json(self.path,self.receipt)

    def trusted(self,code=1):
        return soak.prepublication_failure(self.output,self.source,self.aux,self.fixture,30,self.invocation,code)

    def test_only_current_guarded_source_and_input_bound_zero_publication_receipt_is_trusted(self):
        self.assertEqual(self.trusted()['receipt_sha256'],common.digest(self.path))
        for fault in ('nonce','source','helper','database','input','raw','path','scope','phase','started','unknown','ack','aux','guard','version','passed'):
            value=copy.deepcopy(self.receipt)
            if fault=='nonce':value['invocationToken']='b'*32
            elif fault=='source':value['toolSha256']='0'*64
            elif fault=='helper':value['helperSha256']['publication']='0'*64
            elif fault=='database':value['database']='meshcore'
            elif fault=='input':value['inputSha256']['packets']='0'*64
            elif fault=='raw':value['inputSha256']['raw']='0'*64
            elif fault=='path':value['outputDirectory']=str(self.root)
            elif fault=='scope':value['countsScope']['sha256']='0'*64
            elif fault=='phase':value['phase']='publishing'
            elif fault=='started':value['publicationStarted']=True
            elif fault=='unknown':value['publicationStarted']=None
            elif fault=='ack':value['ackPublished']=True
            elif fault=='aux':value['auxiliaryPublished']=1
            elif fault=='guard':value['guardCompleted']=False
            elif fault=='version':value['version']=True
            else:value['passed']=True
            common.write_json(self.path,value)
            with self.subTest(fault=fault):self.assertIsNone(self.trusted())
        common.write_json(self.path,self.receipt)
        self.assertIsNone(self.trusted(0));self.assertIsNone(self.trusted(None))

    def test_missing_malformed_symlink_and_fifo_receipts_retain_unknown_failure_path(self):
        for content in (b'{',b'[]',b'null'):
            self.path.write_bytes(content);self.assertIsNone(self.trusted())
        self.path.unlink();self.assertIsNone(self.trusted())
        other=self.root/'other.json';common.write_json(other,self.receipt);self.path.symlink_to(other)
        self.assertIsNone(self.trusted());self.path.unlink();os.mkfifo(self.path)
        self.assertIsNone(self.trusted())

    def test_confirmed_prepub_takes_one_snapshot_each_and_cannot_pass_even_if_stale_data_matches(self):
        failure=self.trusted();proof=self.root/'health.json'
        with patch.object(soak,'verify_typed',return_value={'passed':True,'checks':[]}) as typed, \
             patch.object(soak,'verify_companion_acks',return_value={'passed':True,'checks':[]}) as ack,patch.object(health,'PROOF',proof):
            values=soak.verify_persistence(self.source,self.aux,self.fixture,{'passed':True,'checks':[]},failure)
        self.assertEqual(typed.call_args.kwargs,{'timeout':0});self.assertEqual(ack.call_args.kwargs,{'timeout':0})
        self.assertTrue(all(value['passed'] is False and value['not_published'] for value in values))
        self.assertFalse(common.read_json(proof)['passed'])

    def test_snapshot_error_does_not_skip_other_snapshot_or_health_failed_receipt(self):
        failure=self.trusted();proof=self.root/'health.json'
        with patch.object(soak,'verify_typed',side_effect=RuntimeError('postgresql://user:secret@localhost/db')) as typed, \
             patch.object(soak,'verify_companion_acks',return_value={'passed':False,'checks':[]}) as ack,patch.object(health,'PROOF',proof):
            values=soak.verify_persistence(self.source,self.aux,self.fixture,{},failure)
        typed.assert_called_once();ack.assert_called_once();self.assertTrue(proof.is_file())
        self.assertTrue(all(value['passed'] is False for value in values));self.assertNotIn('secret',json.dumps(values))
        self.assertIn('snapshot_error',values[0])

    def test_unknown_or_midpublication_failure_retains_full_existing_verifiers(self):
        with patch.object(soak,'verify_typed',return_value={'passed':True}) as typed, \
             patch.object(soak,'verify_companion_acks',return_value={'passed':True}) as ack:
            values=soak.verify_persistence(self.source,self.aux,self.fixture,{'passed':True})
        self.assertEqual(typed.call_args.kwargs,{});self.assertEqual(ack.call_args.kwargs,{})
        self.assertTrue(all(value['passed'] for value in values))

    def test_owned_health_poll_cancels_cooperatively_and_preserves_explicit_failure(self):
        stop=threading.Event();entered=threading.Event();result={};proof=self.root/'health.json'
        common.write_json(self.root/'runtime/selected-fixture.json',{'token':'owned'})
        session={'sessionId':'session','fixtureToken':'owned','runId':'run','testStartedAt':100}
        def request(_):
            entered.set();return json.dumps({'id':'session','matchedAt':0,'observedCount':0,'receipts':[],'useCount':0}).encode(),'application/json'
        with patch.object(health,'http_get',side_effect=request),patch.object(health,'STATE',self.root),patch.object(health,'PROOF',proof):
            worker=threading.Thread(target=lambda:result.update(health.verify_session(session,stop_event=stop)))
            worker.start();self.assertTrue(entered.wait(1));stop.set();worker.join(1)
            self.assertFalse(worker.is_alive())
        self.assertFalse(result['passed']);self.assertTrue(result['cancelled'])
        self.assertTrue(any(row.get('not_published') and row['passed'] is False for row in result['checks']))

    def test_actual_node_latch_receipt_is_trusted_only_before_either_publisher_can_start(self):
        script=self.root/'publication-fixture.mjs';resultfile=self.root/'node-receipts.json'
        module=(common.REPO/'backend/src/tools/soakPublication.ts').as_uri()
        context=copy.deepcopy(self.receipt)
        context['toolSha256']=common.digest(common.REPO/'backend/src/tools/soakDatabaseLookups.ts')
        context['helperSha256']={key:common.digest(common.REPO/'backend/src/tools'/name) for key,name in
            (('publication','soakPublication.ts'),('counts','soakWorksetCounts.ts'))}
        script.write_text("import {SoakPublication} from "+repr(module)+";\nimport {writeFile} from 'node:fs/promises';\n"
            "const context="+json.dumps(context,separators=(',',':'))+"; const owner=new SoakPublication();\n"
            "const before=owner.failure(context,new Error('synthetic setup failure')); owner.begin();\n"
            "const after=owner.failure(context,new Error('synthetic synchronous publisher failure'));\n"
            "await writeFile("+repr(str(resultfile))+",JSON.stringify({before,after}));\n")
        process=subprocess.run(['node','--max-old-space-size=128','--import',str(common.REPO/'backend/node_modules/tsx/dist/loader.mjs'),str(script)],
                               cwd=common.REPO,env=common.clean_environment(),capture_output=True,timeout=15)
        self.assertEqual(process.returncode,0,process.stderr.decode()[-1000:]);values=common.read_json(resultfile)
        with patch.object(soak,'REPO',common.REPO):
            common.write_json(self.path,values['before']);self.assertIsNotNone(self.trusted())
            common.write_json(self.path,values['after']);self.assertIsNone(self.trusted())


if __name__=='__main__':unittest.main()
