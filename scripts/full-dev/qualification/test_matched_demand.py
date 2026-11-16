"""Synthetic full-count schedule/evidence tests; no HTTP, stores or runtime calls."""
import copy
import datetime as dt
import json
from pathlib import Path
import tempfile
import subprocess
import time
import unittest
import uuid
from unittest.mock import patch, Mock

import common
import companion_reads
import matched_demand as demand
from read_protocol import PROFILE, IDENTITY, STEPS
from test_role_evidence import database_identity, role_context


def cohort(root, source, context, scale, step, batch, *, ms=10, position=0):
    directory = root/f'demand-{uuid.uuid4().hex}'; directory.mkdir()
    policy = PROFILE['demand']; base = dt.datetime(2026, 11, 14, tzinfo=dt.timezone.utc)+dt.timedelta(seconds=1000+position*180)
    at = lambda n: (base+dt.timedelta(milliseconds=n)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    capture = lambda n: {'at': at(n), 'monotonicBeforeMs': n, 'monotonicAfterMs': n}
    events = [{'phase': 'demand', 'edge': edge, 'sequence': index, 'span': 0, 'parent': None,
               'at': at(n), 'offsetMs': n, 'clock': capture(n), **({'outcome': 'passed'} if edge=='finish' else {})}
              for index,(edge,n) in enumerate((('start',10),('finish',120010)))]
    clocks = {'version': 2, 'passed': True, 'originClock': capture(0), 'originMonotonicMs': 0, 'startedAt': at(0),
              'finishedClock': capture(120020), 'finishedAt': at(120020), 'durationMs': 120020, 'events': events}
    samples=[]
    for index in range(policy['requests']):
        scheduled = 10+index*1000/policy['rps']
        samples.append({'index': index, 'name': policy['case_order'][index%12], 'actor': f'127.0.1.{index%128+1}',
            'scheduledMs': scheduled, 'attemptMs': scheduled+.5, 'startMs': scheduled+1, 'socketMs': scheduled+2, 'connectedMs': scheduled+3,
            'endMs': scheduled+4, 'completedMs': scheduled+5, 'latenessMs': 2, 'active': 1, 'queue': 0,
            'ms': 3, 'status': 404 if policy['case_order'][index%12]=='packet-missing' else 200, 'passed': True, 'bytes': 100,
            'entries': None if policy['case_order'][index%12] in ('packet-detail','packet-missing') else 1, 'digest': 'a'*64, 'dataDigest': 'b'*64})
    native = {'passed': True, 'sourceIdentity': source, 'sourceIdentityAfter': source, 'sourceStable': True,
        'databaseIdentity': database_identity(context),
        'settings': copy.deepcopy(policy), 'window': {'startMs': 10, 'endMs': 120010}, 'timeline': 'timeline.json', 'samples': samples,
        'admission': {'planned': 9600, 'attempted': 9600, 'admitted': 9600, 'completed': 9600, 'errors': 0, 'undelivered': 0,
                      'maxActive': 1, 'maxQueue': 0, 'maxLatenessMs': 2, 'dispatchSpanMs': 119987.5}}
    native['warmup']=[{'name':name,'repeat':repeat,'passed':True,'status':404 if name=='packet-missing' else 200,'bytes':100,
                      'entries':None if name in ('packet-detail','packet-missing') else 1,'ms':3,
                      'digest':'a'*64,'dataDigest':'b'*64} for name in policy['case_order'] for repeat in range(3)]
    native['responseValidationLimits']={'workerCount':1,'maxPending':8,'maxBytes':8388608,'timeoutMs':30000,'memoMaxEntries':8,
        'resourceLimits':{'maxOldGenerationSizeMb':128,'maxYoungGenerationSizeMb':16,'stackSizeMb':4}}
    native['resources']={'responseValidation':{'calls':9636,'freshHashes':9636,'hits':0,'misses':9636,'evictions':9628,
        'fullValidations':9636,'errors':0,'entries':8,'maxEntries':8}}
    common.write_json(directory/'matched-demand.json', native); common.write_json(directory/'timeline.json', clocks)
    window = {**step, 'batch_id': batch, 'id': uuid.uuid4().hex}
    row = {'scale': scale, 'label': PROFILE['source_labels'][step['side']], 'fixture_token': scale,
        'measured_source': source, 'context': context, 'context_before': context, 'protocol': IDENTITY,
        'read_window': window, 'diagnostic_only': False, 'passed': True, 'exit_code': 0,
        'native_evidence': {'path': str(directory/'matched-demand.json'), 'sha256': common.digest(directory/'matched-demand.json'),
                            'timeline_sha256': common.digest(directory/'timeline.json')}}
    boundaries = [{'id': index, 'scope': scope, 'offset_seconds': offset, 'at': (base+dt.timedelta(seconds=offset)).isoformat()}
                  for index,(scope,offset) in enumerate((('setup',0),('demand',.001),('teardown',120.1)))]
    records=[]
    for actor,name in companion_reads.COMPANION_ACTORS.items():
        for sequence in range(480):
            start=.0105+sequence*.25;end=start+ms/1000
            records.append({'actor': actor, 'name': name, 'sequence': sequence, 'passed': True, 'ms': ms,
                'start_offset_seconds': start, 'end_offset_seconds': end,
                'started_at': (base+dt.timedelta(seconds=start)).isoformat(), 'finished_at': (base+dt.timedelta(seconds=end)).isoformat(),
                'scope_at_start': 1, 'scope_at_finish': 1})
    collector = {'collector_id': uuid.uuid4().hex, 'actors': companion_reads.COMPANION_ACTORS, 'cadence_seconds': .25,
        'origin_monotonic': 2000+position*180, 'started_at': base.isoformat(), 'stop_requested_at': (base+dt.timedelta(seconds=120.1)).isoformat(),
        'stop_requested_offset_seconds': 120.1, 'finished_at': (base+dt.timedelta(seconds=120.2)).isoformat(), 'finished_offset_seconds': 120.2,
        'attempted': {actor: 480 for actor in companion_reads.COMPANION_ACTORS}, 'boundaries': boundaries, 'records': records}
    raw = {'version': 1, **{key: row[key] for key in ('scale','label','fixture_token','measured_source','context','protocol','read_window','diagnostic_only')},
           'collector': collector, 'summary': companion_reads.companion_summary(records)}
    common.write_json(directory/'companions.json', raw)
    row.update(companions=raw['summary'], companion_evidence={'path': str(directory/'companions.json'),
        'sha256': common.digest(directory/'companions.json'), 'collector_id': collector['collector_id']})
    return row


def cohorts(root, native_runs, *, current_ms=10):
    rows=[]
    for scale in dict.fromkeys(row['scale'] for row in native_runs):
        native=[row for row in native_runs if row['scale']==scale]
        for step in STEPS:
            reference=next(row for row in native if row['label']==PROFILE['source_labels'][step['side']])
            rows.append(cohort(root,reference['measured_source'],reference['context'],scale,step,native[0]['read_window']['batch_id'],
                               ms=current_ms if step['side']=='B' else 10,position=len(rows)))
    return rows


class MatchedDemand(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);self.root=Path(temporary.name)
        self.addCleanup(patch.stopall);patch.object(common,'EVIDENCE',self.root).start();patch.object(companion_reads,'EVIDENCE',self.root).start()
        self.row=cohort(self.root,{'sha256':'source'},{'run_id':'run',**role_context('10x')},'10x',STEPS[0],uuid.uuid4().hex)

    def test_complete9600_schedule_and_uncertain_boundary_observations_are_retained(self):
        value,_,_=demand.native_demand(self.row);self.assertEqual(len(value['samples']),9600)
        raw,selected,audit=demand.cohort_evidence(self.row)
        self.assertEqual(len(selected),1920);self.assertEqual(len(audit['membership']),len(raw['collector']['records']))
        self.assertEqual(sum(row['relation']=='boundary-uncertain' for row in audit['membership']),4)
        missing=next(row for row in value['samples'] if row['name']=='packet-missing')
        detail=next(row for row in value['samples'] if row['name']=='packet-detail')
        self.assertEqual(missing['status'],404);self.assertIsNone(missing['entries']);self.assertIsNone(detail['entries'])

    def test_rehashed_full_demand_ledger_cannot_hide_missing_or_unsafe_role_proof(self):
        path = Path(self.row['native_evidence']['path']); original = path.read_bytes()
        demand.native_demand(self.row)
        for fault in ('missing', 'legacy', 'administrator', 'writable', 'wrong-store'):
            value = json.loads(original)
            if fault == 'missing': value.pop('databaseIdentity')
            elif fault == 'legacy': value['databaseIdentity']['scope'] = 'legacy-inspector-readonly'
            elif fault == 'administrator': value['databaseIdentity']['setup']['http']['session_user'] = 'meshcore'
            elif fault == 'writable': value['databaseIdentity']['after']['http']['transaction_read_only'] = 'off'
            else: value['databaseIdentity']['after']['http']['database'] = 'meshcore'
            try:
                common.write_json(path, value); self.row['native_evidence']['sha256'] = common.digest(path)
                with self.subTest(fault=fault), self.assertRaisesRegex(ValueError, 'application-role'):
                    demand.native_demand(self.row)
            finally:
                path.write_bytes(original); self.row['native_evidence']['sha256'] = common.digest(path)
        demand.native_demand(self.row)

    def test_missing_late_socket_catchup_queue_wrong_case_and_wrong_source_are_invalid(self):
        path=Path(self.row['native_evidence']['path']);original=path.read_bytes()
        for fault in ('missing','late-socket','catchup','queue','case','source','digest','span','warmup','limits','counter'):
            value=common.read_json(path)
            if fault=='missing':value['samples'].pop()
            elif fault=='late-socket':value['samples'][100]['socketMs']=value['samples'][100]['scheduledMs']+101
            elif fault=='catchup':value['samples'][100]['socketMs']=value['samples'][99]['socketMs']+9
            elif fault=='queue':value['samples'][100]['queue']=9
            elif fault=='case':value['samples'][100]['name']='other'
            elif fault=='source':value['sourceIdentityAfter']={'sha256':'stale'}
            elif fault=='digest':value['samples'][9599]['dataDigest']='invalid'
            elif fault=='span':value['admission']['dispatchSpanMs']=120000
            elif fault=='warmup':value['warmup'].pop()
            elif fault=='limits':value['responseValidationLimits']['memoMaxEntries']=12
            else:value['resources']['responseValidation']['freshHashes']-=1
            common.write_json(path,value);self.row['native_evidence']['sha256']=common.digest(path)
            with self.subTest(fault=fault),self.assertRaises(ValueError):demand.native_demand(self.row)
            path.write_bytes(original);self.row['native_evidence']['sha256']=common.digest(path)

    def test_outside_window_failures_still_fail_and_short_observer_coverage_is_invalid(self):
        path=Path(self.row['companion_evidence']['path']);original=path.read_bytes();raw=common.read_json(path)
        row=raw['collector']['records'][0]
        row.update(passed=False,ms=1,start_offset_seconds=0,end_offset_seconds=.001,
                   started_at=raw['collector']['started_at'],finished_at=raw['collector']['boundaries'][1]['at'],scope_at_start=0,scope_at_finish=1)
        raw['summary']=companion_reads.companion_summary(raw['collector']['records'])
        common.write_json(path,raw);self.row['companion_evidence']['sha256']=common.digest(path);self.row['companions']=raw['summary']
        with self.assertRaisesRegex(ValueError,'errors'):demand.cohort_evidence(self.row)
        path.write_bytes(original);raw=common.read_json(path)
        raw['collector']['stop_requested_offset_seconds']=120
        raw['collector']['stop_requested_at']=(dt.datetime.fromisoformat(raw['collector']['started_at'])+dt.timedelta(seconds=120)).isoformat()
        common.write_json(path,raw);self.row['companion_evidence']['sha256']=common.digest(path);self.row['companions']=raw['summary']
        with self.assertRaisesRegex(ValueError,'complete scheduled'):demand.cohort_evidence(self.row)

    def test_a_response_finishing_after_window_end_remains_in_the_primary_pool(self):
        path=Path(self.row['companion_evidence']['path']);raw=common.read_json(path)
        row=next(row for row in raw['collector']['records'] if row['actor']=='corescope-1' and row['sequence']==479)
        base=dt.datetime.fromisoformat(raw['collector']['started_at'])
        row.update(ms=100,start_offset_seconds=119.99,end_offset_seconds=120.09,
                   started_at=(base+dt.timedelta(seconds=119.99)).isoformat(),finished_at=(base+dt.timedelta(seconds=120.09)).isoformat())
        raw['summary']=companion_reads.companion_summary(raw['collector']['records'])
        common.write_json(path,raw);self.row['companions']=raw['summary'];self.row['companion_evidence']['sha256']=common.digest(path)
        _,selected,audit=demand.cohort_evidence(self.row)
        self.assertIn(row,selected)
        crossing=next(row for row in audit['membership'] if row['actor']=='corescope-1' and row['sequence']==479)
        self.assertTrue(crossing['included']);self.assertTrue(crossing['crosses_finish'])

    def test_full_three_pair_pool_uses_every_observation_and_raw_ten_percent_gate(self):
        native=[];batch=uuid.uuid4().hex
        for step in STEPS:
            native.append({'scale':'10x','label':PROFILE['source_labels'][step['side']], 'measured_source':{'sha256':step['side']},
                'context':{'run_id':'run', **role_context('10x')},'read_window':{'batch_id':batch}})
        rows=cohorts(self.root,native,current_ms=10)
        value=demand.demand_comparison(rows,scales=('10x',))
        self.assertTrue(value['passed'],value['missing'])
        self.assertEqual(value['companion_comparisons'][0]['baseline']['requests'],2880)
        target=rows[-1];path=Path(target['companion_evidence']['path']);raw=common.read_json(path)
        for row in raw['collector']['records']:
            row['ms']=11.1;row['end_offset_seconds']=row['start_offset_seconds']+.0111
            row['finished_at']=(dt.datetime.fromisoformat(row['started_at'])+dt.timedelta(milliseconds=11.1)).isoformat()
        raw['summary']=companion_reads.companion_summary(raw['collector']['records'])
        common.write_json(path,raw);target['companions']=raw['summary'];target['companion_evidence']['sha256']=common.digest(path)
        value=demand.demand_comparison(rows,scales=('10x',))
        self.assertTrue(value['valid'],value['missing']);self.assertFalse(value['passed'])
        self.assertTrue(all(row['candidate_p95_ms']==11.1 for row in value['companion_comparisons']))
        changed=copy.deepcopy(rows);changed[-1]['read_window']['id']=changed[-2]['read_window']['id']
        self.assertFalse(demand.demand_comparison(changed,scales=('10x',))['valid'])

    def test_actual_socket_lateness_boundary_is_inclusive_without_a_timer_start_substitute(self):
        path=Path(self.row['native_evidence']['path']);timeline=path.parent/'timeline.json'
        clocks=common.read_json(timeline)
        clocks['finishedClock']={'at':'2026-11-14T00:18:40.200Z','monotonicBeforeMs':120200,'monotonicAfterMs':120200}
        clocks['finishedAt']=clocks['finishedClock']['at'];clocks['durationMs']=120200
        common.write_json(timeline,clocks);self.row['native_evidence']['timeline_sha256']=common.digest(timeline)
        original=common.read_json(path)
        for lateness,allowed in ((100,True),(101,False)):
            value=copy.deepcopy(original);row=value['samples'][-1];scheduled=row['scheduledMs']
            row.update(socketMs=scheduled+lateness,connectedMs=scheduled+lateness+1,endMs=scheduled+lateness+2,
                       completedMs=scheduled+lateness+3,latenessMs=lateness,ms=lateness+1)
            value['admission']['maxLatenessMs']=lateness
            value['admission']['dispatchSpanMs']=row['socketMs']-value['samples'][0]['socketMs']
            common.write_json(path,value);self.row['native_evidence']['sha256']=common.digest(path)
            if allowed:demand.native_demand(self.row)
            else:
                with self.assertRaises(ValueError):demand.native_demand(self.row)

    def test_actual_node_scheduler_full9600_output_matches_python_audit_without_network(self):
        script=self.root/'scheduler-fixture.mjs';output=self.root/'scheduler.json'
        module=(common.REPO/'backend/src/tools/lookupDemand.ts').as_uri()
        policy=(common.REPO/'scripts/full-dev/qualification/read_protocol.json').as_posix()
        script.write_text("import {readDemandPolicy,runMatchedDemand} from "+repr(module)+";\n"
            "import {writeFile} from 'node:fs/promises';\n"
            "const p=await readDemandPolicy("+repr(policy)+"); let tick=0;\n"
            "const cases=p.case_order.map(name=>({name,route:'/unused',expected:name==='packet-missing'?404:200}));\n"
            "const result=await runMatchedDemand(p,cases,0,10,(definition,index)=>{\n"
            " const timing={startedAtMonoMs:tick,socketAtMonoMs:tick,connectedAtMonoMs:tick,actor:`127.0.1.${index+1}`};\n"
            " return {timing,assigned:Promise.resolve(tick),done:Promise.resolve({...timing,receivedAtMonoMs:tick,ms:0,bytes:100,\n"
            " status:definition.name==='packet-missing'?404:200,entries:null,digest:'a'.repeat(64),dataDigest:'b'.repeat(64)})};\n"
            "},{now:()=>tick,pause:async ms=>{tick+=ms;}});\n"
            "await writeFile("+repr(str(output))+",JSON.stringify(result));\n")
        result=subprocess.run(['node','--max-old-space-size=256','--import',str(common.REPO/'backend/node_modules/tsx/dist/loader.mjs'),str(script)],
                              cwd=common.REPO,env=common.clean_environment(),capture_output=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr.decode()[-2000:])
        native_path=Path(self.row['native_evidence']['path']);native=common.read_json(native_path)
        native.update(common.read_json(output));self.assertTrue(native['passed'])
        common.write_json(native_path,native);self.row['native_evidence']['sha256']=common.digest(native_path)
        value,_,_=demand.native_demand(self.row);self.assertEqual(len(value['samples']),9600)

    def test_teardown_annotation_failure_still_finishes_readers_and_preserves_failed_receipt(self):
        collector=Mock(origin=1,records=[],collector_id='a'*32)
        collector.evidence.return_value={'partial':True}
        def scope(name):
            if name=='teardown':raise RuntimeError('injected teardown annotation failure')
        collector.set_scope.side_effect=scope
        directory=self.root/'partial';context={'run_id':'synthetic'}
        fixture={'kind':'10x','token':'owned','databases':[{'label':'ukmesh','database':'owned'}]}
        with patch.object(demand,'CompanionReads',return_value=collector),patch.object(demand,'phase_context',return_value=context):
            with self.assertRaisesRegex(RuntimeError,'teardown annotation'):
                demand.collect_cohort(self.root,fixture,{'source_root':'/candidate','revision':'revision'},'candidate',directory,
                    {'id':'owned'},context,time.monotonic()+10,Mock(return_value=0))
        collector.finish.assert_called_once()
        self.assertFalse(common.read_json(directory/'summary.json')['passed'])
        self.assertEqual(common.read_json(directory/'companions.json')['collector'],{'partial':True})

    def test_rehashed_green_timeline_requires_actual_successful_ordered_demand_finish(self):
        path=Path(self.row['native_evidence']['path']).parent/'timeline.json';original=path.read_bytes()
        for fault in ('missing-finish','failed-finish','wrong-span','sequence'):
            value=common.read_json(path)
            if fault=='missing-finish':value['events'].pop()
            elif fault=='failed-finish':value['events'][-1]['outcome']='failed'
            elif fault=='wrong-span':value['events'][-1]['span']=9
            else:value['events'][-1]['sequence']=0
            common.write_json(path,value);self.row['native_evidence']['timeline_sha256']=common.digest(path)
            with self.subTest(fault=fault),self.assertRaisesRegex(ValueError,'completion'):demand.native_demand(self.row)
            path.write_bytes(original)


if __name__=='__main__':unittest.main()
