"""Owner DTO validation and bounded transport fixtures, without application calls."""
from copy import deepcopy
import datetime as dt
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

SPEC=importlib.util.spec_from_file_location('companion_ingest_state',Path(__file__).resolve().parents[2]/'companion-dev/ingest_state.py')
app=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(app)
NOW=1000
iso=lambda value:dt.datetime.fromtimestamp(value,dt.timezone.utc).isoformat()


def identity():return {'container_id':'owned','image_id':'image','container_started_at':iso(900)}
def document():return {'sampledAt':iso(999),'write_errors':0,'ingest_state':{
    'version':1,'generation':5,'pid':7,'startTicks':100,'ready':True,'accepting':True,
    'pending':0,'active':0,'outstanding':0,'drainState':'running','droppedTotal':10}}
def process():return {'pid':7,'start_ticks':100,'argv':['/bin/companions/corescope-ingestor','-config','/config/config.json']}


class CompanionState(unittest.TestCase):
    def test_core_exact_empty_state_is_not_inferred_from_pending_only(self):
        self.assertTrue(app.core_evidence(document(),process(),identity(),NOW)['quiescent'])
        for pending,active in ((1,0),(0,1),(4,1)):
            value=document();value['ingest_state'].update(pending=pending,active=active,outstanding=pending+active)
            self.assertFalse(app.core_evidence(value,process(),identity(),NOW)['quiescent'])
        value=document();value['ingest_state'].update(accepting=False,drainState='drained')
        self.assertTrue(app.core_evidence(value,process(),identity(),NOW)['quiescent'])
        for changes in ({'ready':False},{'accepting':False},{'drainState':'incomplete'},{'drainState':'draining'}):
            value=document();value['ingest_state'].update(changes)
            self.assertFalse(app.core_evidence(value,process(),identity(),NOW)['quiescent'])

    def test_core_missing_malformed_stale_future_and_wrong_process_refuse(self):
        for key in document()['ingest_state']:
            value=document();del value['ingest_state'][key]
            with self.subTest(key=key),self.assertRaises(ValueError):app.core_evidence(value,process(),identity(),NOW)
        for changes in ({'generation':2**64},{'pending':True},{'outstanding':1},{'active':-1},{'version':2}):
            value=document();value['ingest_state'].update(changes)
            with self.assertRaises(ValueError):app.core_evidence(value,process(),identity(),NOW)
        for sampled in (994,1001,800):
            value=document();value['sampledAt']=iso(sampled)
            with self.assertRaises(ValueError):app.core_evidence(value,process(),identity(),NOW)
        for proc in ({**process(),'pid':8},{**process(),'start_ticks':101},{**process(),'argv':['foreign']}):
            with self.assertRaises(ValueError):app.core_evidence(document(),proc,identity(),NOW)
        with self.assertRaises(ValueError):app.core_evidence({'sampledAt':iso(999)},process(),identity(),NOW)

    def test_core_zero_or_overflow_generation_is_unknown_not_quiescent(self):
        for generation in (0,2**64):
            value=document()
            value['ingest_state']['generation']=generation
            with self.assertRaisesRegex(ValueError,'version or coherence'):
                app.core_evidence(value,process(),identity(),NOW)

    def test_core_unknown_fields_are_not_exported(self):
        value=document();value['ingest_state']['privatePayload']='withheld';value['private']='withheld'
        self.assertNotIn('withheld',str(app.core_evidence(value,process(),identity(),NOW)))

    def test_beacon_depth_includes_active_and_missing_metrics_never_zero(self):
        body='beacon_ingest_queue_depth 0\nbeacon_ingest_dropped_total 12\nprocess_start_time_seconds 901\n'
        self.assertTrue(app.beacon_evidence(body,identity(),NOW)['quiescent'])
        self.assertTrue(app.beacon_evidence(body.replace('901','899'),identity(),NOW)['quiescent'])
        self.assertFalse(app.beacon_evidence(body.replace('depth 0','depth 1'),identity(),NOW)['quiescent'])
        for bad in (body.replace('depth 0','depth NaN'),body.replace('depth 0','depth -1'),
                    body.replace('depth 0','depth 0.5'),body.replace('901','898'),
                    body.replace('901','1001'),body+'beacon_ingest_queue_depth 0\n',
                    body.replace('beacon_ingest_queue_depth 0\n','')):
            with self.assertRaises(ValueError):app.beacon_evidence(bad,identity(),NOW)

    def test_beacon_configuration_is_loopback_only_with_no_new_host_port(self):
        self.assertEqual(app.beacon_address(),'http://127.0.0.1:9091/metrics')
        cfg=app.yaml.safe_load((Path(__file__).resolve().parents[2]/'companion-dev/compose.yml').read_text())
        self.assertNotIn('9091',str(cfg['services']['beacon'].get('ports',[])))

    def test_beacon_transport_error_with_parseable_prefix_and_invalid_port_refuse(self):
        item={'Id':'owned','Image':'image','State':{'Running':True,'Pid':123,'StartedAt':iso(900)},
              'Config':{'Labels':{'com.docker.compose.project':'ukmesh-companions','com.docker.compose.service':'beacon'}}}
        body=b'beacon_ingest_queue_depth 0\nbeacon_ingest_dropped_total 12\nprocess_start_time_seconds 901\n'
        def failed(args,**options):
            self.assertEqual(args[2:6],['bash','-o','pipefail','-ceu'])
            return SimpleNamespace(returncode=18,stdout=body)
        with self.assertRaises(RuntimeError):app.read_beacon(item,execute=failed,now=lambda:NOW)
        for address in ('http://127.0.0.1:65536/metrics','http://127.0.0.1:90000/metrics','http://0.0.0.0:9091/metrics'):
            with self.assertRaises(ValueError):app.read_beacon(item,execute=failed,now=lambda:NOW,address=address)

    def test_transport_reads_exact_container_and_bounds_the_stats_file(self):
        item={'Id':'owned','Image':'image','State':{'Running':True,'Pid':123,'StartedAt':iso(900)},
              'Config':{'Env':[],'Labels':{'com.docker.compose.project':'ukmesh-companions','com.docker.compose.service':'corescope-ingestor'}}}
        import json
        calls=[]
        stat=b'7 (ingestor) '+b' '.join([b'S']+[b'0']*18+[b'100'])+b'\n'
        argv=b'/bin/companions/corescope-ingestor\0-config\0/config/config.json\0'
        def execute(args,**options):
            calls.append((args,options));return SimpleNamespace(returncode=0,stdout=json.dumps(document()).encode() if len(calls)==1 else stat+argv)
        self.assertTrue(app.read_core(item,execute=execute,now=lambda:NOW)['quiescent'])
        self.assertTrue(all(call[0][:2]==['exec','owned'] for call in calls))
        self.assertTrue(all(call[1]['timeout']==2 for call in calls))
        def too_large(*args,**options):return SimpleNamespace(returncode=0,stdout=b'x'*(app.LIMIT+1))
        with self.assertRaises(ValueError):app.read_core(item,execute=too_large,now=lambda:NOW)
        bad=deepcopy(item);bad['State']['Running']=False
        with self.assertRaises(ValueError):app.read_core(bad,execute=execute,now=lambda:NOW)


if __name__=='__main__':unittest.main()
