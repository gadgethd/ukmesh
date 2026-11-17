"""Effective cgroup and native swap evidence, without Docker or host mutations."""
from pathlib import Path
from tempfile import TemporaryDirectory
import copy
import json
import unittest
from unittest.mock import patch

import resources
from test_qualification import resource_rows


class EffectiveMemoryEvidence(unittest.TestCase):
    def files(self, directory):
        group=Path(directory)
        for name,value in {'memory.max':'1024','memory.swap.max':'0','memory.swap.current':'12',
                           'memory.swap.events':'high 0\nmax 2\nfail 1'}.items():
            (group/name).write_text(value+'\n')
        return group

    def test_actual_limits_are_separate_from_docker_intent_and_existing_swap(self):
        with TemporaryDirectory() as temporary:
            group=self.files(temporary)
            item=resources.memory_limit_evidence(group,{'Memory':1024,'MemorySwap':1024})
            self.assertTrue(resources.memory_limits_match(item))
            self.assertEqual(item['swap_current_bytes'],12)
            self.assertEqual(item['swap_events'],{'high':0,'max':2,'fail':1})
            for actual in ('max','1024'):
                (group/'memory.swap.max').write_text(actual)
                item=resources.memory_limit_evidence(group,{'Memory':1024,'MemorySwap':1024})
                self.assertEqual(item['memory_swap_limit_bytes'],1024)
                self.assertEqual(item['effective_swap_max'],'max' if actual=='max' else 1024)
                self.assertFalse(resources.memory_limits_match(item))
            (group/'memory.swap.max').write_text('0')
            (group/'memory.max').write_text('max')
            self.assertFalse(resources.memory_limits_match(resources.memory_limit_evidence(group,{'Memory':1024,'MemorySwap':1024})))

    def test_missing_and_malformed_kernel_values_cannot_be_zero_or_unlimited(self):
        for filename in ('memory.max','memory.swap.max','memory.swap.current','memory.swap.events'):
            with self.subTest(filename=filename),TemporaryDirectory() as temporary:
                group=self.files(temporary);(group/filename).unlink()
                with self.assertRaises(FileNotFoundError):resources.memory_limit_evidence(group,{'Memory':1024,'MemorySwap':1024})
        for filename,bad in [('memory.max','-1'),('memory.swap.max',''),('memory.swap.current','max'),
                             ('memory.swap.current','-2'),('memory.swap.current','True'),
                             ('memory.swap.events','max -1'),('memory.swap.events',''),
                             ('memory.swap.events','max 1\nmax 2'),('memory.swap.events','max 1 extra')]:
            with self.subTest(filename=filename,bad=bad),TemporaryDirectory() as temporary:
                group=self.files(temporary);(group/filename).write_text(bad)
                with self.assertRaises(ValueError):resources.memory_limit_evidence(group,{'Memory':1024,'MemorySwap':1024})

    def test_requested_default_unlimited_or_extra_swap_is_not_no_swap(self):
        with TemporaryDirectory() as temporary:
            group=self.files(temporary)
            for config in ({'Memory':1024,'MemorySwap':0},{'Memory':1024,'MemorySwap':-1},
                           {'Memory':1024,'MemorySwap':2048},{'Memory':0,'MemorySwap':0}):
                with self.subTest(config=config):
                    self.assertFalse(resources.memory_limits_match(resources.memory_limit_evidence(group,config)))
            for config in ({'Memory':True,'MemorySwap':1024},{'Memory':1024,'MemorySwap':-2}):
                with self.assertRaises(ValueError):resources.memory_limit_evidence(group,config)

    def test_cgroup_membership_cannot_escape_root_or_fallback_to_v1(self):
        with TemporaryDirectory() as temporary:
            root=Path(temporary);proc=root/'proc';(proc/'123').mkdir(parents=True)
            groups=root/'cgroups';(groups/'owned').mkdir(parents=True)
            file=proc/'123/cgroup';file.write_text('0::/owned\n')
            self.assertEqual(resources.container_cgroup(123,proc_root=proc,cgroup_root=groups),groups/'owned')
            (groups/'linked').symlink_to(root)
            for text in ('0::/../../outside','0::/linked/outside','1:memory:/owned','0::owned','0::/owned\n0::/other'):
                with self.subTest(text=text):
                    file.write_text(text)
                    with self.assertRaises((ValueError,RuntimeError)):resources.container_cgroup(123,proc_root=proc,cgroup_root=groups)
            with self.assertRaises(ValueError):resources.container_cgroup(True,proc_root=proc,cgroup_root=groups)

    def test_collector_reads_kernel_files_when_docker_claims_no_swap(self):
        with TemporaryDirectory() as temporary:
            group=self.files(temporary)
            for name,value in {'memory.current':'100','memory.stat':'inactive_file 10\nanon 70\nfile 30',
                               'memory.events':'oom_kill 0','io.stat':'8:0 rbytes=1 wbytes=2 rios=3 wios=4',
                               'cpu.stat':'usage_usec 5'}.items():(group/name).write_text(value+'\n')
            (group/'memory.swap.max').write_text('512')
            item={'Name':'/api','Id':'id','RestartCount':0,'State':{'Pid':123,'OOMKilled':False,'StartedAt':'now'},
                  'HostConfig':{'Memory':1024,'MemorySwap':1024}}
            def docker(args):return type('Result',(),{'stdout':b'id\n' if args==['ps','-q'] else json.dumps([item]).encode()})()
            with patch.object(resources,'docker',side_effect=docker),patch.object(resources,'container_cgroup',return_value=group):
                row=resources.container_samples()[0]
            self.assertEqual(row['working_bytes'],90)
            self.assertEqual(row['effective_swap_max'],512)
            self.assertFalse(resources.memory_limits_match(row))

    def test_native_swap_pss_is_separate_and_missing_coverage_explicit(self):
        with TemporaryDirectory() as temporary:
            root=Path(temporary)
            for pid,text in {1:'Pss: 10 kB\nSwapPss: 3 kB\n',2:'Pss: 20 kB\n',3:'Pss: 5 kB\nSwapPss: 2 kB\n'}.items():
                (root/str(pid)).mkdir();(root/str(pid)/'smaps_rollup').write_text(text)
            got=resources.process_memory({1,3,99},proc_root=root)
            self.assertEqual(got['total_bytes'],15*1024)
            self.assertEqual(got['swap_total_bytes'],5*1024)
            self.assertTrue(got['swap_coverage_complete'])
            got=resources.process_memory({1,2},proc_root=root)
            self.assertEqual(got['total_bytes'],30*1024)
            self.assertEqual(got['swap_total_bytes'],3*1024)
            self.assertIsNone(got['swap_processes']['2'])
            self.assertFalse(got['swap_coverage_complete'])

    def test_acceptance_checks_effective_caps_and_keeps_host_swap_thresholds(self):
        rows=resource_rows();self.assertTrue(resources.evaluate(rows,duration=2400,warmup=120)['passed'])
        for key,value in [('effective_swap_max','max'),('effective_swap_max',1024),('effective_memory_max',1),
                          ('memory_swap_limit_bytes',0),('swap_current_bytes',None),('swap_events',{})]:
            bad=copy.deepcopy(rows);bad[-1]['containers'][0][key]=value
            result=resources.evaluate(bad,duration=2400,warmup=120)
            self.assertFalse(result['passed']);self.assertEqual(result['effective_limit_mismatches'],['api'])
        bad=copy.deepcopy(rows);del bad[0]['containers'][0]['effective_swap_max']
        self.assertFalse(resources.evaluate(bad,duration=2400,warmup=120)['passed'])
        for current in (0,256*1024*1024):
            bad=copy.deepcopy(rows);bad[-1]['host']['swap_used_bytes']+=129*resources.MIB
            bad[-1]['containers'][0]['swap_current_bytes']=current
            result=resources.evaluate(bad,duration=2400,warmup=120)
            self.assertFalse(result['checks']['swap_growth'])
            self.assertTrue(result['checks']['effective_container_memory_limits'])


if __name__=='__main__':unittest.main()
