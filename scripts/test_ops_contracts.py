import copy
import importlib.util
from pathlib import Path
import unittest
import yaml

spec = importlib.util.spec_from_file_location('ops_contracts', Path(__file__).with_name('check-ops-contracts.py'))
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)
ROOT = Path(__file__).resolve().parents[1]


class OpsContractTests(unittest.TestCase):
    def test_release_coverage_is_independent_of_yaml_formatting(self):
        workflow = yaml.safe_load((ROOT / '.github/workflows/release.yml').read_text())
        reparsed = yaml.safe_load(yaml.safe_dump(workflow, default_flow_style=True))
        self.assertEqual(checks.release_components(workflow), checks.release_components(reparsed))

    def test_missing_or_duplicate_signatures_fail(self):
        workflow = yaml.safe_load((ROOT / '.github/workflows/release.yml').read_text())
        signing = workflow['jobs']['sign']['strategy']['matrix']['include']
        signing.pop()
        with self.assertRaisesRegex(ValueError, 'differ'):
            checks.release_components(workflow)
        signing.append(signing[0])
        with self.assertRaisesRegex(ValueError, 'unique'):
            checks.release_components(workflow)

    def test_worker_targets_must_resolve_and_have_unique_roles(self):
        compose = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())
        prometheus = yaml.safe_load((ROOT / 'logging/prometheus.yml').read_text())
        self.assertEqual(len(checks.worker_scrapes(compose, prometheus)), 5)
        broken = copy.deepcopy(prometheus)
        worker_job = next(job for job in broken['scrape_configs'] if job['job_name'] == 'meshcore-workers')
        worker_job['static_configs'][0]['targets'] = ['removed-worker:9091']
        with self.assertRaisesRegex(ValueError, 'Compose service'):
            checks.worker_scrapes(compose, broken)
        worker_job['static_configs'][0]['targets'] = ['region-snapshot-worker:9091']
        worker_job['static_configs'][0]['labels']['role'] = worker_job['static_configs'][1]['labels']['role']
        with self.assertRaisesRegex(ValueError, 'roles'):
            checks.worker_scrapes(compose, broken)
