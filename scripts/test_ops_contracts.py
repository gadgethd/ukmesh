import copy
import importlib.util
import json
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


    def test_shared_link_defaults_are_semantic_across_compose_and_example(self):
        contract = json.loads((ROOT / 'contracts/link-v3.json').read_text())
        compose = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())
        example = (ROOT / '.env.example').read_text()
        expected = {spec['env'] for spec in contract['limits'].values()}
        self.assertEqual(checks.link_queue_defaults(contract, compose, example), expected)
        # Compose's list form and quoted decimal documentation mean the same thing.
        for service in ('backend', 'link-worker'):
            compose['services'][service]['environment'] = [f'{key}={value}' for key, value in compose['services'][service]['environment'].items()]
        for spec in contract['limits'].values():
            example = example.replace(f"{spec['env']}={spec['default']}\n", f"export {spec['env']}='{spec['default']}' # documented default\n")
        self.assertEqual(checks.link_queue_defaults(contract, compose, example), expected)

    def test_shared_link_defaults_reject_changed_values_names_and_missing_documentation(self):
        contract = json.loads((ROOT / 'contracts/link-v3.json').read_text())
        baseline = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())
        example = (ROOT / '.env.example').read_text()
        spec = contract['limits']['maxJobs']
        name, default = spec['env'], spec['default']
        for service in ('backend', 'link-worker'):
            for value in (f'${{{name}:-{default + 1}}}', f'${{WRONG_QUEUE_SETTING:-{default}}}', None):
                with self.subTest(service=service, value=value):
                    changed = copy.deepcopy(baseline)
                    changed['services'][service]['environment'][name] = value
                    with self.assertRaisesRegex(ValueError, service):
                        checks.link_queue_defaults(contract, changed, example)
        for replacement in (f'{name}={default + 1}\n', f'WRONG_QUEUE_SETTING={default}\n', ''):
            with self.subTest(documentation=replacement):
                changed = example.replace(f'{name}={default}\n', replacement)
                with self.assertRaisesRegex(ValueError, r'\.env\.example'):
                    checks.link_queue_defaults(contract, baseline, changed)
