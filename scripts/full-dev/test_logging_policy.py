"""Resolve producer logging and existing collector label contracts without a daemon."""
from pathlib import Path
import json
import re
import unittest
import yaml

from test_memory_policy import compose,ROOT

JOURNAL={'driver':'journald','options':{'tag':'{{.Name}}','labels':'com.docker.compose.service,com.docker.compose.project'}}
SELF_LOG={'driver':'json-file','options':{'max-size':'10m','max-file':'3'}}


class LoggingPolicy(unittest.TestCase):
    def check_journal(self,services):
        for name,service in services.items():
            with self.subTest(service=name):self.assertEqual(service.get('logging'),JOURNAL)

    def test_every_companion_including_finite_tools_uses_retained_journal(self):
        services=compose('scripts/companion-dev/compose.yml')
        self.assertEqual(set(services),{'build','web-build','beacon-db','beacon-redis','beacon-migrate',
                                      'corescope-ingestor','corescope','beacon','beacon-web'})
        self.check_journal(services)

    def test_hot_and_public_projection_merges_retain_producer_metadata(self):
        for overlay,settings in [('scripts/companion-dev/hot.yml',{}),
                ('scripts/full-dev/corescope-public.yml',{'CORESCOPE_PUBLIC_DIR':'/owned/public-projection'})]:
            with self.subTest(overlay=overlay):
                self.check_journal(compose('scripts/companion-dev/compose.yml',overlay,**settings))

    def test_sample_infrastructure_and_initializer_have_same_policy(self):
        services=compose('scripts/local-sample/compose.yml')
        self.assertEqual(set(services),{'db','redis','mqtt','mqtt-log-init'})
        self.check_journal(services)

    def test_all43_required_services_have_log_policy_and_alloy_exception_stays_bounded(self):
        owners={'ukmesh-full':compose('docker-compose.yml','scripts/full-dev/compose.yml'),
                'ukmesh-companions':compose('scripts/companion-dev/compose.yml'),
                'ukmesh-sample':compose('scripts/local-sample/compose.yml')}
        actual={service.get('container_name',f'{project}-{name}-1'):(name,service)
                for project,services in owners.items() for name,service in services.items()}
        required=[s for s in json.loads((ROOT/'scripts/full-dev/services.json').read_text())['services'] if s.get('required',True)]
        self.assertEqual(len(required),43)
        for record in required:
            container=record.get('container',record['name'])
            with self.subTest(container=container):
                self.assertIn(container,actual)
                name,service=actual[container]
                self.assertEqual(service.get('logging'),SELF_LOG if name=='alloy' else JOURNAL)
        # The main producer owns this job even outside the local overlay.
        main=yaml.safe_load((ROOT/'docker-compose.yml').read_text())
        self.assertEqual(main['services']['mesh-health-check-data-init']['logging'],JOURNAL)

    def test_existing_collector_accepts_all_projects_and_preserves_query_labels(self):
        source=(ROOT/'logging/alloy/config.alloy').read_text()
        rules=re.findall(r'\brule\s*\{([^}]+)\}',source,re.S)
        mappings={}
        for rule in rules:
            labels=re.search(r'source_labels\s*=\s*\["([^"]+)"\]',rule)
            target=re.search(r'target_label\s*=\s*"([^"]+)"',rule)
            if labels and target:mappings[labels[1]]=target[1]
            if re.search(r'action\s*=\s*"(?:keep|drop)"',rule):
                self.assertIsNotNone(labels);self.assertEqual(labels[1],'__journal_com_docker_compose_service')
                self.assertRegex(rule,r'regex\s*=\s*"\.\+"')
        self.assertEqual(mappings['__journal_com_docker_compose_service'],'service')
        self.assertEqual(mappings['__journal_com_docker_compose_project'],'project')
        self.assertEqual(mappings['__journal_container_name'],'container')
        self.assertIn('loki.source.journal "docker"',source)
        self.assertNotIn('loki.source.docker',source)
        alloy=compose('docker-compose.yml','scripts/full-dev/compose.yml')['alloy']
        mounts={m['target'] for m in alloy['volumes']}
        self.assertTrue({'/var/log/journal','/run/log/journal','/var/lib/alloy'}.issubset(mounts))
        self.assertNotIn('/var/run/docker.sock',mounts)
        self.assertIn('--storage.path=/var/lib/alloy/data',alloy['command'])


if __name__=='__main__':unittest.main()
