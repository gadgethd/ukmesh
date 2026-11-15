"""Resolve every local memory boundary without deployment env or daemon access."""
from pathlib import Path
import json
import os
import subprocess
import unittest

ROOT=Path(__file__).resolve().parents[2]
MIB=1024**2


def compose(*files,**overrides):
    env={k:os.environ[k] for k in ('PATH','HOME') if k in os.environ}
    env.update({
        'POSTGRES_PASSWORD':'owned-fixture-only','MQTT_PASSWORD':'owned-fixture-only',
        'REDIS_PASSWORD':'owned-fixture-only','JWT_SECRET':'owned-fixture-secret-at-least-32-characters',
        'OPERATOR_SITE_TOKEN':'owned-fixture-only','ALERT_RECEIVER_TOKEN':'owned-fixture-only',
        'ANUBIS_ED25519_PRIVATE_KEY_HEX':'00'*64,'GRAFANA_ADMIN_PASSWORD':'owned-fixture-only',
        'HEALTHCHECK_SOURCE_REF':'0'*40,'CLOUDFLARE_TUNNEL_TOKEN':'',
        'BACKEND_IMAGE':'owned-backend:local','APP_IMAGE':'owned-app:local',
        'HEALTHCHECK_IMAGE':'owned-health:local','MOSQUITTO_RELOADER_IMAGE':'owned-reloader:local'})
    env.update(overrides)
    args=['docker','compose','--project-name','owned-memory-contract','--env-file','/dev/null','--profile','*']
    for name in files:args.extend(['-f',name])
    args.extend(['config','--format','json'])
    result=subprocess.run(args,cwd=ROOT,env=env,capture_output=True,text=True,check=True,timeout=30)
    return json.loads(result.stdout)['services']


class LocalMemoryPolicy(unittest.TestCase):
    def check_pairs(self,services,expected):
        self.assertEqual(set(services),set(expected))
        for name,size in expected.items():
            with self.subTest(name=name):
                self.assertEqual(int(services[name]['mem_limit']),size*MIB)
                self.assertEqual(int(services[name]['memswap_limit']),size*MIB)

    def test_sample_caps_unchanged_and_total_swap_equals_ram(self):
        self.check_pairs(compose('scripts/local-sample/compose.yml'),
                         {'db':2048,'redis':256,'mqtt-log-init':16,'mqtt':256})

    def test_all_companion_caps_include_build_and_migration_jobs(self):
        self.check_pairs(compose('scripts/companion-dev/compose.yml'),{
            'build':1200,'beacon-db':512,'beacon-redis':128,'corescope-ingestor':384,
            'corescope':384,'beacon-migrate':256,'beacon':384,'web-build':1200,'beacon-web':48})

    def test_full_overlay_helpers_and_all_required_main_caps_are_paired(self):
        services=compose('docker-compose.yml','scripts/full-dev/compose.yml')
        for name,service in services.items():
            self.assertGreater(int(service['mem_limit']),0,name)
            self.assertEqual(int(service['memswap_limit']),int(service['mem_limit']),name)
        for name in ('external-test','alert-bootstrap','mosquitto-reloader'):
            self.assertEqual(int(services[name]['mem_limit']),64*MIB)
        alloy=services['alloy']
        self.assertEqual(int(alloy['mem_limit']),128*MIB)
        self.assertEqual(alloy['environment']['GOMEMLIMIT'],'96MiB')
        self.assertEqual(alloy['command'],['run','--server.http.listen-addr=0.0.0.0:12345',
                                         '--storage.path=/var/lib/alloy/data','/etc/alloy/config.alloy'])
        mounts={m['target'] for m in alloy['volumes']}
        self.assertTrue({'/etc/alloy/config.alloy','/var/log/journal','/run/log/journal',
                         '/etc/machine-id','/var/lib/alloy'}.issubset(mounts))

    def test_larger_host_can_override_both_alloy_budgets_explicitly(self):
        services=compose('docker-compose.yml','scripts/full-dev/compose.yml',
                         ALLOY_MEMORY_LIMIT='256m',ALLOY_GOMEMLIMIT='192MiB')
        self.assertEqual(int(services['alloy']['mem_limit']),256*MIB)
        self.assertEqual(int(services['alloy']['memswap_limit']),256*MIB)
        self.assertEqual(services['alloy']['environment']['GOMEMLIMIT'],'192MiB')


if __name__=='__main__':unittest.main()
