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

    def postgres_settings(self, service):
        command = service['command']
        self.assertEqual(command[0], 'postgres')
        self.assertEqual(len(command) % 2, 1)
        settings = {}
        for index in range(1, len(command), 2):
            self.assertEqual(command[index], '-c')
            name, value = command[index + 1].split('=', 1)
            self.assertNotIn(name, settings)
            settings[name] = value
        return settings

    def test_postgres_policy_resolves_into_only_existing_database_with_original_bindings(self):
        services = compose('scripts/local-sample/compose.yml')
        self.assertEqual(set(services), {'db', 'redis', 'mqtt-log-init', 'mqtt'})
        database = services['db']
        self.assertEqual(self.postgres_settings(database), {
            'shared_preload_libraries': 'timescaledb,pg_stat_statements',
            'max_parallel_workers_per_gather': '0', 'max_connections': '100',
            'shared_buffers': '256MB', 'maintenance_work_mem': '64MB',
            'autovacuum_work_mem': '32MB',
        })
        self.assertEqual(database['image'], 'timescale/timescaledb@sha256:22e8a5ae7aef121d1537afe946dd7cc5deeeb63ab36ce19849d671bd3b663509')
        self.assertEqual(float(database['cpus']), 2)
        self.assertEqual(int(database['shm_size']), 1024 * MIB)
        self.assertEqual(database['container_name'], 'ukmesh-sample-db')
        self.assertEqual(database['environment'], {
            'POSTGRES_USER': 'meshcore', 'POSTGRES_PASSWORD': 'meshcore', 'POSTGRES_DB': 'meshcore',
        })
        self.assertEqual(len(database['ports']), 1)
        port = database['ports'][0]
        self.assertEqual((port['host_ip'], int(port['published']), int(port['target'])), ('127.0.0.1', 5433, 5432))
        volumes = {item['target']: item for item in database['volumes']}
        self.assertEqual(volumes['/var/lib/postgresql/data']['type'], 'volume')
        self.assertEqual(volumes['/var/lib/postgresql/data']['source'], 'sample-db')
        schema = volumes['/docker-entrypoint-initdb.d/010-base.sql']
        self.assertEqual(Path(schema['source']), ROOT / 'backend/src/db/schema/base.sql')
        self.assertTrue(schema['read_only'])
        self.assertEqual(database['healthcheck']['test'], ['CMD-SHELL', 'pg_isready -h 127.0.0.1 -U meshcore -d meshcore'])

    def test_postgres_memory_overrides_change_only_three_settings_without_changing_resources(self):
        before = compose('scripts/local-sample/compose.yml')
        after = compose('scripts/local-sample/compose.yml', POSTGRES_SHARED_BUFFERS='512MB',
                        POSTGRES_MAINTENANCE_WORK_MEM='128MB', POSTGRES_AUTOVACUUM_WORK_MEM='64MB')
        self.assertEqual(set(after), set(before))
        expected = self.postgres_settings(before['db'])
        expected.update(shared_buffers='512MB', maintenance_work_mem='128MB', autovacuum_work_mem='64MB')
        self.assertEqual(self.postgres_settings(after['db']), expected)
        for service in before:
            if service != 'db':
                self.assertEqual(after[service], before[service])
        expected_database = dict(before['db'])
        expected_database['command'] = after['db']['command']
        self.assertEqual(after['db'], expected_database)
        self.assertEqual(int(after['db']['mem_limit']), 2048 * MIB)
        self.assertEqual(int(after['db']['memswap_limit']), 2048 * MIB)

    def test_ci_retains_its_distinct_database_capacity_and_is_not_affected_by_memory_overrides(self):
        files = ('docker-compose.yml', 'docker-compose.ci.yml')
        before = compose(*files)['timescaledb']
        after = compose(*files, POSTGRES_SHARED_BUFFERS='512MB',
                        POSTGRES_MAINTENANCE_WORK_MEM='128MB', POSTGRES_AUTOVACUUM_WORK_MEM='64MB')['timescaledb']
        self.assertEqual(after, before)
        self.assertEqual(int(before['mem_limit']), 4096 * MIB)
        self.assertEqual(int(before['memswap_limit']), 4096 * MIB)
        self.assertEqual(int(before['shm_size']), 1024 * MIB)
        self.assertEqual(self.postgres_settings(before), {
            'max_connections': '300', 'max_worker_processes': '24', 'max_parallel_workers_per_gather': '0',
        })

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
