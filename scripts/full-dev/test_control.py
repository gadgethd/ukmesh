"""Broker builds and finite jobs fail startup without touching retained fixtures."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class FullStartupCompletion(unittest.TestCase):
    def test_migration_and_backfill_have_distinct_successful_completion_gates(self):
        with tempfile.TemporaryDirectory(prefix='full controller ') as temporary:
            root = Path(temporary)
            tools = root / 'scripts/full-dev'
            tools.mkdir(parents=True)
            shutil.copyfile(Path(__file__).with_name('control.sh'), tools / 'control.sh')
            (tools / 'wiring.py').write_text('')
            # This fixture isolates finite-job completion. Lease/entry behavior
            # is exercised by qualification/test_target_entry.py with real FDs.
            (tools/'qualification').mkdir()
            (tools/'qualification/target_entry.py').write_text('import sys;sys.exit(0)\n')
            (tools / 'access.sh').write_text('#!/bin/sh\ntouch access-called\n')
            (tools / 'runtime.sh').write_text('''repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
compose=(docker compose)
UKMESH_DATABASE=lean_fixture_owned OWNER_DATABASE=lean_owner_owned
BACKEND_IMAGE=ready APP_IMAGE=ready WEBSITE_IMAGE=ready WEBSITE_DEV_IMAGE=ready
TAGGER_WORKER_IMAGE=ready RF_WORKER_IMAGE=ready HOPREACH_IMAGE=ready HEALTHCHECK_IMAGE=ready MOSQUITTO_RELOADER_IMAGE=ready
refresh_wiring_revisions() { :; }
''')
            library = root / 'scripts/lib'
            library.mkdir()
            (library / 'owned-processes.sh').write_text("owned_stop_process() { printf 'APP_STOP\\n' >> calls; }\n")
            executables = root / 'tools'
            executables.mkdir()
            docker = executables / 'docker'
            docker.write_text('''#!/usr/bin/python3
from pathlib import Path
import sys
args=sys.argv[1:]
def called(value):
    with (Path.cwd()/'calls').open('a') as output: output.write(value+'\\n')
if args[:1]==['compose']:
    sample='--project-name' in args and args[args.index('--project-name')+1]=='ukmesh-sample'
    if sample and 'build' in args:
        if args[-2:]!=['build','mqtt']: sys.exit(92)
        called('BROKER_IMAGE')
        if (Path.cwd()/'fail-broker-image').exists():
            print('fixture broker image build failed',file=sys.stderr);sys.exit(43)
    elif sample and 'up' in args: called('INFRA_UP')
    elif not sample and 'up' in args: called('MAIN_UP:'+args[-1])
if 'config' in args and '--services' in args: print('backend\\nlink-backfill-worker\\ndb-migrate')
if 'ps' in args: print('owned-'+args[-1]+'-container')
if 'logs' in args: print(args[-1]+' retained output')
if args and args[0]=='inspect':
    name='migration-result' if args[-1]=='owned-db-migrate-container' else 'backfill-result'
    print((Path.cwd()/name).read_text())
''')
            docker.chmod(0o755)
            environment = {'PATH': str(executables) + ':' + os.environ['PATH'], 'HOME': temporary}
            for stage in ('migration', 'backfill'):
              for state in ('exited:1', 'running:0', 'exited:0'):
                with self.subTest(stage=stage, state=state):
                    (root / 'migration-result').write_text(state if stage=='migration' else 'exited:0')
                    (root / 'backfill-result').write_text(state if stage=='backfill' else 'exited:0')
                    (root / 'access-called').unlink(missing_ok=True)
                    (root / 'calls').write_text('')
                    result = subprocess.run(['bash', str(tools / 'control.sh'), 'start'], cwd=root,
                                            env=environment, capture_output=True, text=True, timeout=20)
                    calls = (root / 'calls').read_text().splitlines()
                    self.assertEqual(calls[:3], ['BROKER_IMAGE', 'INFRA_UP', 'MAIN_UP:db-migrate'])
                    self.assertEqual(calls.count('BROKER_IMAGE'), 1)
                    if state == 'exited:0':
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertIn('UKMesh http://', result.stdout)
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        label='Database migration' if stage=='migration' else 'Link backfill'
                        self.assertIn(label+' job failed or did not complete', result.stderr)
                        self.assertNotIn('UKMesh http://', result.stdout)
                    self.assertEqual((root / 'access-called').exists(), stage!='migration' or state=='exited:0')
                    self.assertIn('db-migrate retained output', (root / 'plans/lean-runtime/runtime/migrate.log').read_text())

            # Broker failure precedes infrastructure, migration, process retirement and apps.
            state = root / 'plans/dev-stack'
            state.mkdir(parents=True)
            (state / 'fixture-ready').write_text('retained snapshot\n')
            (state / 'backend.pid').write_text('retained PID authority\n')
            data = root / 'retained-data'
            data.mkdir()
            (data / 'marker').write_bytes(b'owned fixture bytes\x00unchanged')
            protected = [state / 'fixture-ready', state / 'backend.pid', data / 'marker']
            before = {path: path.read_bytes() for path in protected}
            (root / 'fail-broker-image').touch()
            (root / 'calls').write_text('')
            (root / 'access-called').unlink(missing_ok=True)
            result = subprocess.run(['bash', str(tools / 'control.sh'), 'start'], cwd=root,
                                    env=environment, capture_output=True, text=True, timeout=20)
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertIn('fixture broker image build failed', result.stderr)
            self.assertEqual((root / 'calls').read_text().splitlines(), ['BROKER_IMAGE'])
            self.assertFalse((root / 'access-called').exists())
            self.assertNotIn('UKMesh http://', result.stdout)
            self.assertEqual({path: path.read_bytes() for path in protected}, before)


if __name__ == '__main__':
    unittest.main()
