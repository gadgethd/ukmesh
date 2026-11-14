"""A finite backfill failure must fail startup even when Compose wait returns zero."""
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
            (library / 'owned-processes.sh').write_text('owned_stop_process() { :; }\n')
            executables = root / 'tools'
            executables.mkdir()
            docker = executables / 'docker'
            docker.write_text('''#!/usr/bin/python3
from pathlib import Path
import sys
args=sys.argv[1:]
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
                    result = subprocess.run(['bash', str(tools / 'control.sh'), 'start'], cwd=root,
                                            env=environment, capture_output=True, text=True, timeout=20)
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


if __name__ == '__main__':
    unittest.main()
