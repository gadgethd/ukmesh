"""Exercise component lifecycle at the external Docker boundary without a daemon."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('build_inputs', HERE / 'source-hash.py')
build_inputs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_inputs)


class CompanionLifecycle(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='companion lifecycle ')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tools = self.root / 'scripts/companion-dev'
        self.tools.mkdir(parents=True)
        for name in ('control.sh', 'source-hash.py', 'artifacts.py', 'build.sh',
                     'build-corescope.sh', 'build-beacon.sh', 'web-build.sh', 'compose.yml',
                     'corescope.json', 'beacon.yaml', 'nginx.conf'):
            shutil.copyfile(HERE / name, self.tools / name)
        full = self.root / 'scripts/full-dev'
        full.mkdir()
        shutil.copyfile(HERE.parent / 'full-dev/environment.sh', full / 'environment.sh')
        self.git(self.root, 'init', '-q')
        (self.root / 'README').write_text('owned test checkout')
        self.git(self.root, 'add', 'README')
        self.git(self.root, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'fixture')
        for name in ('corescope', 'beacon-server', 'beacon-web'):
            checkout = self.root / 'third_party' / name
            checkout.mkdir(parents=True)
            self.git(checkout, 'init', '-q')
            (checkout / 'source.txt').write_text(name)
            self.git(checkout, 'add', 'source.txt')
            self.git(checkout, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'fixture')
        self.state = self.root / 'plans/companion-dev/runtime'
        (self.state / 'bin').mkdir(parents=True)
        for name in ('beacon', 'corescope-server', 'corescope-ingestor'):
            binary = self.state / 'bin' / name
            binary.write_text('last good executable')
            binary.chmod(0o755)
        self.revisions = {mode: build_inputs.fingerprint(self.root, mode) for mode in build_inputs.PROJECTS}
        for component in ('corescope', 'beacon'):
            (self.state / (component + '-source')).write_text(self.revisions[component])
        (self.state / 'beacon-web').mkdir()
        (self.state / 'beacon-web/index.html').write_text('last good site')
        (self.state / 'web-source').write_text(self.revisions['web'])
        self.executables = self.root / 'tools'
        self.executables.mkdir()
        docker = self.executables / 'docker'
        docker.write_text('''#!/usr/bin/python3
import json, os
from pathlib import Path
import sys
root=Path.cwd(); args=sys.argv[1:]
with (root/'calls.jsonl').open('a') as log:
    log.write(json.dumps({'args':args, 'revisions':{key:os.getenv(key) for key in
      ('COMPANION_CORESCOPE_SOURCE','COMPANION_BEACON_SOURCE','COMPANION_WEB_SOURCE',
       'COMPANION_CORESCOPE_CONFIG','COMPANION_BEACON_CONFIG','COMPANION_WEB_CONFIG')}})+'\\n')
if 'run' in args and 'build' in args:
    component=args[-1]
    names=['beacon'] if component=='beacon' else ['corescope-server','corescope-ingestor']
    for name in names:
        file=root/'plans/companion-dev/runtime/bin'/name
        file.write_text('rebuilt '+component); file.chmod(0o755)
if 'run' in args and 'web-build' in args:
    output=root/'plans/companion-dev/runtime/beacon-web.next'
    (output/'partial.js').write_text('partial asset')
    if (root/'fail-web').exists(): sys.exit(42)
    (output/'index.html').write_text('complete new site')
if 'ps' in args and 'beacon-migrate' in args: print('owned-migration-container')
if args and args[0]=='inspect': print('exited:1' if (root/'fail-migration').exists() else 'exited:0')
''')
        docker.chmod(0o755)
        (self.tools / 'beacon-access.sh').write_text('#!/bin/sh\ntouch access-called\n')

    def git(self, checkout, *arguments):
        return subprocess.check_output(['git', '-C', str(checkout), *arguments], stderr=subprocess.DEVNULL)

    def run_control(self, *arguments):
        environment = {'PATH': str(self.executables) + ':' + os.environ['PATH'],
                       'HOME': str(self.root), 'LANG': 'C.UTF-8'}
        return subprocess.run(['bash', str(self.tools / 'control.sh'), *arguments],
                              cwd=self.root, env=environment, capture_output=True, text=True, timeout=30)

    def calls(self):
        return [json.loads(line) for line in (self.root / 'calls.jsonl').read_text().splitlines()]

    def test_web_restart_does_not_require_or_restart_unrelated_repositories(self):
        shutil.rmtree(self.root / 'third_party/corescope')
        (self.root / 'third_party/beacon-web/source.txt').write_text('changed web source')
        result = self.run_control('restart', 'beacon-web')
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual([row['args'][-1] for row in calls if 'run' in row['args']], ['web-build'])
        starts = [row for row in calls if 'up' in row['args'] or 'restart' in row['args']]
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]['args'][-1], 'beacon-web')
        self.assertIn('--no-deps', starts[0]['args'])
        self.assertIn('--force-recreate', starts[0]['args'])
        self.assertEqual(starts[0]['revisions']['COMPANION_BEACON_SOURCE'], self.revisions['beacon'])
        self.assertEqual(starts[0]['revisions']['COMPANION_CORESCOPE_SOURCE'], self.revisions['corescope'])
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'complete new site')

    def test_core_rebuild_replaces_both_processes_without_starting_beacon(self):
        (self.root / 'third_party/corescope/source.txt').write_text('changed core source')
        result = self.run_control('restart', 'corescope-ingestor')
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual([row['args'][-1] for row in calls if 'run' in row['args']], ['corescope'])
        starts = [row for row in calls if 'up' in row['args'] or 'restart' in row['args']]
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]['args'][-2:], ['corescope-ingestor', 'corescope'])

    def test_failed_web_build_preserves_site_and_next_restart_recovers(self):
        (self.root / 'third_party/beacon-web/source.txt').write_text('changed web source')
        (self.root / 'fail-web').touch()
        result = self.run_control('restart', 'beacon-web')
        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
        self.assertFalse(any('up' in row['args'] for row in self.calls()))
        (self.root / 'fail-web').unlink()
        result = self.run_control('restart', 'beacon-web')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'complete new site')

    def test_fingerprint_failure_does_not_build_or_start(self):
        (self.root / 'third_party/beacon-server/escaped').symlink_to(self.tools / 'build.sh')
        result = self.run_control('restart', 'beacon')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('outside its repository', result.stderr)
        self.assertFalse(any('run' in row['args'] or 'up' in row['args'] for row in self.calls()))
        self.assertEqual((self.state / 'beacon-source').read_text(), self.revisions['beacon'])

    def test_migration_restart_executes_named_job_once_before_access_provisioning(self):
        result = self.run_control('restart', 'beacon-migrate')
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertFalse(any('run' in row['args'] for row in calls))
        self.assertEqual(len([row for row in calls if 'up' in row['args'] and row['args'][-1]=='beacon-migrate']), 1)
        self.assertTrue((self.root / 'access-called').exists())

    def test_failed_migration_never_provisions_access(self):
        (self.root / 'fail-migration').touch()
        result = self.run_control('restart', 'beacon-migrate')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('migration job failed', result.stderr)
        self.assertFalse((self.root / 'access-called').exists())

    def test_incomplete_unrelated_static_build_does_not_block_core_restart(self):
        (self.state / 'beacon-web.next').symlink_to(self.root, target_is_directory=True)
        result = self.run_control('restart', 'corescope')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.state / 'beacon-web.next').is_symlink())

    def test_config_change_does_not_rebuild_companion_sources(self):
        (self.tools / 'beacon.yaml').write_text('changed local settings')
        result = self.run_control('start')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any('build' in row['args'] or 'web-build' in row['args'] for row in self.calls()))
        final_start = [row for row in self.calls() if 'up' in row['args']][-1]
        self.assertEqual(final_start['revisions']['COMPANION_BEACON_CONFIG'],
                         hashlib.sha256(b'changed local settings').hexdigest())


if __name__ == '__main__':
    unittest.main()
