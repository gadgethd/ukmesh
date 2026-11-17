"""Published metadata has the same owner in canonical and temporary starts."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


class PublishedRevisions(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='published revisions ')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tools = self.root / 'scripts/companion-dev'
        self.tools.mkdir(parents=True)
        for name in ('revisions.sh', 'artifacts.py'):
            shutil.copyfile(HERE / name, self.tools / name)
        self.state = self.root / 'plans/companion-dev/runtime'
        self.state.mkdir(parents=True)
        (self.state / 'corescope-source').write_text('a' * 64 + '\n')
        (self.state / 'beacon-source').write_text('b' * 64 + '\n')
        for name, body in (('corescope.json', '{"mqttSources":[]}'),
                           ('beacon.yaml', 'retention: 30'), ('nginx.conf', 'server {}')):
            (self.tools / name).write_text(body)
        active = self.state / 'beacon-web'
        active.mkdir()
        (active / 'index.html').write_text('complete owned site')
        (active / '.ukmesh-source-revision').write_text('c' * 64 + '\n')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name in ('git', 'go', 'node', 'docker'):
            file = self.bin / name
            file.write_text('#!/bin/sh\ntouch "$HOME/unexpected-external-command"\nexit 99\n')
            file.chmod(0o755)
        self.environment = {'PATH': str(self.bin) + ':' + os.environ['PATH'],
            'HOME': str(self.root), 'LANG': 'C.UTF-8',
            'COMPANION_CORESCOPE_SOURCE': 'poisoned source', 'COMPANION_CORESCOPE_CONFIG': 'poisoned config'}

    def load(self, component='all', config=None, arguments=()):
        command = ['bash', str(self.tools / 'revisions.sh'), '--component', component]
        if config is not None:
            command += ['--core-config', str(config)]
        command += ['--', sys.executable, '-c',
            'import json,os,sys; print(json.dumps({"arguments":sys.argv[1:],"revisions":{k:v for k,v in os.environ.items() if k.startswith("COMPANION_")}}))',
            *arguments]
        completed = subprocess.run(command, cwd=self.root, env=self.environment, capture_output=True, text=True, timeout=10)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertFalse((self.root / 'unexpected-external-command').exists())
        return json.loads(completed.stdout)

    def test_canonical_metadata_uses_published_inputs_and_artifact_owner(self):
        value = self.load()['revisions']
        self.assertEqual(value['COMPANION_CORESCOPE_SOURCE'], 'a' * 64)
        self.assertEqual(value['COMPANION_BEACON_SOURCE'], 'b' * 64)
        self.assertEqual(value['COMPANION_WEB_SOURCE'], 'c' * 64)
        for component, name in (('CORESCOPE', 'corescope.json'), ('BEACON', 'beacon.yaml'), ('WEB', 'nginx.conf')):
            self.assertEqual(value['COMPANION_' + component + '_CONFIG'],
                             hashlib.sha256((self.tools / name).read_bytes()).hexdigest())
        self.assertRegex(value['COMPANION_WEB_PUBLICATION'], '^[a-f0-9]{64}$')
        (self.state / 'beacon-web/index.html').write_text('changed published bytes')
        changed = self.load()['revisions']
        self.assertEqual(changed['COMPANION_WEB_SOURCE'], value['COMPANION_WEB_SOURCE'])
        self.assertNotEqual(changed['COMPANION_WEB_PUBLICATION'], value['COMPANION_WEB_PUBLICATION'])

    def test_core_only_never_requires_other_components_or_compilation(self):
        for name in ('beacon.yaml', 'nginx.conf', 'artifacts.py'):
            (self.tools / name).unlink()
        (self.state / 'beacon-source').unlink()
        shutil.rmtree(self.state / 'beacon-web')
        argument = 'literal argument; $(touch injected)'
        value = self.load('corescope', arguments=[argument])
        self.assertEqual(value['arguments'], [argument])
        self.assertEqual(set(value['revisions']), {'COMPANION_CORESCOPE_SOURCE', 'COMPANION_CORESCOPE_CONFIG'})
        self.assertEqual(value['revisions']['COMPANION_CORESCOPE_SOURCE'], 'a' * 64)
        self.assertFalse((self.root / 'injected').exists())

    def test_public_variant_and_restore_return_exact_canonical_core_metadata(self):
        canonical = self.load('corescope')['revisions']
        public = self.root / 'owned public config.json'
        public.write_text('{"publicProjection":true}')
        variant = self.load('corescope', public)['revisions']
        self.assertEqual(variant['COMPANION_CORESCOPE_SOURCE'], canonical['COMPANION_CORESCOPE_SOURCE'])
        self.assertEqual(variant['COMPANION_CORESCOPE_CONFIG'], hashlib.sha256(public.read_bytes()).hexdigest())
        self.assertNotEqual(variant['COMPANION_CORESCOPE_CONFIG'], canonical['COMPANION_CORESCOPE_CONFIG'])
        self.assertEqual(self.load('corescope')['revisions'], canonical)

    def test_missing_config_fails_before_consumer_with_or_without_errexit(self):
        missing, sentinel = self.root / 'missing config', self.root / 'consumer-ran'
        completed = subprocess.run(['bash', str(self.tools / 'revisions.sh'), '--component', 'corescope',
            '--core-config', str(missing), '--', 'touch', str(sentinel)],
            env=self.environment, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(completed.returncode, 0)
        self.assertFalse(sentinel.exists())
        completed = subprocess.run(['bash', '-c',
            'source "$1"; load_companion_revisions corescope "$2" && touch "$3"',
            'owned-loader', str(self.tools / 'revisions.sh'), str(missing), str(sentinel)],
            env=self.environment, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(completed.returncode, 0)
        self.assertFalse(sentinel.exists())


if __name__ == '__main__':
    unittest.main()
