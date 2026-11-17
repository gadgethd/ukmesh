"""Only a component's own source and build recipe should invalidate its binary."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
import yaml

spec = importlib.util.spec_from_file_location('source_hash', Path(__file__).with_name('source-hash.py'))
source_hash = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source_hash)


class BuildInputs(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tools = self.root / 'scripts/companion-dev'
        self.tools.mkdir(parents=True)
        for name in ('build.sh', 'build-corescope.sh', 'build-beacon.sh', 'web-build.sh'):
            (self.tools / name).write_text(name)
        self.configuration = {'services': {'build': {'image': 'go:v1', 'environment': {'GOFLAGS': '-p=2'}},
                                          'web-build': {'image': 'node:v1', 'environment': {'VITE_API_BASE': '/api/v1'}},
                                          'beacon': {'ports': ['3103:8080']}}}
        self.write_configuration()
        for name in ('corescope', 'beacon-server', 'beacon-web'):
            checkout = self.root / 'third_party' / name
            checkout.mkdir(parents=True)
            (checkout / '.gitignore').write_text('node_modules/\n')
            (checkout / 'source.txt').write_text(name)
            self.git(checkout, 'init', '-q')
            self.git(checkout, 'add', '.')
            self.git(checkout, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'fixture')

    def git(self, checkout, *arguments):
        return subprocess.check_output(['git', '-C', str(checkout), *arguments], stderr=subprocess.DEVNULL)

    def write_configuration(self):
        (self.tools / 'compose.yml').write_text(yaml.safe_dump(self.configuration))

    def values(self):
        return {mode: source_hash.fingerprint(self.root, mode) for mode in source_hash.PROJECTS}

    def test_independent_source_changes_include_new_and_deleted_files(self):
        before = self.values()
        checkout = self.root / 'third_party/beacon-server'
        (checkout / 'new.go').write_text('package beacon')
        changed = self.values()
        self.assertNotEqual(changed['beacon'], before['beacon'])
        self.assertEqual(changed['corescope'], before['corescope'])
        self.assertEqual(changed['web'], before['web'])
        (checkout / 'new.go').unlink()
        (checkout / 'source.txt').unlink()
        self.assertNotEqual(self.values()['beacon'], before['beacon'])

    def test_dependency_cache_and_runtime_configuration_do_not_rebuild_sources(self):
        before = self.values()
        cache = self.root / 'third_party/beacon-web/node_modules'
        cache.mkdir()
        (cache / 'downloaded.js').write_text('dependency cache')
        self.configuration['services']['beacon']['ports'] = ['9999:8080']
        self.write_configuration()
        self.assertEqual(self.values(), before)

    def test_compiler_and_frontend_build_settings_invalidate_only_their_consumers(self):
        before = self.values()
        self.configuration['services']['build']['image'] = 'go:v2'
        self.write_configuration()
        changed = self.values()
        self.assertNotEqual(changed['beacon'], before['beacon'])
        self.assertNotEqual(changed['corescope'], before['corescope'])
        self.assertEqual(changed['web'], before['web'])
        self.configuration['services']['web-build']['environment']['VITE_API_BASE'] = '/custom-api'
        self.write_configuration()
        self.assertNotEqual(self.values()['web'], before['web'])

    def test_changed_component_builder_is_an_input(self):
        before = self.values()
        (self.tools / 'build-corescope.sh').write_text('changed compiler flags')
        changed = self.values()
        self.assertNotEqual(changed['corescope'], before['corescope'])
        self.assertEqual(changed['beacon'], before['beacon'])
        self.assertEqual(changed['web'], before['web'])

    def test_symlinked_files_and_directories_cannot_import_untracked_external_inputs(self):
        for name, target in [('outside-file', self.tools / 'build.sh'), ('outside-dir', self.tools)]:
            with self.subTest(name=name):
                link = self.root / 'third_party/beacon-server' / name
                link.symlink_to(target)
                try:
                    with self.assertRaisesRegex(ValueError, 'outside'):
                        source_hash.fingerprint(self.root, 'beacon')
                finally:
                    link.unlink()


if __name__ == '__main__':
    unittest.main()
