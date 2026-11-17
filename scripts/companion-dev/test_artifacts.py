"""A failed or interrupted build must preserve the last published site."""
import importlib.util
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('artifacts', Path(__file__).with_name('artifacts.py'))
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


class StaticPublication(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state = Path(self.temporary.name)
        artifacts.prepare_web(self.state)
        (self.state / 'beacon-web.next/index.html').write_text('last good site')
        artifacts.publish_web(self.state, 'a' * 64)

    def stage(self):
        artifacts.prepare_web(self.state)
        (self.state / 'beacon-web.next/index.html').write_text('complete new site')

    def test_revision_moves_with_complete_site(self):
        self.stage()
        artifacts.publish_web(self.state, 'b' * 64)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'complete new site')
        self.assertEqual(artifacts.web_revision(self.state), 'b' * 64)
        self.assertFalse((self.state / 'beacon-web.previous').exists())

    def test_identical_complete_publication_preserves_live_directory_and_files(self):
        active = self.state / 'beacon-web'
        original = {path.name: path.stat().st_ino for path in [active, *active.iterdir()]}
        artifacts.prepare_web(self.state)
        (self.state / 'beacon-web.next/index.html').write_text('last good site')
        artifacts.publish_web(self.state, 'a' * 64)
        self.assertEqual({path.name: path.stat().st_ino for path in [active, *active.iterdir()]}, original)
        self.assertFalse((self.state / 'beacon-web.next').exists())
        self.assertFalse((self.state / 'beacon-web.previous').exists())
        self.assert_public_modes(active)

    def test_same_source_changed_output_changes_published_identity(self):
        before = artifacts.web_publication(self.state)
        self.stage()
        artifacts.publish_web(self.state, 'a' * 64)
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)
        self.assertNotEqual(artifacts.web_publication(self.state), before)

    def test_publication_identity_tracks_paths_and_bytes_not_creation_order_or_modes(self):
        active = self.state / 'beacon-web'
        (active / 'assets').mkdir()
        for name in ('a.js', 'z.js'):
            (active / 'assets' / name).write_text(name)
        before = artifacts.web_publication(self.state)
        inode = active.stat().st_ino
        artifacts.prepare_web(self.state)
        staged = self.state / 'beacon-web.next'
        (staged / 'assets').mkdir()
        for name in ('z.js', 'a.js'):
            (staged / 'assets' / name).write_text(name)
        (staged / 'index.html').write_text('last good site')
        artifacts.publish_web(self.state, 'a' * 64)
        self.assertEqual(artifacts.web_publication(self.state), before)
        self.assertEqual(active.stat().st_ino, inode)
        (active / 'assets/a.js').chmod(0o600)
        self.assertEqual(artifacts.web_publication(self.state), before)
        (active / 'assets/a.js').rename(active / 'assets/renamed.js')
        renamed = artifacts.web_publication(self.state)
        self.assertNotEqual(renamed, before)
        (active / 'assets/renamed.js').write_text('changed bytes')
        self.assertNotEqual(artifacts.web_publication(self.state), renamed)

    def test_legacy_cache_migrates_once_without_fabricating_a_source_revision(self):
        active = self.state / 'beacon-web'
        (active / '.ukmesh-source-revision').unlink()
        (self.state / 'web-source').write_text('a' * 64)
        legacy = artifacts.web_publication(self.state)
        self.assertRegex(legacy, '^[a-f0-9]{64}$')
        self.assertEqual(artifacts.web_publication(self.state), legacy)
        artifacts.prepare_web(self.state)
        (self.state / 'beacon-web.next/index.html').write_text('last good site')
        artifacts.publish_web(self.state, 'a' * 64)
        self.assertNotEqual(artifacts.web_publication(self.state), legacy)
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)
        inode = active.stat().st_ino
        artifacts.prepare_web(self.state)
        (self.state / 'beacon-web.next/index.html').write_text('last good site')
        artifacts.publish_web(self.state, 'a' * 64)
        self.assertEqual(active.stat().st_ino, inode)

    def test_failed_build_does_not_replace_site_or_revision(self):
        artifacts.prepare_web(self.state)
        (self.state / 'beacon-web.next/partial.js').write_text('incomplete output')
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            artifacts.publish_web(self.state, 'b' * 64)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)
        artifacts.prepare_web(self.state)
        self.assertFalse((self.state / 'beacon-web.next/partial.js').exists())

    def test_failed_publication_restores_last_good_site(self):
        self.stage()
        rename = artifacts.os.replace

        def fail_new_site(source, destination):
            if source == self.state / 'beacon-web.next':
                raise OSError('simulated interrupted publication')
            return rename(source, destination)

        with patch.object(artifacts.os, 'replace', side_effect=fail_new_site):
            with self.assertRaisesRegex(OSError, 'interrupted'):
                artifacts.publish_web(self.state, 'b' * 64)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)

    def test_next_invocation_recovers_process_death_between_renames(self):
        self.stage()
        (self.state / 'beacon-web').rename(self.state / 'beacon-web.previous')
        (self.state / 'beacon-web.previous').chmod(0o700)
        for path in (self.state / 'beacon-web.previous').iterdir():
            path.chmod(0o600)
        artifacts.recover_web(self.state)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)
        self.assert_public_modes(self.state / 'beacon-web')

    def test_committed_site_wins_if_death_prevented_backup_cleanup(self):
        self.stage()
        (self.state / 'beacon-web.next/.ukmesh-source-revision').write_text('b' * 64)
        (self.state / 'beacon-web').rename(self.state / 'beacon-web.previous')
        (self.state / 'beacon-web.next').rename(self.state / 'beacon-web')
        artifacts.recover_web(self.state)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'complete new site')
        self.assertEqual(artifacts.web_revision(self.state), 'b' * 64)
        self.assert_public_modes(self.state / 'beacon-web')

    def test_artifact_directory_cannot_escape_through_symlink(self):
        (self.state / 'beacon-web.next').symlink_to(self.state / 'beacon-web', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symbolic'):
            artifacts.prepare_web(self.state)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')

    def assert_public_modes(self, directory):
        for path in [directory, *directory.rglob('*')]:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o755 if path.is_dir() else 0o644, str(path))

    def test_private_build_umask_publishes_readable_assets_without_opening_state(self):
        self.state.chmod(0o700)
        private = self.state / 'private-state'
        private.write_text('private')
        private.chmod(0o600)
        previous = os.umask(0o077)
        try:
            self.stage()
            assets = self.state / 'beacon-web.next/assets/nested'
            assets.mkdir(parents=True)
            (assets / 'app.js').write_text('complete script')
            artifacts.publish_web(self.state, 'b' * 64)
        finally:
            os.umask(previous)
        self.assert_public_modes(self.state / 'beacon-web')
        self.assertEqual(stat.S_IMODE(self.state.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(private.stat().st_mode), 0o600)
        self.assertEqual(artifacts.web_revision(self.state), 'b' * 64)

    def test_recovery_repairs_restrictive_cached_artifact_without_rebuilding(self):
        active = self.state / 'beacon-web'
        active.chmod(0o700)
        for path in active.iterdir():
            path.chmod(0o600)
        artifacts.recover_web(self.state)
        self.assert_public_modes(active)
        self.assertEqual((active / 'index.html').read_text(), 'last good site')
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)

    def test_failed_permission_preparation_keeps_last_site_and_revision(self):
        self.stage()
        asset = self.state / 'beacon-web.next/app.js'
        asset.write_text('complete script')
        chmod = Path.chmod

        def fail_asset(path, mode, **options):
            if path == asset:
                raise OSError('simulated permission failure')
            return chmod(path, mode, **options)

        with patch.object(Path, 'chmod', autospec=True, side_effect=fail_asset):
            with self.assertRaisesRegex(OSError, 'permission failure'):
                artifacts.publish_web(self.state, 'b' * 64)
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
        self.assertFalse((self.state / 'beacon-web.previous').exists())

    def test_symlinked_static_entries_never_touch_external_files_or_replace_site(self):
        external = self.state / 'private-external'
        external.mkdir(mode=0o700)
        target = external / 'secret'
        target.write_text('private original')
        target.chmod(0o600)
        for name in ('index.html', '.ukmesh-source-revision', 'app.js', 'assets'):
            with self.subTest(name=name):
                self.stage()
                link = self.state / 'beacon-web.next' / name
                link.unlink(missing_ok=True)
                link.symlink_to(external if name == 'assets' else target, target_is_directory=name == 'assets')
                with self.assertRaisesRegex(ValueError, 'symbolic'):
                    artifacts.publish_web(self.state, 'b' * 64)
                self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)
                self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
                self.assertEqual(target.read_text(), 'private original')
                self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(external.stat().st_mode), 0o700)


if __name__ == '__main__':
    unittest.main()
