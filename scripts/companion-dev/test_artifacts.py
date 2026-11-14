"""A failed or interrupted build must preserve the last published site."""
import importlib.util
from pathlib import Path
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
        artifacts.recover_web(self.state)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')
        self.assertEqual(artifacts.web_revision(self.state), 'a' * 64)

    def test_committed_site_wins_if_death_prevented_backup_cleanup(self):
        self.stage()
        (self.state / 'beacon-web.next/.ukmesh-source-revision').write_text('b' * 64)
        (self.state / 'beacon-web').rename(self.state / 'beacon-web.previous')
        (self.state / 'beacon-web.next').rename(self.state / 'beacon-web')
        artifacts.recover_web(self.state)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'complete new site')
        self.assertEqual(artifacts.web_revision(self.state), 'b' * 64)

    def test_artifact_directory_cannot_escape_through_symlink(self):
        (self.state / 'beacon-web.next').symlink_to(self.state / 'beacon-web', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symbolic'):
            artifacts.prepare_web(self.state)
        self.assertEqual((self.state / 'beacon-web/index.html').read_text(), 'last good site')


if __name__ == '__main__':
    unittest.main()
