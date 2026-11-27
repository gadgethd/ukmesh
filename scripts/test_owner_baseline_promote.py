#!/usr/bin/env python3

from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest


PROMOTE = Path(__file__).with_name("lib") / "owner-baseline-promote.py"


class BaselinePromotionTest(unittest.TestCase):
    def run_promotion(self, candidate, previous):
        return subprocess.run([sys.executable, PROMOTE, candidate, previous], capture_output=True)

    def test_activation_preserves_prior_bytes_and_permissions(self):
        with tempfile.TemporaryDirectory() as work:
            live, candidate, previous = (Path(work) / name for name in ("current.json", "candidate.json", "prior.json"))
            live.write_text("old evidence\n")
            live.chmod(0o640)
            candidate.write_text("new evidence\n")
            result = self.run_promotion(candidate, previous)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(live.read_bytes(), candidate.read_bytes())
            self.assertEqual(stat.S_IMODE(live.stat().st_mode), 0o640)
            self.assertEqual(previous.read_text(), "old evidence\n")

    def test_existing_backup_or_symlink_prevents_activation(self):
        with tempfile.TemporaryDirectory() as work:
            live, candidate, previous = (Path(work) / name for name in ("current.json", "candidate.json", "prior.json"))
            live.write_text("old evidence\n")
            candidate.write_text("new evidence\n")
            previous.write_text("another rollback copy\n")
            self.assertNotEqual(self.run_promotion(candidate, previous).returncode, 0)
            self.assertEqual(live.read_text(), "old evidence\n")
            self.assertEqual(previous.read_text(), "another rollback copy\n")
            previous.unlink()
            live.unlink()
            live.symlink_to(candidate)
            self.assertNotEqual(self.run_promotion(candidate, previous).returncode, 0)
            self.assertTrue(live.is_symlink())
            self.assertFalse(previous.exists())


if __name__ == "__main__":
    unittest.main()
