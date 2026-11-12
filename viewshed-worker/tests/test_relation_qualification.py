"""Regression guard for external consumers of moved UKMesh relations.

The link worker, Discord bot and CoreScope are separate deployment units.  A
green backend suite does not prove that their SQL still resolves after a table
move, so scan their runtime sources before deploying a move.

The checker lives in this tree (not ``scripts/``) because the worker image
builds from ``viewshed-worker/`` and runs this suite inside the container; see
``test_checker_shipping.py`` for the guard that keeps it there.
"""
import os
import sys
import unittest
from pathlib import Path

WORKER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_ROOT))

from check_external_consumer_sql import scan_roots, scan_text  # noqa: E402


class RelationQualificationTest(unittest.TestCase):
    def test_link_worker_sources_are_qualified(self):
        violations = scan_roots([WORKER_ROOT])
        self.assertEqual(violations, [], f"unqualified moved relations: {violations}")

    def test_scanner_rejects_short_names_and_accepts_schema_qualified_sql(self):
        bad = "SELECT * FROM nodes JOIN packets p ON true"
        good = "SELECT * FROM core.nodes JOIN mesh.packets p ON true"
        self.assertEqual(
            [relation for _, _, relation in scan_text(bad)],
            ["nodes", "packets"],
        )
        self.assertEqual(scan_text(good), [])

    def test_optional_external_roots(self):
        roots = [Path(part) for part in os.environ.get("EXTERNAL_CONSUMER_ROOTS", "").split(os.pathsep) if part]
        if not roots:
            self.skipTest("EXTERNAL_CONSUMER_ROOTS is not configured")
        self.assertEqual(scan_roots(roots), [])


if __name__ == "__main__":
    unittest.main()
