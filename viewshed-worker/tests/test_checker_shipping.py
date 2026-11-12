"""Guard: the external-consumer SQL checker must ship inside the worker image.

``test_relation_qualification.py`` imports the checker from the worker root, and
CI runs this suite inside the built image (``python -m unittest discover -s
/app/tests``).  When the checker lived only in the repo-level ``scripts/``
directory the container had no such path and the whole job died with
``ModuleNotFoundError: check_external_consumer_sql``.

This test can only pass when the module is present in the image's import path,
so deleting it from the ``COPY`` line in ``viewshed-worker/Dockerfile`` turns
into a named failure instead of a bare import error.
"""
import contextlib
import importlib
import io
import sys
import unittest
from pathlib import Path

WORKER_ROOT = Path(__file__).resolve().parents[1]
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))


class CheckerShippingTest(unittest.TestCase):
    def test_checker_is_importable_from_the_worker_root(self):
        try:
            checker = importlib.import_module("check_external_consumer_sql")
        except ModuleNotFoundError as error:  # pragma: no cover - the CI failure
            self.fail(
                f"check_external_consumer_sql is not importable from {WORKER_ROOT}: "
                f"{error}. The worker image builds from viewshed-worker/, so the "
                "module has to be COPYed there (see viewshed-worker/Dockerfile); "
                "a repo-level scripts/ directory does not exist in the container."
            )
        module_file = getattr(checker, "__file__", None)
        if module_file is None:  # pragma: no cover - builtins only
            self.fail("checker module has no __file__")
        self.assertEqual(
            Path(module_file).resolve().parent,
            WORKER_ROOT,
            "the checker must resolve from the worker tree, not a repo-level scripts/ dir",
        )

    def test_shipped_checker_cli_accepts_the_worker_tree(self):
        checker = importlib.import_module("check_external_consumer_sql")
        with contextlib.redirect_stdout(io.StringIO()):
            exit_code = checker.main(["--root", str(WORKER_ROOT)])
        self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
