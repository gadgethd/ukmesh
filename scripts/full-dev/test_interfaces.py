import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("interface_proof", Path(__file__).with_name("interfaces.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


class FailedRevalidation(unittest.TestCase):
    def test_timeout_replaces_old_success_with_failed_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            (run / "interfaces.json").write_text('{"passed":true}')
            with patch.object(proof, "phase_context", return_value={}), \
                 patch.object(proof, "command", side_effect=subprocess.TimeoutExpired(["owned-native-proof"], 600)):
                result = proof.interfaces(run)
            self.assertFalse(result["passed"])
            self.assertFalse(json.loads((run / "interfaces.json").read_text())["passed"])
            self.assertTrue(result["checks"])


if __name__ == "__main__":
    unittest.main()
