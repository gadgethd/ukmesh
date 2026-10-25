"""Regression tests for the guard, using Git-visible isolated YAML fixtures only."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("check-postgres-shm.py")
SPEC = importlib.util.spec_from_file_location("postgres_shm", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class PostgresShmTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".shm-test-", dir=SCRIPT.parent.parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)

    def write(self, path, content):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def compose(self, size="1g", image="postgres:17", extra=""):
        shm = f"    shm_size: {size}\n" if size is not None else ""
        self.write("compose.yml", f"services:\n  db:\n    image: {image}\n{shm}{extra}")

    def cli(self):
        result = subprocess.run(
            ["python3", str(SCRIPT), "--root", str(self.root), "--json"],
            capture_output=True, text=True,
        )
        return result.returncode, json.loads(result.stdout)

    def test_missing_or_inadequate_size_fails_cli(self):
        for size in (None, "64m", "512m", "1073741823", "null", "true", "0", "-1g",
                     "${PG_SHM:-1g}", "!reset 1g", "nonsense"):
            with self.subTest(size=size):
                self.compose(size)
                code, report = self.cli()
                self.assertEqual(code, 1)
                self.assertIn("requires explicit literal shm_size >= 1g", report["errors"][0])

    def test_adequate_docker_units_and_byte_counts_pass(self):
        for size in ("1g", "1024M", "1048576k", "1073741824", "'1GB'", "2g"):
            with self.subTest(size=size):
                self.compose(size)
                code, report = self.cli()
                self.assertEqual(code, 0)
                self.assertEqual(report["inventory"][0]["status"], "safe")

    def test_postgres_variants_profiles_and_new_nested_files_are_checked(self):
        self.compose()
        for index, image in enumerate(("timescale/timescaledb@sha256:fixture",
                                       "timescale/timescaledb-ha:pg17", "postgis/postgis:17",
                                       "bitnami/postgresql:17", "pgvector/pgvector:pg17",
                                       "${DB_IMAGE:-postgres:17}")):
            self.write(f"stacks/{index}/anything.yaml", f"services:\n  other:\n"
                       f"    image: {image}\n    profiles: [optional]\n")
        report = CHECK.audit(self.root)
        self.assertEqual(len(report["errors"]), 6)

    def test_yaml_anchors_and_exporter_client(self):
        self.write("compose.yml", """x-db: &db
  image: postgres:17
  shm_size: 1g
services:
  database:
    <<: *db
  postgres-exporter:
    image: quay.io/prometheuscommunity/postgres-exporter:latest
    environment:
      DATA_SOURCE_NAME: postgresql://db/app
  backend:
    image: app:local
    environment:
      DATABASE_URL: postgresql://db/app
""")
        report = CHECK.audit(self.root)
        self.assertEqual(report["errors"], [])
        self.assertEqual(sum(row["postgres"] for row in report["inventory"]), 1)

    def test_overlay_cannot_lower_or_reset_size(self):
        self.compose()
        for size in ("64m", "null", "!reset null", "${SHM_SIZE}"):
            with self.subTest(size=size):
                self.write("compose.override.yml", f"services:\n  db:\n    shm_size: {size}\n")
                self.assertEqual(len(CHECK.audit(self.root)["errors"]), 1)
        self.write("compose.override.yml", "services:\n  db:\n    profiles: [infra-owned]\n")
        report = CHECK.audit(self.root)
        self.assertEqual(report["errors"], [])
        self.assertIn("unknown", [row["status"] for row in report["inventory"]])

    def test_custom_server_command_environment_and_marker(self):
        self.compose()
        self.write("custom.yml", """services:
  command:
    image: company/database:local
    command: [postgres, -c, max_parallel_workers_per_gather=0]
  environment:
    image: company/database:local
    environment: [PGDATA=/data]
  marked:
    image: company/database:local
    x-postgres: true
""")
        self.assertEqual(len(CHECK.audit(self.root)["errors"]), 3)

    def test_ipc_and_dev_shm_mounts_cannot_bypass_size(self):
        for extra in ("    ipc: host\n", "    ipc: service:other\n", "    ipc: none\n",
                      "    volumes: ['/host/shm:/dev/shm']\n",
                      "    volumes: [{type: tmpfs, target: /dev/shm}]\n",
                      "    tmpfs: ['/dev/shm:size=64m']\n"):
            with self.subTest(extra=extra):
                self.compose(extra=extra)
                self.assertTrue(CHECK.audit(self.root)["errors"])

    def test_actions_service_options_are_enforced(self):
        for options, expected in (("--shm-size=1g", 0), ("--shm-size 1024m", 0),
                                  ("--health-interval 5s", 1), ("--shm-size=64m", 1),
                                  ("--shm-size=1g --ipc=host", 1),
                                  ("--shm-size=1g --tmpfs=/dev/shm:size=64m", 1)):
            with self.subTest(options=options):
                self.write(".github/workflows/test.yml", f"""jobs:
  tests:
    services:
      pg:
        image: timescale/timescaledb:pg17
        options: {options}
""")
                self.assertEqual(len(CHECK.audit(self.root)["errors"]), expected)

    def test_invalid_yaml_and_unsupported_inheritance_fail_closed(self):
        self.compose()
        for content in ("services: [broken", "include: [elsewhere.yml]",
                        "services: {db: {extends: other}}"):
            self.write("other.yml", content)
            result = subprocess.run(["python3", str(SCRIPT), "--root", str(self.root)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("ERROR", result.stderr)

    def test_empty_inventory_is_an_error(self):
        self.assertIn("no PostgreSQL", CHECK.audit(self.root)["errors"][0])


if __name__ == "__main__":
    unittest.main()
