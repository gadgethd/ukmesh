#!/usr/bin/env python3
"""Resolve the main Compose contract with owned values and no deployment .env."""
import json
import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parent.parent
WORKERS = (
    "path-learning-worker", "health-worker", "tagger-worker",
    "region-snapshot-worker", "privacy-remat-worker", "synthetic-monitor",
    "link-backfill-worker", "link-worker",
)


def resolved(**overrides):
    env = {
        key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ
    }
    env.update({
        "POSTGRES_PASSWORD": "owned-fixture-only",
        "MQTT_PASSWORD": "owned-fixture-only",
        "REDIS_PASSWORD": "owned-fixture-only",
        "JWT_SECRET": "owned-fixture-secret-at-least-32-characters",
        "OPERATOR_SITE_TOKEN": "owned-fixture-only",
        "ALERT_RECEIVER_TOKEN": "owned-fixture-only",
        "ANUBIS_ED25519_PRIVATE_KEY_HEX": "00" * 64,
        "GRAFANA_ADMIN_PASSWORD": "owned-fixture-only",
        "HEALTHCHECK_SOURCE_REF": "0" * 40,
        "CLOUDFLARE_TUNNEL_TOKEN": "",
    })
    env.update(overrides)
    result = subprocess.run([
        "docker", "compose", "--env-file", "/dev/null", "--profile", "dev",
        "-f", "docker-compose.yml", "-f", "docker-compose.ci.yml",
        "config", "--format", "json",
    ], cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)["services"]


class MainRuntimeContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.services = resolved()

    def test_bounded_pools_and_completed_backfill(self):
        self.assertEqual(self.services["backend"]["environment"]["DATABASE_POOL_MAX"], "8")
        self.assertEqual(self.services["backend"]["environment"]["OWNER_DATABASE_POOL_MAX"], "2")
        for name in WORKERS:
            self.assertEqual(self.services[name]["environment"]["DATABASE_POOL_MAX"], "2", name)
        self.assertEqual(self.services["link-backfill-worker"]["restart"], "no")

    def test_role_credentials_reach_each_execution_boundary(self):
        urls = {
            "MESHCORE_HTTP_DATABASE_URL": "postgresql://api@fixture/meshcore",
            "MESHCORE_INGEST_DATABASE_URL": "postgresql://ingest@fixture/meshcore",
            "MESHCORE_WORKER_DATABASE_URL": "postgresql://worker@fixture/meshcore",
            "MESHCORE_MIGRATE_DATABASE_URL": "postgresql://migrate@fixture/meshcore",
            "OWNER_DATABASE_URL": "postgresql://owner@fixture/owner",
            "POSTGRES_EXPORTER_DATABASE_URL": "postgresql://monitor@fixture/meshcore",
        }
        services = resolved(DATABASE_ROLE_ENFORCEMENT="true", **urls)
        worker_url = urls["MESHCORE_WORKER_DATABASE_URL"]
        for name in WORKERS:
            env = services[name]["environment"]
            for key in ("DATABASE_URL", "MESHCORE_HTTP_DATABASE_URL",
                        "MESHCORE_WORKER_DATABASE_URL", "MESHCORE_INGEST_DATABASE_URL"):
                self.assertEqual(env[key], worker_url, (name, key))
            self.assertEqual(env["DATABASE_ROLE_ENFORCEMENT"], "true", name)
        backend = services["backend"]["environment"]
        self.assertEqual(backend["DATABASE_URL"], urls["MESHCORE_HTTP_DATABASE_URL"])
        for key in ("MESHCORE_HTTP_DATABASE_URL", "MESHCORE_INGEST_DATABASE_URL",
                    "MESHCORE_WORKER_DATABASE_URL", "OWNER_DATABASE_URL"):
            self.assertEqual(backend[key], urls[key])
        migrate = services["db-migrate"]["environment"]
        self.assertEqual(migrate["DATABASE_URL"], urls["MESHCORE_MIGRATE_DATABASE_URL"])
        self.assertEqual(migrate["MESHCORE_MIGRATE_DATABASE_URL"], urls["MESHCORE_MIGRATE_DATABASE_URL"])
        self.assertEqual(services["postgres-exporter"]["environment"]["DATA_SOURCE_NAME"],
                         urls["POSTGRES_EXPORTER_DATABASE_URL"])
        for name in ("backend", *WORKERS, "postgres-exporter"):
            self.assertNotIn(urls["MESHCORE_MIGRATE_DATABASE_URL"],
                             services[name]["environment"].values(), name)

    def test_memory_overrides_move_both_ceiling_constraints(self):
        services = resolved(BACKEND_MEMORY_LIMIT="2g", HOPREACH_MEMORY_LIMIT="1536m",
                            DATABASE_POOL_MAX="12", WORKER_DATABASE_POOL_MAX="3")
        for name, count in (("backend", 2 * 1024**3), ("hopreach", 1536 * 1024**2)):
            self.assertEqual(int(services[name]["mem_limit"]), count)
            self.assertEqual(int(services[name]["memswap_limit"]), count)
        self.assertEqual(services["backend"]["environment"]["DATABASE_POOL_MAX"], "12")
        for name in WORKERS:
            self.assertEqual(services[name]["environment"]["DATABASE_POOL_MAX"], "3", name)

    def test_default_service_ceilings_and_external_sink_are_explicit(self):
        # Infrastructure is supplied by the existing CI overlay; its settings
        # deliberately differ from the deployed main application configuration.
        for name, service in self.services.items():
            if name in ("timescaledb", "mosquitto", "redis", "mosquitto-reloader",
                        "alert-forward-sink", "alert-receiver-bootstrap"):
                continue
            self.assertGreater(int(service["mem_limit"]), 0, name)
            self.assertEqual(int(service["mem_limit"]), int(service["memswap_limit"]), name)
        coordinator = self.services["hopreach"]
        self.assertEqual(coordinator["environment"]["GOMAXPROCS"], "1")
        self.assertEqual(float(coordinator["cpus"]), 1)
        self.assertEqual(int(coordinator["mem_limit"]), 512 * 1024**2)
        self.assertEqual(len(coordinator["ports"]), 1)
        ingress = coordinator["ports"][0]
        self.assertEqual((ingress["host_ip"], str(ingress["published"]), ingress["target"]),
                         ("127.0.0.1", "19101", 8083))
        services = resolved(TYPESAFE_API_URL="http://owned-sink:8080/classify")
        self.assertEqual(services["tagger-worker"]["environment"]["TYPESAFE_API_URL"],
                         "http://owned-sink:8080/classify")


if __name__ == "__main__":
    unittest.main()
