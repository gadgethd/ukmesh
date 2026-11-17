"""Pure capability, target, environment and redaction behavior; no services."""
import copy
import json
import subprocess
import unittest
from unittest.mock import patch

import common
import node_invocation as owner


MAIN = "lean_1x_fixture"
OWNER = "lean_owner_fixture"
SECRET = "test-only-do-not-log"


def inputs():
    fixture = {"ready": True, "kind": "1x", "databases": [
        {"label": "ukmesh", "database": MAIN, "container": "ukmesh-sample-db"},
        {"label": "owner", "database": OWNER, "container": "ukmesh-sample-db"},
        {"label": "beacon", "database": "lean_beacon_fixture", "container": "beacon-db"}]}
    env = {"DATABASE_URL": f"postgresql://meshcore_api:{SECRET}@ukmesh-sample-db:5432/{MAIN}",
        "MESHCORE_HTTP_DATABASE_URL": f"postgresql://meshcore_api:{SECRET}@ukmesh-sample-db:5432/{MAIN}",
        "MESHCORE_WORKER_DATABASE_URL": f"postgresql://meshcore_worker:{SECRET}@ukmesh-sample-db:5432/{MAIN}",
        "MESHCORE_INGEST_DATABASE_URL": f"postgresql://meshcore_ingest:{SECRET}@ukmesh-sample-db:5432/{MAIN}",
        "OWNER_DATABASE_URL": f"postgresql://meshcore_owner:{SECRET}@ukmesh-sample-db:5432/{OWNER}?sslmode=disable",
        "DATABASE_POOL_MAX": "8", "OWNER_DATABASE_POOL_MAX": "2", "DATABASE_ROLE_ENFORCEMENT": "true",
        "DATABASE_STATEMENT_TIMEOUT_MS": "30000", "UNRELATED_SECRET": SECRET}
    backend = {"Id": "backend-id", "Image": "backend-image", "Name": "/ukmesh-full-backend-1",
        "Config": {"Env": [k + "=" + v for k, v in env.items()], "Labels": {
            "com.docker.compose.project": "ukmesh-full", "com.docker.compose.service": "backend"}},
        "State": {"Running": True, "Status": "running", "Pid": 123, "StartedAt": "2026-11-16T01:00:00Z",
                  "Health": {"Status": "healthy"}}}
    db = {"Name": "/ukmesh-sample-db", "State": {"Running": True},
        "Config": {"Labels": {"com.docker.compose.project": "ukmesh-sample"}},
        "NetworkSettings": {"Ports": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "5433"}]}}}
    return fixture, {"ukmesh-full-backend-1": backend, "ukmesh-sample-db": db}


def setting(containers, key, value):
    env = containers["ukmesh-full-backend-1"]["Config"]["Env"]
    env[:] = [v for v in env if not v.startswith(key + "=")]
    if value is not None:
        env.append(key + "=" + value)


class ApplicationBoundary(unittest.TestCase):
    def test_exact_tool_categories_and_unknown_refusal(self):
        self.assertEqual(len(owner.READ_TOOLS), 3)
        self.assertEqual(len(owner.ADMIN_TOOLS), 8)
        for tool in owner.READ_TOOLS:
            self.assertEqual(owner.tool_category(tool), "application-read")
        for tool in owner.ADMIN_TOOLS:
            self.assertEqual(owner.tool_category(tool), "administrative-fixture-oracle")
        with self.assertRaises(ValueError): owner.tool_category("unclassified")

    def test_distinct_actual_roles_databases_and_pool_limits_are_preserved(self):
        fixture, containers = inputs()
        before = copy.deepcopy(containers)
        env, proof = owner.tool_environment("benchmarkLookups", MAIN, fixture, containers=containers)
        self.assertIn("meshcore_api:", env["MESHCORE_HTTP_DATABASE_URL"])
        self.assertIn("meshcore_worker:", env["MESHCORE_WORKER_DATABASE_URL"])
        self.assertIn("meshcore_ingest:", env["MESHCORE_INGEST_DATABASE_URL"])
        self.assertIn("meshcore_owner:", env["OWNER_DATABASE_URL"])
        self.assertIn("/" + OWNER + "?sslmode=disable", env["OWNER_DATABASE_URL"])
        self.assertTrue(all("@127.0.0.1:5433/" in env[k] for k in owner.URL_KEYS))
        self.assertEqual((env["DATABASE_POOL_MAX"], env["OWNER_DATABASE_POOL_MAX"]), ("8", "2"))
        self.assertEqual(env["DATABASE_ROLE_ENFORCEMENT"], "true")
        self.assertNotIn("UNRELATED_SECRET", env)
        self.assertNotIn(SECRET, json.dumps(proof))
        self.assertEqual(containers, before)

    def test_all_read_benchmarks_get_application_scope(self):
        fixture, containers = inputs()
        for tool in owner.READ_TOOLS:
            _, proof = owner.tool_environment(tool, MAIN, fixture, containers=containers)
            self.assertEqual(proof["category"], "application-read")

    def test_statistics_resource_settings_follow_read_consumers_without_leaking_into_admin_tools(self):
        fixture, containers = inputs()
        settings = {"DATABASE_ANALYTICS_RANDOM_PAGE_COST": "4",
                    "DATABASE_ANALYTICS_EFFECTIVE_CACHE_SIZE": "512MB",
                    "STATS_DB_QUERY_CONCURRENCY": "1"}
        for key, value in settings.items(): setting(containers, key, value)
        before = copy.deepcopy(containers)
        for tool in owner.READ_TOOLS:
            env, proof = owner.tool_environment(tool, MAIN, fixture, containers=containers)
            self.assertEqual({key: env[key] for key in settings}, settings)
            self.assertEqual({key: proof["pool_settings"][key] for key in settings}, settings)
            self.assertNotIn(SECRET, json.dumps(proof))
        for tool in owner.ADMIN_TOOLS:
            env, _ = owner.tool_environment(tool, MAIN, fixture,
                admin_endpoint=f"postgresql://meshcore:{SECRET}@127.0.0.1:5433/{MAIN}")
            self.assertTrue(set(settings).isdisjoint(env))
        self.assertEqual(containers, before)
        fixture, containers = inputs()
        env, _ = owner.tool_environment("benchmarkOperations", MAIN, fixture, containers=containers)
        self.assertTrue(set(settings).isdisjoint(env), 'absent consumer settings must remain absent')

    def test_read_capability_never_accepts_admin_fallback(self):
        fixture, containers = inputs()
        with self.assertRaises(ValueError):
            owner.tool_environment("benchmarkLookups", MAIN, fixture, containers=containers,
                admin_endpoint=f"postgresql://meshcore:{SECRET}@127.0.0.1:5433/{MAIN}")

    def test_admin_tools_keep_explicit_owned_admin_endpoint_and_redact_it(self):
        fixture, _ = inputs()
        endpoint = f"postgresql://meshcore:{SECRET}@127.0.0.1:5433/{MAIN}"
        for tool in owner.ADMIN_TOOLS:
            env, proof = owner.tool_environment(tool, MAIN, fixture, admin_endpoint=endpoint)
            self.assertEqual(env["DATABASE_URL"], endpoint)
            self.assertNotIn("MESHCORE_HTTP_DATABASE_URL", env)
            self.assertNotIn(SECRET, json.dumps(proof))

    def test_wrong_target_or_original_database_refused(self):
        fixture, containers = inputs()
        for database in ("meshcore", "lean_1x_foreign"):
            with self.assertRaises(ValueError):
                owner.tool_environment("benchmarkLookups", database, fixture, containers=containers)
        setting(containers, "OWNER_DATABASE_URL", f"postgresql://meshcore_owner:{SECRET}@ukmesh-sample-db:5432/{MAIN}")
        with self.assertRaises(ValueError): owner.read_environment(fixture, MAIN, containers)

    def test_missing_or_wrong_role_fails_without_mutating_input(self):
        for value in (None, f"postgresql://meshcore:{SECRET}@ukmesh-sample-db:5432/{MAIN}"):
            fixture, containers = inputs(); setting(containers, "MESHCORE_HTTP_DATABASE_URL", value)
            before = copy.deepcopy(containers)
            with self.assertRaises(ValueError) as caught: owner.read_environment(fixture, MAIN, containers)
            self.assertNotIn(SECRET, str(caught.exception))
            self.assertEqual(containers, before)

    def test_nonlocal_service_and_public_or_ambiguous_published_ports_refused(self):
        fixture, containers = inputs()
        setting(containers, "MESHCORE_WORKER_DATABASE_URL", f"postgresql://meshcore_worker:{SECRET}@remote.invalid:5432/{MAIN}")
        with self.assertRaises(ValueError): owner.read_environment(fixture, MAIN, containers)
        for ports in ([{"HostIp": "0.0.0.0", "HostPort": "5433"}],
                      [{"HostIp": "127.0.0.1", "HostPort": "5433"}] * 2):
            fixture, containers = inputs()
            containers["ukmesh-sample-db"]["NetworkSettings"]["Ports"]["5432/tcp"] = ports
            with self.assertRaises(ValueError): owner.read_environment(fixture, MAIN, containers)

    def test_enforcement_and_actual_pool_bounds_cannot_be_inferred(self):
        for key, value in (("DATABASE_ROLE_ENFORCEMENT", "false"), ("OWNER_DATABASE_POOL_MAX", None),
                           ("OWNER_DATABASE_POOL_MAX", "3"), ("DATABASE_POOL_MAX", "16")):
            fixture, containers = inputs(); setting(containers, key, value)
            with self.assertRaises(ValueError): owner.read_environment(fixture, MAIN, containers)


class StructuredChildEnvironment(unittest.TestCase):
    def test_secret_is_environment_only_and_inherited_lease_is_preserved(self):
        with patch.object(common, "child_context", return_value=({"UKMESH_TARGET_LEASE_FD": "77"}, (77,))), \
                patch.object(common.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            common.command(["node", "tool.ts"], environment_overrides={"DATABASE_URL": "postgresql://u:" + SECRET + "@localhost/db"})
        args, kwargs = run.call_args
        self.assertNotIn(SECRET, json.dumps(args))
        self.assertIn(SECRET, kwargs["env"]["DATABASE_URL"])
        self.assertEqual(kwargs["env"]["UKMESH_TARGET_LEASE_FD"], "77")
        self.assertEqual(kwargs["pass_fds"], (77,))

    def test_environment_cannot_spoof_inherited_lease(self):
        with patch.object(common.subprocess, "run") as run:
            with self.assertRaises(ValueError):
                common.command(["node", "tool.ts"], environment_overrides={"UKMESH_TARGET_LEASE_FD": "8"})
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
