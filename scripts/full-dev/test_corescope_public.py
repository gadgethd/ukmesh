"""Source selection keeps the revocable database and authority together."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import sqlite3
import shlex
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("core_public", Path(__file__).with_name("corescope-public.py"))
core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core)


class PublicProjectionConfiguration(unittest.TestCase):
    def test_compose_loads_core_only_published_metadata_for_each_actual_config(self):
        with tempfile.TemporaryDirectory(prefix='core metadata ') as temporary:
            root = Path(temporary)
            state = root / 'plans/lean-runtime'
            (state / 'runtime/fixtures/owned').mkdir(parents=True)
            fixture = {'token': 'owned', 'corescope_data_dir': str(state / 'runtime/fixtures/owned/corescope')}
            for public in (True, False):
                with self.subTest(public=public), patch.object(core, 'REPO', root), \
                     patch.object(core, 'STATE', state), patch.object(core, 'command') as command:
                    core.compose(fixture, public=public, args=['config', '--quiet'])
                    argv = command.call_args.args[0]
                    shell = shlex.split(argv[-1])
                    config = state / 'runtime/fixtures/owned/corescope-public/config.json' if public else root / 'scripts/companion-dev/corescope.json'
                    self.assertEqual(shell[:7], ['bash', str(root / 'scripts/companion-dev/revisions.sh'),
                        '--component', 'corescope', '--core-config', str(config), '--'])
                    self.assertEqual(shell[7:13], ['docker', 'compose', '--project-name', 'ukmesh-companions', '--env-file', '/dev/null'])
                    self.assertEqual(shell[-2:], ['config', '--quiet'])
                    self.assertEqual(argv[0], 'env')
                    self.assertIn('CORESCOPE_DATA_DIR=' + fixture['corescope_data_dir'], argv)
                    self.assertEqual(argv[-4:-1], ['sg', 'docker', '-c'])

    def test_ram_drain_includes_undelivered_entries(self):
        responses = [1, [{"name": "owned", "pending": 0, "lag": 2}],
                     1, [{"name": "owned", "pending": 0, "lag": 0}]]
        with patch.object(core, "redis", side_effect=responses) as redis, patch.object(core.time, "sleep"):
            core.drain_stream("owned-stream", "owned")
        self.assertEqual(redis.call_count, 4, "empty PEL must not hide an undelivered backlog")

    def test_partial_public_startup_restores_the_existing_pair(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            with patch.object(core, "corescope_application_context", return_value={}), \
                 patch.object(core, "switch", side_effect=[RuntimeError("ingestor failed to start"), {}]) as switch:
                with self.assertRaisesRegex(RuntimeError, "ingestor failed"):
                    core.qualify_pg(run)
            self.assertEqual([call.args for call in switch.call_args_list], [(True,), (False,)])
            result = core.read_json(run / "corescope-public-pg.json")
            self.assertFalse(result["passed"])
            self.assertTrue(result["restored_mqtt"])

    def test_both_processes_keep_authority_and_cursor_contract(self):
        base = {"mqttSources": [{"broker": "fixture"}], "channelKeys": {"#test": "public-key"},
                "map": {"tiles": {"darkDefault": "osm-dark"}}, "retention": {"packetDays": 30}}
        original = deepcopy(base)
        config = core.public_config(base, "lean_fixture_owned")
        self.assertEqual(base, original)
        self.assertEqual(config["channelKeys"], original["channelKeys"])
        self.assertEqual(config["map"], original["map"])
        self.assertEqual(config["mqttSources"], [])
        self.assertTrue(config["publicProjection"])
        self.assertTrue(config["publicAuthorityURL"].endswith("/public/policy"))
        self.assertEqual(config["publicAuthorityToken"], config["redisSources"][0]["publicToken"])
        self.assertEqual(config["redisSources"][0]["url"], "")
        for source in config["postgresSources"] + config["redisSources"]:
            self.assertTrue(source["statePath"].startswith("/data/"))
            self.assertNotIn("privacyDsn", source)
            self.assertNotIn("password", source)

    def test_measurement_database_cannot_be_reused_as_projection(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            fixture = {"token": "owned", "databases": [{"label": "ukmesh", "database": "lean_fixture_owned"}]}
            directory = state / "runtime/fixtures/owned/corescope-public"
            directory.mkdir(parents=True)
            with sqlite3.connect(directory / "analyzer.db") as connection:
                connection.execute("CREATE TABLE observations(id integer)")
                connection.execute("INSERT INTO observations VALUES (1)")
            with patch.object(core, "STATE", state):
                with self.assertRaisesRegex(ValueError, "non-revocable"):
                    core.prepare(fixture)
            with sqlite3.connect(directory / "analyzer.db") as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM observations").fetchone()[0], 1)

    def test_projection_path_cannot_escape_through_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary) / "state"
            parent = state / "runtime/fixtures/owned"
            parent.mkdir(parents=True)
            outside = Path(temporary) / "outside"
            outside.mkdir()
            (parent / "corescope-public").symlink_to(outside, target_is_directory=True)
            with patch.object(core, "STATE", state):
                with self.assertRaisesRegex(ValueError, "symlinks"):
                    core.projection_directory({"token": "owned"})


if __name__ == "__main__":
    unittest.main()
