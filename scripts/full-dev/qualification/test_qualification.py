"""Safety and acceptance boundaries; all mutations use temporary test files."""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

import access
import backups
import benchmarks
import common
import fixtures
import health
import probes
import report
import resources
import targets
import inventory_schema
import read_transport
import recovery
import soak
import core_projection
import source_identity
import context
import operations
from unittest.mock import Mock


MIB = resources.MIB


def resource_rows(*, seconds=2400, interval=5) -> list[dict]:
    return [{"host": {"monotonic": index, "mem_available_bytes": 800 * MIB,
                       "swap_used_bytes": 1000 * MIB, "swap_in_bytes_total": 0,
                       "swap_out_bytes_total": 0, "disk_free_bytes": 20 * 1024 ** 3},
             "runtime_working_bytes": 2000 * MIB,
             "databases": [{"name": "ukmesh", "connections": 50, "max_connections": 100,
                            "fsync": "on", "synchronous_commit": "on", "deadlocks": 0}],
             "containers": [{"name": "api", "id": "same-container", "restarts": 0,
                              "oom_kills": 0, "oom_killed": False}]}
            for index in range(0, seconds + 1, interval)]


class OriginalProtection(unittest.TestCase):
    def test_sqlite_online_backup_includes_committed_uncheckpointed_wal(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            original = directory / "original.sqlite"
            with sqlite3.connect(original) as writer:
                writer.execute("PRAGMA journal_mode=WAL")
                writer.execute("CREATE TABLE observations(id integer primary key)")
                writer.execute("INSERT INTO observations VALUES(42)")
                writer.commit()
                result = backups.sqlite_backup(directory, original)
                self.assertTrue(result["verified"])
                with sqlite3.connect(directory / "corescope.sqlite") as reader:
                    self.assertEqual(reader.execute("SELECT id FROM observations").fetchall(), [(42,)])
                self.assertEqual(writer.execute("SELECT id FROM observations").fetchall(), [(42,)])
            self.assertEqual((directory / "corescope.sqlite").stat().st_mode & 0o777, 0o600)

    def test_backup_receipt_rejects_corrupt_or_escaped_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            archive = directory / "archive.dump"
            archive.write_bytes(b"archive")
            store = {"archive": archive.name, "sha256": common.digest(archive), "verified": True}
            common.write_json(directory / "backups.json", {"passed": True, "stores": [store] * 4})
            fixtures.verify_backups(directory)
            archive.write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "checksum"):
                fixtures.verify_backups(directory)

    def test_failed_database_creation_never_drops_an_existing_database(self):
        calls = []
        def fake_docker(args, **kwargs):
            calls.append(args)
            if "inspect" in args:
                return subprocess.CompletedProcess(args, 0, b"container-id", b"")
            if "pg_dump" in args:
                kwargs["output_stream"].write(b"archive")
                return subprocess.CompletedProcess(args, 0, b"", b"")
            if "createdb" in args:
                raise subprocess.CalledProcessError(1, args)
            raise AssertionError("unexpected mutation: " + str(args))
        with tempfile.TemporaryDirectory() as temporary, patch.object(backups, "docker", fake_docker):
            with self.assertRaises(subprocess.CalledProcessError):
                backups.postgres_backup(Path(temporary), "ukmesh", "local", "original", "admin")
        self.assertFalse(any("dropdb" in call for call in calls))

    def test_database_name_prefix_and_parent_traversal_do_not_authorize_fixture(self):
        for name in ("../outside", "../../meshcore", "lean_/etc", "original;DROP DATABASE meshcore"):
            with self.assertRaises(ValueError):
                fixtures.bundle(name)

    def test_process_environment_discards_inherited_application_settings(self):
        with patch.dict(os.environ, {"DATABASE_URL": "postgres://private.invalid/real", "UNTRUSTED_SETTING": "bad"}):
            result = common.command(["python3", "-c", "import os; print(os.getenv('DATABASE_URL')); print(os.getenv('UNTRUSTED_SETTING'))"])
        self.assertEqual(result.stdout.decode(), "None\nNone\n")

    def test_literal_target_paths_support_spaces_and_restore_is_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="fixture with spaces ") as temporary:
            directory = Path(temporary)
            with patch.object(fixtures, "STATE", directory), patch.object(fixtures, "TARGETS", directory / "runtime/targets.env"), \
                    patch.object(fixtures, "SQLITE", directory / "original store/analyzer.db"), \
                    patch.object(fixtures, "bundle") as validate:
                fixtures.restore_originals()
                fixtures.restore_originals()
                self.assertEqual(fixtures.current_targets()["ukmesh"], "meshcore")
                self.assertIn(str(directory / "original store"), (directory / "runtime/targets.env").read_text())
                validate.assert_not_called()

    def test_target_settings_cannot_inject_another_line(self):
        with self.assertRaisesRegex(ValueError, "single-line"):
            fixtures.write_targets({"UKMESH_DATABASE": "lean_fixture_1\nDATABASE_URL=bad",
                "OWNER_DATABASE": "owner", "BEACON_DATABASE": "beacon", "CORESCOPE_DATA_DIR": "/owned"})


class ActualTargetOwnership(unittest.TestCase):
    def fixture(self):
        stores = {"ukmesh": "lean_1x_owned", "owner": "lean_owner_owned", "beacon": "lean_beacon_owned"}
        rows = {name: {"Config": {"Env": []}, "Mounts": [{"Type": "bind", "Source": "/owned/core", "Destination": "/data"}]}
                for name in ("ukmesh-companions-corescope-1", "ukmesh-companions-corescope-ingestor-1")}
        rows["ukmesh-full-backend-1"] = {"Config": {"Env": [
            "DATABASE_URL=postgresql://meshcore_api:secret@ukmesh-sample-db:5432/lean_1x_owned",
            "MESHCORE_HTTP_DATABASE_URL=postgresql://meshcore_api:secret@ukmesh-sample-db:5432/lean_1x_owned",
            "MESHCORE_INGEST_DATABASE_URL=postgresql://meshcore_ingest:secret@ukmesh-sample-db:5432/lean_1x_owned",
            "MESHCORE_WORKER_DATABASE_URL=postgresql://meshcore_worker:secret@ukmesh-sample-db:5432/lean_1x_owned",
            "OWNER_DATABASE_URL=postgresql://meshcore_owner:secret@ukmesh-sample-db:5432/lean_owner_owned"]}, "Mounts": []}
        return stores, rows

    def test_selected_file_does_not_authorize_stale_runtime(self):
        stores, rows = self.fixture()
        self.assertEqual(len(targets.validate_consumers(rows, stores, Path('/owned/core'))), 5)
        for replacement in ("/meshcore", "/lean_owner_owned"):
            stale = copy.deepcopy(rows)
            stale["ukmesh-full-backend-1"]["Config"]["Env"][0] = stale["ukmesh-full-backend-1"]["Config"]["Env"][0].replace("/lean_1x_owned", replacement)
            with self.assertRaisesRegex(ValueError, "selected store"):
                targets.validate_consumers(stale, stores, Path('/owned/core'))

    def test_wrong_role_or_mixed_core_mounts_are_rejected(self):
        stores, rows = self.fixture()
        wrong = copy.deepcopy(rows)
        wrong["ukmesh-full-backend-1"]["Config"]["Env"][0] = wrong["ukmesh-full-backend-1"]["Config"]["Env"][0].replace('meshcore_api:', 'meshcore:')
        with self.assertRaisesRegex(ValueError, "session role"):
            targets.validate_consumers(wrong, stores, Path('/owned/core'))
        rows["ukmesh-companions-corescope-ingestor-1"]["Mounts"][0]["Source"] = '/original/core'
        with self.assertRaisesRegex(ValueError, "CoreScope store"):
            targets.validate_consumers(rows, stores, Path('/owned/core'))

    def test_driver_query_overrides_and_missing_sessions_are_rejected(self):
        stores,rows=self.fixture()
        for suffix in ('?host=outside','?user=meshcore','?dbname=meshcore','?options=-csearch_path=public','?sslmode=disable&user=meshcore'):
            wrong=copy.deepcopy(rows)
            wrong['ukmesh-full-backend-1']['Config']['Env'][0]+=suffix
            with self.assertRaisesRegex(ValueError,'selected store'):
                targets.validate_consumers(wrong,stores,Path('/owned/core'))
        rows['ukmesh-full-backend-1']['Config']['Env'].pop()
        with self.assertRaisesRegex(ValueError,'required database'):
            targets.validate_consumers(rows,stores,Path('/owned/core'))

    def test_exporter_fixture_name_is_normalized_after_actual_target_validation(self):
        item={'Name':'/exporter','Image':'image','Config':{'Env':[],'Cmd':[],'Entrypoint':[]},'Mounts':[],
              'HostConfig':{'Memory':1,'MemorySwap':1,'NanoCpus':1,'CpusetCpus':'','ShmSize':1}}
        def profile(database):
            item['Config']['Env']=['DATA_SOURCE_NAME=postgresql://meshcore_monitor:secret@ukmesh-sample-db:5432/'+database]
            with patch.object(context,'service_manifest',return_value={'services':[{'name':'exporter','container':'exporter'}]}),\
                    patch.object(context,'docker',return_value=subprocess.CompletedProcess([],0,json.dumps([item]).encode(),b'')),\
                    patch.object(targets,'runtime_targets') as validate:
                result=context.running_profile({'ukmesh':database,'owner':'owner','beacon':'beacon'})
                validate.assert_called_once()
                return result
        self.assertEqual(profile('lean_1x_owned'),profile('lean_10x_owned'))


class LookupSourceEvidence(unittest.TestCase):
    def test_all_measurements_require_the_sealed_baseline_and_current_candidate(self):
        expected={label:{'source_root':'/'+label,'sha256':label,'revision':'ref','files':2}
                  for label in ('baseline','candidate')}
        rows=[{'label':label,'measured_source':expected[label],
               'modes':[{'source_identity':expected[label],'source_stable':True}]} for label in expected]
        source_identity.validate_measurements(rows,expected)
        for mutation in ('archive','candidate','drift','missing'):
            changed=copy.deepcopy(rows)
            if mutation=='archive':changed[0]['measured_source']['sha256']='edited'
            elif mutation=='candidate':changed[1]['measured_source']['sha256']='stale'
            elif mutation=='drift':changed[0]['modes'][0]['source_stable']=False
            else:del changed[1]['measured_source']
            with self.assertRaises(ValueError,msg=mutation):source_identity.validate_measurements(changed,expected)

    def test_unverified_seal_is_rejected_before_spawning_source_loader(self):
        with patch.object(source_identity,'read_json',return_value={'git_verified':False}),\
                patch.object(source_identity,'identify') as loader:
            with self.assertRaisesRegex(ValueError,'Git-verified'):source_identity.expected_sources()
            loader.assert_not_called()

    def test_baseline_package_and_dependency_resolution_are_bound_to_git_and_current_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'plans/baseline/backend/src';source.mkdir(parents=True)
            installed=root/'backend/node_modules';installed.mkdir(parents=True)
            metadata={'package.json':b'{"type":"module"}', 'package-lock.json':b'{"lockfileVersion":3}'}
            for name,value in metadata.items(): (source.parent/name).write_bytes(value)
            (root/'backend/package-lock.json').write_bytes(metadata['package-lock.json'])
            link=source.parent/'node_modules';link.symlink_to(installed,target_is_directory=True)
            def git(args):
                self.assertEqual(args[:2],['git','show'])
                return subprocess.CompletedProcess(args,0,metadata[args[-1].split('/')[-1]],b'')
            with patch.object(source_identity,'REPO',root),patch.object(source_identity,'command',side_effect=git):
                source_identity.validate_baseline_dependencies(source,'a'*40)
                for name in metadata:
                    path=source.parent/name;path.write_bytes(b'edited')
                    with self.assertRaisesRegex(ValueError,'Git blob'):source_identity.validate_baseline_dependencies(source,'a'*40)
                    path.write_bytes(metadata[name])
                package=source.parent/'package.json';package.unlink();package.symlink_to(root/'backend/package-lock.json')
                with self.assertRaisesRegex(ValueError,'Git blob'):source_identity.validate_baseline_dependencies(source,'a'*40)
                package.unlink();package.write_bytes(metadata['package.json'])
                (root/'backend/package-lock.json').write_bytes(b'other dependency versions')
                with self.assertRaisesRegex(ValueError,'locks differ'):source_identity.validate_baseline_dependencies(source,'a'*40)
                (root/'backend/package-lock.json').write_bytes(metadata['package-lock.json'])
                link.unlink();foreign=root/'foreign-dependencies';foreign.mkdir();link.symlink_to(foreign,target_is_directory=True)
                with self.assertRaisesRegex(ValueError,'dependency link'):source_identity.validate_baseline_dependencies(source,'a'*40)


class HTTPReaderOwnership(unittest.TestCase):
    def test_empty_sampler_error_and_unready_timeout_stop_before_workload(self):
        ready=Mock();ready.wait.return_value=True
        with self.assertRaisesRegex(RuntimeError,'first complete sample'):
            soak.await_first_sample(ready,{'passed':False,'error':''})
        ready.wait.return_value=False
        with self.assertRaisesRegex(RuntimeError,'first complete sample'):soak.await_first_sample(ready,{})
        ready.wait.return_value=True
        soak.await_first_sample(ready,{})

    def backend(self,running=True):
        return {'Id':'owned-backend-id','Image':'immutable-image','State':{'Running':running},
                'Config':{'Labels':{'com.docker.compose.project':'ukmesh-full','com.docker.compose.service':'backend'}}}

    def test_failed_run_collision_never_removes_existing_container(self):
        calls=[]
        def fake(args,**kwargs):
            calls.append(args)
            if args[0]=='inspect':return subprocess.CompletedProcess(args,0,json.dumps([self.backend()]).encode(),b'')
            raise subprocess.CalledProcessError(1,args)
        with tempfile.TemporaryDirectory() as directory,patch.object(read_transport,'docker',fake):
            with self.assertRaises(subprocess.CalledProcessError):read_transport.start_reader(Path(directory))
        self.assertEqual([call[0] for call in calls],['inspect','run'])

    def test_wrong_cleanup_label_never_authorizes_deletion(self):
        item={'Name':'/ukmesh-owned-http-reader-owned','Config':{'Labels':{}}}
        with patch.object(read_transport,'docker',return_value=subprocess.CompletedProcess([],0,json.dumps([item]).encode(),b'')) as docker:
            with self.assertRaisesRegex(ValueError,'ownership marker'):read_transport.stop_reader('ukmesh-owned-http-reader-owned')
            self.assertEqual(docker.call_count,1)

    def test_stopped_backend_is_not_a_client_namespace(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(read_transport,'docker',return_value=
                subprocess.CompletedProcess([],0,json.dumps([self.backend(False)]).encode(),b'')) as docker:
            with self.assertRaisesRegex(ValueError,'running owned backend'):read_transport.start_reader(Path(directory))
            self.assertEqual(docker.call_count,1)

    def test_missing_alias_cannot_keep_a_proven_owned_helper_resident(self):
        name='ukmesh-owned-http-reader-alias-test'
        item={'Name':'/'+name,'Config':{'Labels':{'ukmesh.owned-http-reader':name,
              'ukmesh.http-reader.alias':'/not-owned','ukmesh.http-reader.directory':'/private-run'}}}
        def fake(args,**kwargs):
            return subprocess.CompletedProcess(args,0,json.dumps([item]).encode() if args[0]=='inspect' else b'',b'')
        with patch.object(read_transport,'docker',side_effect=fake) as docker:
            with self.assertRaisesRegex(ValueError,'reader was removed'):read_transport.stop_reader(name)
            self.assertEqual(docker.call_args_list[-1].args[0],['rm','-f',name])

    def test_reader_is_removed_when_companion_finish_fails_after_partial_start(self):
        worker=Mock();worker.is_alive.return_value=False
        health_worker=Mock();health_worker.is_alive.return_value=False
        companions=Mock();companions.finish.side_effect=RuntimeError('reader failed')
        stop=Mock()
        with patch.object(soak,'stop_reader') as remove:
            with self.assertRaisesRegex(RuntimeError,'reader failed'):
                soak.stop_measurements(worker,health_worker,companions,stop,'owned-reader')
            remove.assert_called_once_with('owned-reader');stop.set.assert_called_once()
        worker.join.assert_not_called();health_worker.join.assert_not_called()


class RecoverySafety(unittest.TestCase):
    def test_provider_control_rejects_unbounded_or_unexpected_faults(self):
        for settings in ({'delay_ms':2001},{'delay_ms':-1},{'status':500},{'delay_ms':'1000'},{'status':False},{'delay_ms':True},
                         {'status':503},{'status':503,'alert_probe':'a'*33},{'status':503,'alert_probe':'A'*32},
                         {'status':503,'alert_probe':'a'*32,'delay_ms':1},{'status':429,'alert_probe':'a'*32}):
            with patch.object(recovery,'urlopen') as network:
                with self.assertRaises(ValueError):recovery.provider(**settings)
                network.assert_not_called()

    def test_fault_control_scopes_tag_and_exact_owned_alert(self):
        with patch.object(recovery,'urlopen') as network:
            network.return_value.__enter__.return_value.status=204
            recovery.provider(status=429)
            self.assertEqual(json.loads(network.call_args.args[0].data),{'status':429,'delayMs':0,'route':'/tag'})
            recovery.provider(delay_ms=2000)
            self.assertEqual(json.loads(network.call_args.args[0].data),{'status':0,'delayMs':2000,'route':'/tag'})
            recovery.provider(status=503,alert_probe='a'*32)
            self.assertEqual(json.loads(network.call_args.args[0].data),
                {'status':503,'delayMs':0,'route':'/alerts','alertProbe':'a'*32})

    def test_unrelated_pending_alert_cannot_trigger_restart_before_owned_failed_forward(self):
        probe='a'*32
        retained={'dead_letter_count':1,'unresolved_dead_letter_count':0,'resolved_dead_letter_count':1}
        health=iter([retained,
            {**retained,'pending_count':1},{**retained,'pending_count':1},
            {**retained,'pending_count':1,'status':'ok'},
            {**retained,'pending_count':0,'status':'ok'}])
        counts=iter([{'failedAlertProbes':[]},{'failedAlertProbes':[probe]},
                     {'alertProbes':[]},{'alertProbes':[probe]}])
        failed_checks=[]
        def response(url):
            value=next(counts if url.endswith('/counts') else health)
            if 'failedAlertProbes' in value:failed_checks.append(value)
            return json.dumps(value).encode(),200
        def wait(check):
            for _ in range(5):
                if check():return {'passed':True,'elapsed_seconds':0}
            raise AssertionError('native predicate did not complete')
        def restart(args,**kwargs):
            self.assertEqual(args,['restart','ukmesh-full-alert-receiver-1'])
            self.assertEqual(len(failed_checks),2,'unrelated pending count must not trigger restart')
            self.assertEqual(failed_checks[-1]['failedAlertProbes'],[probe])
        with patch.object(recovery.uuid,'uuid4',return_value=Mock(hex=probe)), \
             patch.object(recovery,'provider') as provider,patch.object(recovery,'http_get',side_effect=response), \
             patch.object(recovery,'wait_for',side_effect=wait),patch.object(recovery,'docker',side_effect=restart), \
             patch.object(recovery,'urlopen') as network:
            network.return_value.__enter__.return_value.status=200
            result=recovery.alert_delivery()
            self.assertTrue(result['passed'])
            self.assertEqual(result['retained_dead_letter_history'],{'raw':1,'unresolved':0,'resolved':1})
            self.assertEqual(provider.call_args_list[0].kwargs,{'status':503,'alert_probe':probe})
            self.assertEqual(provider.call_count,3)

    def test_alert_fault_is_released_after_restart_failure_and_existing_dead_letters_refuse_fault(self):
        with patch.object(recovery,'http_get',return_value=(b'{"dead_letter_count":1}',200)), \
             patch.object(recovery,'provider') as provider,patch.object(recovery,'docker') as docker:
            with self.assertRaisesRegex(RuntimeError,'pre-existing unresolved dead letters'):recovery.alert_delivery()
            provider.assert_not_called();docker.assert_not_called()
        with patch.object(recovery,'http_get',return_value=(b'{"dead_letter_count":0}',200)), \
             patch.object(recovery,'provider') as provider,patch.object(recovery,'wait_for',return_value={'passed':True}), \
             patch.object(recovery,'urlopen') as network,patch.object(recovery,'docker',side_effect=RuntimeError('restart failed')):
            network.return_value.__enter__.return_value.status=200
            with self.assertRaisesRegex(RuntimeError,'restart failed'):recovery.alert_delivery()
            self.assertEqual(provider.call_count,2)
            self.assertEqual(provider.call_args.kwargs,{})

    def test_dead_letter_history_fallback_is_only_for_absent_fields_and_inconsistent_counts_fail(self):
        self.assertEqual(recovery.alert_history({'dead_letter_count':0}),{'raw':0,'unresolved':0,'resolved':0})
        self.assertEqual(recovery.alert_history({'dead_letter_count':1}),{'raw':1,'unresolved':1,'resolved':0})
        for health in (
            {'dead_letter_count':1,'unresolved_dead_letter_count':None,'resolved_dead_letter_count':1},
            {'dead_letter_count':1,'unresolved_dead_letter_count':0},
            {'dead_letter_count':1,'unresolved_dead_letter_count':0,'resolved_dead_letter_count':0},
            {'dead_letter_count':True}, {'dead_letter_count':-1},
        ):
            with self.assertRaisesRegex(ValueError,'inconsistent'):recovery.alert_history(health)

    def test_new_or_changed_dead_letter_history_blocks_owned_fault_recovery(self):
        probe='a'*32
        initial={'dead_letter_count':1,'unresolved_dead_letter_count':0,'resolved_dead_letter_count':1}
        for changed in (
            {'dead_letter_count':2,'unresolved_dead_letter_count':1,'resolved_dead_letter_count':1},
            {'dead_letter_count':2,'unresolved_dead_letter_count':0,'resolved_dead_letter_count':2},
        ):
            def response(url):
                if url.endswith('/counts'):return json.dumps({'failedAlertProbes':[probe]}).encode(),200
                return json.dumps({**changed,'pending_count':1}).encode(),200
            def wait(check):
                self.assertFalse(check(),'changed history must not qualify even with zero unresolved items')
                raise RuntimeError('history changed')
            with patch.object(recovery.uuid,'uuid4',return_value=Mock(hex=probe)), \
                 patch.object(recovery,'provider') as provider,patch.object(recovery,'http_get') as http, \
                 patch.object(recovery,'wait_for',side_effect=wait),patch.object(recovery,'docker') as docker, \
                 patch.object(recovery,'urlopen') as network:
                network.return_value.__enter__.return_value.status=200
                def first_then_changed(url):
                    http.side_effect=response
                    return json.dumps(initial).encode(),200
                http.side_effect=first_then_changed
                with self.assertRaisesRegex(RuntimeError,'history changed'):recovery.alert_delivery()
                docker.assert_not_called();self.assertEqual(provider.call_args.kwargs,{})

    def test_another_selected_fixture_stops_driver_before_process_creation(self):
        with patch.object(recovery,'active_fixture',return_value={'token':'other'}),patch.object(recovery,'command') as process:
            with self.assertRaisesRegex(ValueError,'fixture changed'):
                recovery.driver(Path('/unused'),{'token':'owned'},'publish')
            process.assert_not_called()


class CorePublicEvidence(unittest.TestCase):
    def test_native_receipt_requires_bound_application_exact_variants_and_no_skips(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);run=root/'run';run.mkdir()
            directory=root/'runtime/fixtures/owned/corescope-public';directory.mkdir(parents=True)
            base={'mqttSources':[{'name':'normal'}],'postgresSources':[],'redisSources':[],'publicProjection':False}
            public={**base,'mqttSources':[],'postgresSources':[{'dsn':'postgres://corescope_ro:secret@ukmesh-sample-db:5432/lean_fixture_owned?sslmode=disable'}],
                    'redisSources':[{'publicURL':'http://ukmesh-full-backend-1:3000/internal/ingest/public/entries'}],
                    'publicProjection':True,'publicAuthorityURL':'http://ukmesh-full-backend-1:3000/internal/ingest/public/policy'}
            common.write_json(root/'scripts/companion-dev/corescope.json',base);common.write_json(directory/'config.json',public)
            application={'proof_code_sha256':'frozen'}
            checks=[{'name':name,'passed':True} for name in core_projection.PG_NAMES]
            for row in checks:
                if row['name'] in ('public-postgresql-backfill-and-follow','public-restoration-replays-once'):
                    row.update(journal=2,observations=2,duplicateIdentities=0)
                elif row['name']=='privacy-revocation-prunes-copied-and-derived-data':row.update(journal=0,observations=0,duplicateIdentities=0)
                elif row['name']=='cursor-restart-and-observation-deduplication':row.update(lastSeqBefore=2,lastSeqAfter=2)
            value={'passed':True,'application':application,'fixture_token':'owned','checks':checks,'restored_mqtt':True,
                   'attempt':str(run/'attempt'),'variant':{'mode':'public-postgresql','fixture_token':'owned',
                    'projection_directory':str(directory),'core_changed_fields':sorted(k for k in base.keys()|public.keys() if base.get(k)!=public.get(k)),
                    'backend_changed_fields':[]}}
            fixture={'kind':'sample','databases':[{'label':'ukmesh','database':'lean_fixture_owned'}]}
            with patch.object(core_projection,'REPO',root),patch.object(core_projection,'STATE',root),patch.object(core_projection,'bundle',return_value=fixture):
                core_projection.validate_receipt(value,ram=False,application=application,run=run)
                for mutation in ('stale-code','missing-native','skipped','private-copy','outside-storage','undeclared-setting'):
                    changed=copy.deepcopy(value)
                    if mutation=='stale-code':changed['application']={'proof_code_sha256':'old'}
                    elif mutation=='missing-native':changed['checks'].pop()
                    elif mutation=='skipped':changed['checks'][0]['skipped']=True
                    elif mutation=='private-copy':next(row for row in changed['checks'] if row['name']=='privacy-revocation-prunes-copied-and-derived-data')['journal']=1
                    elif mutation=='outside-storage':changed['variant']['projection_directory']=str(root/'original')
                    else:changed['variant']['core_changed_fields'].append('privateSecret')
                    with self.assertRaises(ValueError,msg=mutation):core_projection.validate_receipt(changed,ram=False,application=application,run=run)


class ExecutableInventory(unittest.TestCase):
    def test_missing_new_duplicate_and_unproved_services_fail(self):
        manifest = common.service_manifest()
        self.assertTrue(inventory_schema.coverage(common.REPO, manifest)["passed"])
        missing = copy.deepcopy(manifest)
        removed = missing["services"].pop()
        self.assertIn(removed["name"], inventory_schema.coverage(common.REPO, missing)["missing"])
        unknown = copy.deepcopy(manifest)
        unknown["services"].append({**removed, "name": "new-unregistered-service", "container": "unique"})
        self.assertFalse(inventory_schema.coverage(common.REPO, unknown)["passed"])
        duplicate = copy.deepcopy(manifest)
        duplicate["services"].append(duplicate["services"][0])
        self.assertFalse(inventory_schema.coverage(common.REPO, duplicate)["passed"])
        unproved = copy.deepcopy(manifest)
        unproved["services"][0]["work"] = []
        self.assertFalse(inventory_schema.coverage(common.REPO, unproved)["passed"])

    def test_running_public_tunnel_fails_even_if_excluded_from_manifest(self):
        entry={'Name':'/owned','State':{'Running':True,'Status':'running','ExitCode':0,'OOMKilled':False},
               'RestartCount':0,'Config':{'Labels':{}},'Mounts':[]}
        tunnel=copy.deepcopy(entry)
        tunnel['Config']['Labels']={'com.docker.compose.project':'ukmesh-full','com.docker.compose.service':'cloudflared'}
        with tempfile.TemporaryDirectory() as directory,patch.object(probes,'service_manifest',return_value={
                'services':[{'name':'owned','container':'owned','kind':'container','required':True}]}),\
                patch.object(probes,'coverage',return_value={'passed':True}),\
                patch.object(probes,'inspected_containers',return_value={'owned':entry,'tunnel':tunnel}),\
                patch.object(targets,'runtime_targets',return_value=[]),patch.object(probes,'phase_context',return_value={}):
            result=probes.inventory(Path(directory))
            self.assertFalse(result['passed']);self.assertFalse(result['public_tunnel_off'])


class ResourceAcceptance(unittest.TestCase):
    def sample_workload(self,duration,finished_at):
        test_clock={'now':0.0};ready_at=[]
        base=resource_rows()[0]
        class WorkloadStop:
            def is_set(self):return test_clock['now']>=finished_at
            def wait(self,seconds):test_clock['now']+=seconds
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            class Ready(threading.Event):
                def set(self):
                    if not (directory/'resources.jsonl').read_text().strip():raise AssertionError('ready before a sample was persisted')
                    ready_at.append(test_clock['now']);super().set()
            def host():return dict(base['host'],monotonic=test_clock['now'])
            container=dict(base['containers'][0],working_bytes=2000*MIB)
            with patch.object(resources,'phase_context',return_value={}),patch.object(resources,'current_targets',return_value={}),\
                    patch.object(resources,'service_manifest',return_value={'services':[{'name':'api','required':True}]}),\
                    patch.object(resources.time,'monotonic',side_effect=lambda:test_clock['now']),\
                    patch.object(resources,'host_sample',side_effect=host),patch.object(resources,'container_samples',return_value=[container]),\
                    patch.object(resources,'native_pss',return_value={'total_bytes':0}),\
                    patch.object(resources,'database_samples',return_value=base['databases']):
                result=resources.sample(directory,seconds=duration,interval=.5,warmup=0,
                                        stop_event=WorkloadStop(),ready_event=Ready())
            return result,ready_at

    def test_first_sample_barrier_and_sampling_include_actual_workload_drain(self):
        result,ready=self.sample_workload(2,3)
        self.assertEqual(ready,[0]);self.assertEqual(result['duration_seconds'],3)
        self.assertTrue(result['passed'])

    def test_early_native_failure_never_waits_out_a_fake_successful_window(self):
        result,ready=self.sample_workload(10,.5)
        self.assertEqual(ready,[0]);self.assertEqual(result['duration_seconds'],.5)
        self.assertFalse(result['passed']);self.assertFalse(result['checks']['complete_duration'])

    def test_existing_swap_usage_is_not_new_swap_growth(self):
        result = resources.evaluate(resource_rows(), duration=2400, warmup=120)
        self.assertTrue(result["passed"])
        self.assertEqual(result["swap_growth_bytes"], 0)

    def test_heap_limits_cannot_substitute_for_total_runtime_memory(self):
        rows = resource_rows()
        rows[-1]["runtime_working_bytes"] = 3072 * MIB + 1
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["checks"]["runtime_memory"])

    def test_warmup_memory_spike_still_violates_runtime_cap(self):
        rows=resource_rows();rows[0]['runtime_working_bytes']=3072*MIB+1
        self.assertFalse(resources.evaluate(rows,duration=2400,warmup=120)['checks']['runtime_memory'])

    def test_more_than_five_percent_low_headroom_fails(self):
        rows = resource_rows()
        for row in rows[-30:]:
            row["host"]["mem_available_bytes"] = 383 * MIB
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["checks"]["headroom"])

    def test_sixty_seconds_sustained_swap_traffic_fails(self):
        rows = resource_rows(seconds=240, interval=5)
        for row in rows:
            row["host"]["swap_out_bytes_total"] = row["host"]["monotonic"] * 2 * MIB
        self.assertFalse(resources.evaluate(rows, duration=240, warmup=120)["checks"]["swap_churn"])

    def test_disappeared_or_recreated_container_is_not_stable(self):
        rows = resource_rows()
        rows[-1]["containers"] = []
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["checks"]["stable_containers"])
        rows = resource_rows()
        rows[-1]["containers"][0]["id"] = "replacement"
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["checks"]["stable_containers"])

    def test_short_window_or_no_postwarmup_samples_cannot_pass(self):
        self.assertFalse(resources.evaluate(resource_rows(seconds=60), duration=2400, warmup=0)["passed"])
        self.assertFalse(resources.evaluate(resource_rows(seconds=60), duration=60, warmup=60)["passed"])

    def test_empty_first_sample_and_late_required_service_fail_coverage(self):
        rows = resource_rows()
        rows[0]["containers"] = []
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120,
            required_containers={"api"})["checks"]["complete_resident_coverage"])
        for row in rows:
            row["containers"] = []
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["passed"])

    def test_connections_and_durability_cannot_be_traded_for_latency(self):
        rows = resource_rows()
        rows[-1]["databases"][0]["connections"] = 91
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["checks"]["database_connection_reserve"])
        rows[-1]["databases"][0]["synchronous_commit"] = "off"
        self.assertFalse(resources.evaluate(rows, duration=2400, warmup=120)["checks"]["database_durability"])


class EvidenceAcceptance(unittest.TestCase):
    def test_cold_route_failures_or_missing_warmup_cannot_disappear_from_soak_evidence(self):
        value={'passed':True,'settings':{'count':72000,'rate':30,'httpConcurrency':8},'drained':True,
               'wsPackets':72000,'logs_passed':True,'socketReadiness':{'initialState':True,'unexpectedCloses':0},
               'warmupReads':[{'status':200} for _ in range(8)]}
        self.assertFalse(report.phase_check('soak',value)[0])
        value['settings']['httpWarmup']=True
        self.assertTrue(report.phase_check('soak',value)[0])
        value['warmupReads'][0]['status']=503
        self.assertFalse(report.phase_check('soak',value)[0])

    def test_missing_phases_and_short_soak_fail_even_with_passed_flag(self):
        self.assertFalse(report.phase_check("soak", {"passed": True, "settings": {"count": 30, "rate": 30,
            "httpConcurrency": 8}, "drained": True, "wsPackets": 30})[0])
        self.assertFalse(report.phase_check("recovery", {"passed": True, "checks": []})[0])
        self.assertFalse(report.phase_check("tests", {"passed": True, "checks": [{"passed": True, "skipped": True}]})[0])
        with tempfile.TemporaryDirectory() as temporary, patch.object(report, "phase_context", return_value={}):
            result = report.report(Path(temporary))
            self.assertFalse(result["passed"])
            self.assertEqual(set(result["failed_phases"]), set(report.REQUIRED))

    def test_old_successful_evidence_cannot_qualify_current_source(self):
        current = {"run_id": "run", "profile_sha256": "profile", "source": {
            "root_content_sha256": "current", "submodules_sha256": "same"}}
        old = copy.deepcopy(current)
        old["source"]["root_content_sha256"] = "old"
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "run"
            common.write_json(directory / "inventory.json", {"passed": True,
                "services": [{"passed": True, "required": True}], "context": old})
            with patch.object(report, "phase_context", return_value=current):
                result = report.report(directory)
            entry = next(row for row in result["phases"] if row["phase"] == "inventory")
            self.assertFalse(entry["passed"])
            self.assertIn("source changed", entry["error"])

    def test_http_200_without_content_assertion_is_not_work_proof(self):
        with patch.object(probes, "http_get", return_value=(b"{}", "application/json")):
            with self.assertRaisesRegex(ValueError, "requires a JSON assertion"):
                probes.work_probe({"kind": "http", "url": "http://127.0.0.1:1234/"})

    def test_permission_errors_do_not_leak_password_or_sql(self):
        value = subprocess.CompletedProcess([], 1, b"", b"ERROR: 42501\nprivate payload")
        with patch.object(access, "docker", return_value=value):
            result = access.session("owned", "fixture", "role", "private SQL", host="172.18.0.2")
        self.assertEqual(result["sqlstate"], "42501")
        self.assertNotIn("private", json.dumps(result))

    def test_connection_authentication_failure_is_not_a_permission_denial(self):
        value = subprocess.CompletedProcess([], 2, b"", b'FATAL: password authentication failed for user "role"')
        with patch.object(access, "docker", return_value=value):
            result = access.session("owned", "fixture", "role", "SELECT 1", host="172.18.0.2")
        self.assertTrue(result["authentication_rejected"])
        self.assertFalse(result["database_access_denied"])
        self.assertIsNone(result["sqlstate"])

    def test_read_comparison_requires_both_scales_and_three_matched_trials(self):
        self.assertFalse(benchmarks.read_comparison([])["passed"])
        runs = []
        for scale in ("1x", "10x"):
            for label in ("baseline", "candidate"):
                modes = [{"concurrency": concurrency, "cache": cache,
                          "results": [{"name": "map", "p95Ms": 40} for _ in range(3)]}
                         for concurrency in (1, 8) for cache in ("miss", "warm")]
                runs.append({"scale": scale, "label": label, "modes": modes,
                    "companions": {name: {"p95_ms": 10, "requests": 100, "failures": 0}
                                   for name in ("corescope", "beacon")}})
        self.assertTrue(benchmarks.read_comparison(runs)["passed"])
        runs[-1]["modes"][-1]["results"].pop()
        self.assertFalse(benchmarks.read_comparison(runs)["passed"])


class OperationSourceEvidence(unittest.TestCase):
    def records(self):
        sources = {label: {"source_root": "/"+label, "revision": digit*40, "sha256": digit*64, "files": 20}
                   for label, digit in (("baseline", "a"), ("candidate", "b"))}
        runs = []
        for scale in ("1x", "10x"):
            for label in sources:
                pair='a'*32 if scale=='1x' else 'b'*32
                records = [{"name": name, "trial": trial, "passed": True, "status": 200,
                    "bytes": 100, "ms": 100, "digest": "c"*64, "dataDigest": "d"*64,
                    "entries": 2 if name.startswith("nodes-") else None,
                    "identityDigest": "e"*64 if name.startswith("nodes-") else None}
                    for trial in range(3) for name in sorted(operations.NAMES)]
                runs.append({"scale": scale, "label": label, "fixture_token": scale,
                    "passed": True, "measured_source": sources[label], "source_identity": sources[label],
                    'pair':{'token':pair,'label':label},'context_before':{},'context':{},'source_settings_fixture_stable':True,
                    "source_identity_after": sources[label], "source_stable": True,
                    "settings": {"trials": 3, "deadlineMs": 30000, "maxResponseBytes": 8*1024*1024,
                        "exportSpacingMs": 13000, "readOnly": True, "httpConcurrency": 1},
                    "chart_snapshot": {"generatedAt": "2026-11-14T00:00:00Z", "visibilityGeneration": 7, "maxAgeMs": 1800000},
                    "records": records, "identity_checks": [{"trial": trial, "passed": True} for trial in range(3)]})
                for item in records:
                    started=dt.datetime(2026,11,14,0,10,tzinfo=dt.timezone.utc)+dt.timedelta(seconds=item['trial']*40+{'summary':0,'charts':1,'nodes-csv':2,'nodes-geojson':16}[item['name']])
                    item.update(startedAt=started.isoformat(),finishedAt=(started+dt.timedelta(milliseconds=100)).isoformat(),
                        paired={'token':pair,'readyAt':(started-dt.timedelta(milliseconds=10)).isoformat(),
                                'releaseAt':(started-dt.timedelta(milliseconds=1)).isoformat()})
                    if item["name"] == "charts": item["snapshot"] = {**runs[-1]["chart_snapshot"], "status": "complete", "scope": "ukmesh", "ageMs": 1000}
        return runs, sources

    def test_operations_require_actual_baseline_bytes_complete_trials_identity_and_data_parity(self):
        runs, sources = self.records()
        self.assertTrue(operations.comparison(runs, sources)["passed"])
        for mutation in ("source", "drift", "incomplete", "duplicate", "identity", "data", "regression", "fixture",'pair','skew','release','context','spacing','snapshot'):
            changed = copy.deepcopy(runs)
            if mutation == "source": changed[0]["source_identity"] = sources["candidate"]
            elif mutation == "drift": changed[0]["source_stable"] = False
            elif mutation == "incomplete": changed[0]["records"].pop()
            elif mutation == "duplicate": changed[0]["records"][-1] = changed[0]["records"][0]
            elif mutation == "identity": changed[0]["records"][1]["identityDigest"] = "f"*64
            elif mutation == "data": changed[1]["records"][0]["dataDigest"] = "f"*64
            elif mutation == "regression":
                for item in changed[1]["records"]: item["ms"] = 111
            elif mutation=='fixture':changed[1]["fixture_token"] = "other"
            elif mutation=='pair':changed[1]['pair']['token']='c'*32
            elif mutation=='skew':changed[1]['records'][0]['startedAt']='2026-11-14T00:10:01.101+00:00'
            elif mutation=='release':changed[1]['records'][0]['paired']['releaseAt']='2026-11-14T00:10:00.998+00:00'
            elif mutation=='context':changed[1]['context']={'changed':True}
            elif mutation=='snapshot':changed[1]['chart_snapshot']['generatedAt']='2026-11-14T00:00:01Z'
            else:changed[1]['records'][2]['startedAt']='2026-11-14T00:10:03+00:00'
            self.assertFalse(operations.comparison(changed, sources)["passed"], mutation)

    def test_report_cannot_accept_two_current_source_runs_relabelled_as_baseline(self):
        runs, sources = self.records()
        current = {"run_id": "run", "profile_sha256": "profile", "source": {
            "root_content_sha256": "current", "submodules_sha256": "same"}}
        for row in runs: row["context"] = row['context_before'] = current
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)/"run"
            value = {"passed": True, "context": current, "runs": runs, **operations.comparison(runs, sources)}
            common.write_json(directory/"other-operations.json", value)
            with patch.object(report, "phase_context", return_value=current), patch.object(source_identity, "expected_sources", return_value=sources):
                result = report.report(directory)
                self.assertTrue(next(item for item in result["phases"] if item["phase"] == "other-operations")["passed"])
                value["runs"][0]["source_identity"] = sources["candidate"]
                common.write_json(directory/"other-operations.json", value)
                result = report.report(directory)
                self.assertFalse(next(item for item in result["phases"] if item["phase"] == "other-operations")["passed"])

    def test_matching_trials_allow_clock_changes_between_trials_without_dropping_exact_pair_parity(self):
        runs,sources=self.records()
        for row in runs:
            for item in row['records']:item['dataDigest']=f"{item['trial']+1:064x}"
        self.assertTrue(operations.comparison(runs,sources)['passed'])
        runs[1]['records'][0]['dataDigest']='f'*64
        self.assertFalse(operations.comparison(runs,sources)['passed'])

    def test_pair_start_skew_keeps_the_exact_hundred_millisecond_boundary(self):
        for skew,allowed in ((100,True),(101,False)):
            runs,sources=self.records()
            record=runs[1]['records'][0]
            record['startedAt']=(dt.datetime.fromisoformat(record['startedAt'])+dt.timedelta(milliseconds=skew)).isoformat()
            record['finishedAt']=(dt.datetime.fromisoformat(record['finishedAt'])+dt.timedelta(milliseconds=skew)).isoformat()
            self.assertEqual(operations.comparison(runs,sources)['passed'],allowed)

    def test_one_exited_process_aborts_its_waiting_peer_and_releases_shared_readers(self):
        _,sources=self.records()
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            fixture={'kind':'1x','token':'owned','databases':[{'label':'ukmesh','database':'lean_1x_owned'}]}
            peer_closed=threading.Event()
            def process(tool,args,database,log,**kwargs):
                self.assertEqual(tool,'benchmarkOperations');self.assertEqual(database,'lean_1x_owned')
                label=args[args.index('--pair-label')+1]
                if label=='candidate':return 1
                pair=Path(args[args.index('--pair-directory')+1])
                for _ in range(100):
                    if (pair/'abort.json').exists():peer_closed.set();return 1
                    peer_closed.wait(.01)
                raise AssertionError('peer was not aborted')
            companions=Mock();companions.finish.return_value={name:{'requests':1,'failures':0} for name in ('corescope','beacon')}
            with patch.object(operations,'active_fixture',return_value=fixture),patch.object(operations,'inventory',return_value={'passed':True}), \
                 patch.object(operations,'expected_sources',return_value=sources),patch.object(operations,'phase_context',return_value={'stable':True}), \
                 patch.object(operations,'CompanionReads',return_value=companions),patch.object(operations,'node_tool',side_effect=process):
                result=operations.operations_pair(directory)
                self.assertFalse(result['passed']);self.assertTrue(peer_closed.is_set())
                companions.start.assert_called_once();companions.finish.assert_called_once()
                self.assertTrue((Path(result['attempt'])/'summary.json').is_file())
                self.assertFalse(common.read_json(directory/'other-operations.json')['passed'])

    def test_controller_releases_only_exact_sources_same_snapshot_and_fresh_state(self):
        _,sources=self.records();token='a'*32
        for mutation in ('none','source','token','step','snapshot','expiry','short-ttl'):
            with tempfile.TemporaryDirectory() as temporary:
                directory=Path(temporary)
                generated=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=1)).isoformat()
                snapshot={'generatedAt':generated,'visibilityGeneration':7,'maxAgeMs':1800000}
                for trial in range(3):
                    for name in operations.ORDER:
                        for label in sources:
                            value={'token':token,'label':label,'trial':trial,'name':name,'source':sources[label],
                                   'snapshot':copy.deepcopy(snapshot),'readyAt':dt.datetime.now(dt.timezone.utc).isoformat()}
                            if trial==0 and name=='summary' and label=='candidate':
                                if mutation=='source':value['source']=sources['baseline']
                                elif mutation=='token':value['token']='b'*32
                                elif mutation=='step':value['name']='charts'
                                elif mutation=='snapshot':value['snapshot']['visibilityGeneration']=8
                                elif mutation=='expiry':value['snapshot']['maxAgeMs']=1
                                elif mutation=='short-ttl':value['snapshot']['maxAgeMs']=300000
                            common.write_json(directory/f'{trial}-{name}-{label}.ready.json',value)
                futures={label:Mock() for label in sources}
                for future in futures.values():future.done.return_value=False
                if mutation=='none':
                    releases=operations.release_pair(directory,token,sources,futures)
                    self.assertEqual(len(releases),12)
                    self.assertEqual(len(list(directory.glob('*.release.json'))),12)
                else:
                    with self.assertRaises(ValueError):operations.release_pair(directory,token,sources,futures)
                    self.assertFalse(list(directory.glob('*.release.json')))


class HealthSessionEvidence(unittest.TestCase):
    def test_current_session_receipts_prove_both_observers_without_code_in_report(self):
        session = {"sessionId": "owned-session", "fixtureToken": "owned", "runId": "run", "testStartedAt": 100}
        native = {"id": "owned-session", "matchedAt": 101, "observedCount": 2,
                  "receipts": [{}, {}], "useCount": 1, "code": "PRIVATE-TEST-CODE", "messageBody": "private"}
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            common.write_json(directory / "runtime/selected-fixture.json", {"token": "owned"})
            with patch.object(health, "STATE", directory), patch.object(health, "PROOF", directory / "proof.json"), \
                    patch.object(health, "http_get", return_value=(json.dumps(native).encode(), "application/json")):
                result = health.verify_session(session, timeout=0)
            self.assertTrue(result["passed"])
            self.assertNotIn("PRIVATE-TEST-CODE", json.dumps(result))
            self.assertNotIn("messageBody", result)

    def test_old_match_duplicate_use_and_another_fixture_are_failures(self):
        session = {"sessionId": "owned-session", "fixtureToken": "owned", "runId": "run", "testStartedAt": 100}
        base = {"id": "owned-session", "matchedAt": 101, "observedCount": 2, "receipts": [{}, {}], "useCount": 1}
        for field, value in (("matchedAt", 99), ("useCount", 2), ("id", "other-session")):
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                common.write_json(directory / "runtime/selected-fixture.json", {"token": "owned"})
                with patch.object(health, "STATE", directory), patch.object(health, "PROOF", directory / "proof.json"), \
                        patch.object(health, "http_get", return_value=(json.dumps(base | {field: value}).encode(), "application/json")):
                    self.assertFalse(health.verify_session(session, timeout=0)["passed"])


if __name__ == "__main__":
    unittest.main()
