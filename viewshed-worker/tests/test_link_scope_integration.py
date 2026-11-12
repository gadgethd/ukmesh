"""A rebuild job must commit to staging without changing the published graph."""

import os
import unittest
from unittest import mock
from urllib.parse import urlsplit
from uuid import uuid4
from types import SimpleNamespace
import link_model
import psycopg2
from psycopg2 import sql
import redis
import link_queue_v3
import link_jobs
from worker_context import WorkerContext
from worker_settings import parse_settings

URL = os.environ.get("TEST_LINK_DATABASE_URL")


@unittest.skipUnless(
    URL and os.environ.get("TEST_REDIS_URL"),
    "owned PostgreSQL and Redis link fixtures are required",
)
class LinkScopeIntegrationTests(unittest.TestCase):
    def test_generation_job_writes_only_staging_and_has_no_live_notifications(self):
        if urlsplit(URL).hostname not in ("127.0.0.1", "localhost"):
            raise RuntimeError("link tests require an owned loopback database")
        database = "untangle_link_" + uuid4().hex
        generation = "link_rebuild_" + uuid4().hex[:16]
        admin = psycopg2.connect(URL)
        admin.autocommit = True
        conn = queue = None
        try:
            with admin.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
                )
            conn = psycopg2.connect(URL, dbname=database)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("""CREATE SCHEMA core; CREATE SCHEMA ops;
                  CREATE TABLE core.nodes(node_id text PRIMARY KEY, lat float, lon float, elevation_m float, name text, role int);
                  CREATE TABLE core.node_coverage(node_id text PRIMARY KEY, radius_m float, antenna_height_m float);
                  CREATE TABLE core.node_links(node_a_id text,node_b_id text,observed_count int,last_observed timestamptz,
                    count_a_to_b int,count_b_to_a int,multibyte_observed_count int,itm_computed_at timestamptz,
                    itm_path_loss_db float,itm_viable boolean,force_viable boolean DEFAULT false,terrain_profile_json jsonb,
                    PRIMARY KEY(node_a_id,node_b_id));
                  CREATE TABLE ops.link_job_commits(job_id text PRIMARY KEY,job_type text,generation text,logical_job_id text,payload_hash text);""")
                a, b = "aa" * 32, "bb" * 32
                cur.execute(
                    "INSERT INTO core.nodes VALUES (%s,51.5,-1.2,100,'A',2),(%s,51.6,-1.3,110,'B',2)",
                    (a, b),
                )
                cur.execute(
                    "INSERT INTO core.node_links VALUES (%s,%s,7,now(),7,0,7,now(),100,true,false,NULL)",
                    (a, b),
                )
                cur.execute(
                    sql.SQL(
                        "CREATE SCHEMA {}; CREATE TABLE {}.node_links (LIKE core.node_links INCLUDING ALL)"
                    ).format(sql.Identifier(generation), sql.Identifier(generation))
                )
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {}.node_links SELECT * FROM core.node_links"
                    ).format(sql.Identifier(generation))
                )
            context = WorkerContext(
                settings=parse_settings(
                    {
                        "REDIS_URL": os.environ["TEST_REDIS_URL"],
                        "REDIS_PASSWORD": os.environ.get("TEST_REDIS_PASSWORD", ""),
                    }
                )
            )
            queue = redis.Redis.from_url(
                os.environ["TEST_REDIS_URL"],
                password=os.environ.get("TEST_REDIS_PASSWORD") or None,
                decode_responses=True,
            )
            queue.flushdb()
            job = {
                "version": 3,
                "type": "observe",
                "job_id": "scope-test",
                "dedupe_key": "scope-test",
                "logical_job_id": "logical-scope",
                "generation": generation,
                "rx_node_id": b,
                "src_node_id": a,
                "packet_hash": "fixture",
                "path_hashes": [a[:4]],
                "path_hash_size_bytes": 2,
            }
            self.assertEqual(
                link_queue_v3.admit(queue, job), ("accepted", "scope-test")
            )
            claimed = link_queue_v3.claim(queue)
            with mock.patch.object(queue, "publish") as publish:
                link_jobs.process_claimed_link_job(
                    conn, queue, claimed, context=context
                )
                publish.assert_not_called()
            with conn.cursor() as cur:
                cur.execute("SELECT observed_count FROM core.node_links")
                self.assertEqual(cur.fetchone()[0], 7)
                cur.execute(
                    sql.SQL("SELECT observed_count FROM {}.node_links").format(
                        sql.Identifier(generation)
                    )
                )
                self.assertEqual(cur.fetchone()[0], 8)
                cur.execute(
                    "SELECT generation FROM ops.link_job_commits WHERE job_id=%s",
                    ("scope-test",),
                )
                self.assertEqual(cur.fetchone()[0], generation)
            self.assertEqual(queue.hget(link_queue_v3.STATES, "scope-test"), "complete")
            # Exercise the missing-pair SELECT/INSERT/UPDATE SQL against the
            # same staged capability. Terrain acquisition is replaced at its
            # external boundary; graph persistence remains real PostgreSQL.
            c = "cc" * 32
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO core.nodes VALUES (%s,51.7,-1.4,120,'C',2)", (c,)
                )
            physical = {
                "version": 3,
                "type": "physical_pair",
                "job_id": "physical-scope",
                "dedupe_key": "physical-scope",
                "generation": generation,
                "node_a_id": a,
                "node_b_id": c,
            }
            self.assertEqual(link_queue_v3.admit(queue, physical)[0], "accepted")
            with (
                mock.patch.object(link_model, "ensure_tiles_for_link"),
                mock.patch.object(
                    link_model, "build_link_vrt", return_value="fixture.vrt"
                ),
                mock.patch.object(
                    link_model,
                    "compute_path_loss",
                    return_value=SimpleNamespace(
                        path_loss_db=90.0, viable=True, profile=()
                    ),
                ),
                mock.patch.object(queue, "publish") as publish,
            ):
                link_jobs.process_claimed_link_job(
                    conn, queue, link_queue_v3.claim(queue), context=context
                )
                publish.assert_not_called()
            with conn.cursor() as cur:
                cur.execute("SELECT count(*),sum(observed_count) FROM core.node_links")
                self.assertEqual(cur.fetchone(), (1, 7))
                cur.execute(
                    sql.SQL(
                        "SELECT itm_path_loss_db,itm_viable FROM {}.node_links WHERE node_a_id=%s AND node_b_id=%s"
                    ).format(sql.Identifier(generation)),
                    (a, c),
                )
                self.assertEqual(cur.fetchone(), (90.0, True))
            self.assertEqual(
                queue.hget(link_queue_v3.STATES, "physical-scope"), "complete"
            )
        finally:
            if queue is not None:
                queue.close()
            if conn is not None:
                conn.close()
            with admin.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(database)
                    )
                )
            admin.close()
