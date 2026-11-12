"""Worker connections: explicit worker subsystem ownership."""

from __future__ import annotations
import time
import psycopg2
import redis
import logging

log = logging.getLogger("worker")


def redis_connection(*, context):
    return redis.Redis.from_url(
        context.settings.REDIS_URL,
        decode_responses=True,
        password=context.settings.REDIS_PASSWORD,
        socket_connect_timeout=5,
        # Must exceed the maximum five-second BLPOP plus scheduling/network slack.
        socket_timeout=30,
    )


def wait_for_db(*, context) -> psycopg2.extensions.connection:
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                context.settings.DATABASE_URL,
                application_name=context.settings.DB_APPLICATION_NAME,
            )
            # autocommit=True prevents SELECT queries from holding open transactions
            # that would block schema DDL (CREATE EXTENSION etc.) on app restart.
            conn.autocommit = True
            conn.cursor().execute("SELECT 1")
            return conn
        except Exception:
            log.info(f"Waiting for DB… (attempt {attempt + 1}/30)")
            time.sleep(3)
    raise RuntimeError("DB never became ready")
