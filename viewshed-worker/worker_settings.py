"""Immutable process settings; only the entrypoint selects the environment."""

from pathlib import Path
from dataclasses import dataclass


def env_flag_enabled(env, name: str, default: bool = True) -> bool:
    value = env.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() not in ("0", "false", "no", "off", "disabled")


@dataclass(frozen=True)
class WorkerSettings:
    SRTM_DIR: Path
    REDIS_URL: str
    REDIS_PASSWORD: str | None
    DATABASE_URL: str | None
    RADIO_BOT_URL: str
    WORKER_MODE: str
    VIEWSHED_ENABLED: bool
    COVERAGE_MODEL: str
    DB_APPLICATION_NAME: str
    JOB_QUEUE: str
    JOB_PENDING_SET: str
    JOB_PAYLOADS: str
    JOB_COUNTERS: str
    LINK_JOB_QUEUE: str
    LIVE_CHANNEL: str
    VIEWSHED_WORKER_HEARTBEAT: str
    PLANNED_RESULT_TTL_S: int
    PLANNED_COVERAGE_QUEUE_MAX: int
    COVERAGE_MODEL_VERSION: int
    COVERAGE_JOB_DEADLINE_SECONDS: int
    RF_PREFIX_ENDPOINT_BATCH: int
    RF_PREFIX_RAY_BATCH: int
    PREFIX_AMBIGUITY_RADIUS_KM: float
    MAX_RADIUS_M: float
    SIMPLIFY_DEG: float
    GEOMETRY_GRID_DEG: float
    N_RAYS: int
    STEP_M: float
    ANGLE_EPS: float
    RF_RADIAL_STEP_M: float
    RF_N_RAYS: int
    RF_RADIUS_MULTIPLIER: float
    RF_MIN_RADIUS_M: float
    RF_SOURCE_LINK_RADIUS_MULTIPLIER: float
    DEFAULT_PHYSICAL_LINK_RADIUS_KM: float
    MIN_PHYSICAL_LINK_RADIUS_KM: float
    MAX_PHYSICAL_LINK_RADIUS_KM: float
    SUPPORT_REFRESH_S: int
    LINK_TOPOLOGY_REFRESH_S: int
    SUPPORT_NEARBY_REPEATER_KM: int
    SUPPORT_PENALTY_PER_KM_DB: float
    SUPPORT_MAX_PENALTY_DB: float
    SUPPORT_PROJECTION_LAT: float
    RADIO_NEIGHBOR_REFRESH_S: int
    RADIO_NEIGHBOR_MAX_AGE_HOURS: int
    UK_LAT_MIN: int
    UK_LAT_MAX: int
    UK_LON_MIN: int
    UK_LON_MAX: int
    MAX_PLANNED_LINK_PEERS: int


def parse_settings(env):
    SRTM_DIR = Path(env.get("SRTM_DIR", "/data/srtm"))
    REDIS_URL = env.get("REDIS_URL", "redis://redis:6379")
    REDIS_PASSWORD = env.get("REDIS_PASSWORD") or None
    DATABASE_URL = env.get("DATABASE_URL")
    RADIO_BOT_URL = env.get("RADIO_BOT_URL", "").strip()
    WORKER_MODE = env.get("WORKER_MODE", "all").lower()
    VIEWSHED_ENABLED = env_flag_enabled(env, "VIEWSHED_ENABLED", False)
    COVERAGE_MODEL = env.get("COVERAGE_MODEL", "rf_radial_100m").lower()
    DB_APPLICATION_NAME = env.get(
        "DATABASE_APPLICATION_NAME",
        f"meshcore-{WORKER_MODE if WORKER_MODE in ('viewshed', 'link') else 'viewshed-worker'}",
    )
    JOB_QUEUE = "meshcore:viewshed_jobs"
    JOB_PENDING_SET = "meshcore:viewshed_pending"
    JOB_PAYLOADS = "meshcore:viewshed:payloads"
    JOB_COUNTERS = "meshcore:viewshed:counters"
    LINK_JOB_QUEUE = "meshcore:link_jobs"
    LIVE_CHANNEL = "meshcore:live"
    VIEWSHED_WORKER_HEARTBEAT = "meshcore:viewshed:worker_heartbeat"
    PLANNED_RESULT_TTL_S = max(
        3600,
        min(7 * 24 * 3600, int(env.get("PLANNED_COVERAGE_TTL_SECONDS", "86400"))),
    )
    PLANNED_COVERAGE_QUEUE_MAX = max(
        1,
        min(1000, int(env.get("PLANNED_COVERAGE_QUEUE_MAX", "100"))),
    )
    COVERAGE_MODEL_VERSION = int(
        env.get(
            "COVERAGE_MODEL_VERSION",
            "7" if COVERAGE_MODEL == "rf_radial_100m" else "2",
        )
    )
    COVERAGE_JOB_DEADLINE_SECONDS = max(
        60,
        min(3600, int(env.get("COVERAGE_JOB_DEADLINE_SECONDS", "600"))),
    )
    RF_PREFIX_ENDPOINT_BATCH = max(
        1,
        min(512, int(env.get("RF_PREFIX_ENDPOINT_BATCH", "64"))),
    )
    RF_PREFIX_RAY_BATCH = max(
        1,
        min(64, int(env.get("RF_PREFIX_RAY_BATCH", "8"))),
    )
    PREFIX_AMBIGUITY_RADIUS_KM = 45.0
    MAX_RADIUS_M = 100_000
    # A 0.001 degree simplification is approximately 100 m at UK latitudes.
    SIMPLIFY_DEG = 0.001
    # GEOS overlay tolerance: fixed precision at approximately 0.1 mm.
    GEOMETRY_GRID_DEG = 1e-9
    N_RAYS = 720
    STEP_M = 50.0
    ANGLE_EPS = 1e-9
    RF_RADIAL_STEP_M = 100.0
    RF_N_RAYS = 360
    RF_RADIUS_MULTIPLIER = 1.35
    RF_MIN_RADIUS_M = 20_000
    RF_SOURCE_LINK_RADIUS_MULTIPLIER = float(
        env.get("RF_SOURCE_LINK_RADIUS_MULTIPLIER", "1.25")
    )
    DEFAULT_PHYSICAL_LINK_RADIUS_KM = float(
        env.get("DEFAULT_PHYSICAL_LINK_RADIUS_KM", "60")
    )
    MIN_PHYSICAL_LINK_RADIUS_KM = float(env.get("MIN_PHYSICAL_LINK_RADIUS_KM", "20"))
    MAX_PHYSICAL_LINK_RADIUS_KM = float(env.get("MAX_PHYSICAL_LINK_RADIUS_KM", "100"))
    SUPPORT_REFRESH_S = int(env.get("COVERAGE_SUPPORT_REFRESH_S", "900"))
    LINK_TOPOLOGY_REFRESH_S = max(5, int(env.get("LINK_TOPOLOGY_REFRESH_S", "60")))
    SUPPORT_NEARBY_REPEATER_KM = float(
        env.get("COVERAGE_SUPPORT_NEARBY_REPEATER_KM", "12")
    )
    SUPPORT_PENALTY_PER_KM_DB = float(
        env.get("COVERAGE_SUPPORT_PENALTY_PER_KM_DB", "0.6")
    )
    SUPPORT_MAX_PENALTY_DB = float(env.get("COVERAGE_SUPPORT_MAX_PENALTY_DB", "14"))
    SUPPORT_PROJECTION_LAT = float(env.get("COVERAGE_SUPPORT_PROJECTION_LAT", "54.0"))
    RADIO_NEIGHBOR_REFRESH_S = int(env.get("RADIO_NEIGHBOR_REFRESH_S", "300"))
    RADIO_NEIGHBOR_MAX_AGE_HOURS = float(env.get("RADIO_NEIGHBOR_MAX_AGE_HOURS", "72"))
    UK_LAT_MIN = 49.5
    UK_LAT_MAX = 61.5
    UK_LON_MIN = -8.5
    UK_LON_MAX = 2.5
    MAX_PLANNED_LINK_PEERS = max(
        1,
        min(200, int(env.get("MAX_PLANNED_LINK_PEERS", "60"))),
    )
    return WorkerSettings(
        SRTM_DIR=SRTM_DIR,
        REDIS_URL=REDIS_URL,
        REDIS_PASSWORD=REDIS_PASSWORD,
        DATABASE_URL=DATABASE_URL,
        RADIO_BOT_URL=RADIO_BOT_URL,
        WORKER_MODE=WORKER_MODE,
        VIEWSHED_ENABLED=VIEWSHED_ENABLED,
        COVERAGE_MODEL=COVERAGE_MODEL,
        DB_APPLICATION_NAME=DB_APPLICATION_NAME,
        JOB_QUEUE=JOB_QUEUE,
        JOB_PENDING_SET=JOB_PENDING_SET,
        JOB_PAYLOADS=JOB_PAYLOADS,
        JOB_COUNTERS=JOB_COUNTERS,
        LINK_JOB_QUEUE=LINK_JOB_QUEUE,
        LIVE_CHANNEL=LIVE_CHANNEL,
        VIEWSHED_WORKER_HEARTBEAT=VIEWSHED_WORKER_HEARTBEAT,
        PLANNED_RESULT_TTL_S=PLANNED_RESULT_TTL_S,
        PLANNED_COVERAGE_QUEUE_MAX=PLANNED_COVERAGE_QUEUE_MAX,
        COVERAGE_MODEL_VERSION=COVERAGE_MODEL_VERSION,
        COVERAGE_JOB_DEADLINE_SECONDS=COVERAGE_JOB_DEADLINE_SECONDS,
        RF_PREFIX_ENDPOINT_BATCH=RF_PREFIX_ENDPOINT_BATCH,
        RF_PREFIX_RAY_BATCH=RF_PREFIX_RAY_BATCH,
        PREFIX_AMBIGUITY_RADIUS_KM=PREFIX_AMBIGUITY_RADIUS_KM,
        MAX_RADIUS_M=MAX_RADIUS_M,
        SIMPLIFY_DEG=SIMPLIFY_DEG,
        GEOMETRY_GRID_DEG=GEOMETRY_GRID_DEG,
        N_RAYS=N_RAYS,
        STEP_M=STEP_M,
        ANGLE_EPS=ANGLE_EPS,
        RF_RADIAL_STEP_M=RF_RADIAL_STEP_M,
        RF_N_RAYS=RF_N_RAYS,
        RF_RADIUS_MULTIPLIER=RF_RADIUS_MULTIPLIER,
        RF_MIN_RADIUS_M=RF_MIN_RADIUS_M,
        RF_SOURCE_LINK_RADIUS_MULTIPLIER=RF_SOURCE_LINK_RADIUS_MULTIPLIER,
        DEFAULT_PHYSICAL_LINK_RADIUS_KM=DEFAULT_PHYSICAL_LINK_RADIUS_KM,
        MIN_PHYSICAL_LINK_RADIUS_KM=MIN_PHYSICAL_LINK_RADIUS_KM,
        MAX_PHYSICAL_LINK_RADIUS_KM=MAX_PHYSICAL_LINK_RADIUS_KM,
        SUPPORT_REFRESH_S=SUPPORT_REFRESH_S,
        LINK_TOPOLOGY_REFRESH_S=LINK_TOPOLOGY_REFRESH_S,
        SUPPORT_NEARBY_REPEATER_KM=SUPPORT_NEARBY_REPEATER_KM,
        SUPPORT_PENALTY_PER_KM_DB=SUPPORT_PENALTY_PER_KM_DB,
        SUPPORT_MAX_PENALTY_DB=SUPPORT_MAX_PENALTY_DB,
        SUPPORT_PROJECTION_LAT=SUPPORT_PROJECTION_LAT,
        RADIO_NEIGHBOR_REFRESH_S=RADIO_NEIGHBOR_REFRESH_S,
        RADIO_NEIGHBOR_MAX_AGE_HOURS=RADIO_NEIGHBOR_MAX_AGE_HOURS,
        UK_LAT_MIN=UK_LAT_MIN,
        UK_LAT_MAX=UK_LAT_MAX,
        UK_LON_MIN=UK_LON_MIN,
        UK_LON_MAX=UK_LON_MAX,
        MAX_PLANNED_LINK_PEERS=MAX_PLANNED_LINK_PEERS,
    )


DEFAULT_SETTINGS = parse_settings({})
