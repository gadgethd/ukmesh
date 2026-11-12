"""Import-safe immutable tagger configuration."""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class TaggerSettings:
    api_url: str
    api_key: str
    database_url: str
    poll_s: float
    call_floor_s: float
    batch_limit: int
    overlap_s: int
    max_retry_attempts: int
    retry_batch: int
    model: str
    start: str
    networks: tuple[str, ...]

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        return cls(
            api_url=env.get("TYPESAFE_API_URL", "https://api.typesafe.ai/v1/systemone"),
            api_key=env.get("TYPESAFE_API_KEY", "").strip(),
            database_url=env["DATABASE_URL"],
            poll_s=float(env.get("TAGGER_POLL_S", "5")),
            call_floor_s=float(env.get("TAGGER_CALL_FLOOR_S", "0.5")),
            batch_limit=int(env.get("TAGGER_BATCH_LIMIT", "200")),
            overlap_s=int(env.get("TAGGER_OVERLAP_S", "120")),
            max_retry_attempts=int(env.get("TAGGER_MAX_RETRY_ATTEMPTS", "30")),
            retry_batch=int(env.get("TAGGER_RETRY_BATCH", "20")),
            model=env.get("TYPESAFE_MODEL", "jev-latest"),
            start=env.get("TAGGER_START", "").strip(),
            networks=tuple(
                [
                    n.strip()
                    for n in env.get(
                        "TAGGER_NETWORKS", "ukmesh,northeast,teesside"
                    ).split(",")
                    if n.strip()
                ]
            ),
        )
