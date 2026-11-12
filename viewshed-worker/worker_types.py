"""Worker types: explicit worker subsystem ownership."""

from __future__ import annotations
from dataclasses import dataclass, field
from rf.terrain import RetryableTerrainError


@dataclass(frozen=True)
class Computed:
    geom: dict
    strength_geoms: dict[str, dict]
    radius_m: float
    elevation_m: float
    stage_seconds: dict[str, float] = field(default_factory=dict)
    peak_rss_mb: float = 0.0


@dataclass(frozen=True)
class LinkUpdate:
    node_a_id: str
    node_b_id: str
    observed_count: int
    itm_path_loss_db: float | None
    itm_viable: bool | None
    count_a_to_b: int
    count_b_to_a: int
    multibyte_observed_count: int


class InvalidJob(ValueError):
    pass


class Cancelled(RuntimeError):
    pass


class JobDeadlineExceeded(RetryableTerrainError):
    pass
