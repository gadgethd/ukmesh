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


class InvalidJob(ValueError):
    pass


class Cancelled(RuntimeError):
    pass


class JobDeadlineExceeded(RetryableTerrainError):
    pass
