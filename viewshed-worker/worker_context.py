"""Runtime ownership of immutable settings, refresh state and graph caches."""

from dataclasses import dataclass, field
from worker_settings import DEFAULT_SETTINGS, WorkerSettings
from rf_snapshots import RfRefreshState, UNLOADED_MAINLAND
from link_topology import TopologyCache


@dataclass
class WorkerContext:
    settings: WorkerSettings = DEFAULT_SETTINGS
    rf: RfRefreshState = field(default_factory=RfRefreshState)
    topology: object = None
    mainland: object = UNLOADED_MAINLAND

    def __post_init__(self):
        if self.topology is None:
            self.topology = TopologyCache(settings=self.settings)
