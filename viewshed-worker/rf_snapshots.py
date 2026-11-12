"""Immutable RF model inputs, published together by the worker refresh owner."""

from dataclasses import dataclass, field
from types import MappingProxyType
from collections.abc import Mapping
from rf.config import DEFAULT_USABLE_PATH_LOSS_DB


@dataclass(frozen=True)
class CalibrationSnapshot:
    usable_path_loss_db: float = DEFAULT_USABLE_PATH_LOSS_DB
    signal_thresholds_db: Mapping = field(
        default_factory=lambda: MappingProxyType(
            {
                "green": max(116.0, DEFAULT_USABLE_PATH_LOSS_DB - 16.0),
                "amber": max(124.0, DEFAULT_USABLE_PATH_LOSS_DB - 8.0),
                "red": DEFAULT_USABLE_PATH_LOSS_DB,
            }
        )
    )
    samples: int = 0
    updated_at: float = 0.0
    source_signature: object = None

    def values(self):
        return {**vars(self), "signal_thresholds_db": dict(self.signal_thresholds_db)}


@dataclass(frozen=True)
class SupportSnapshot:
    coordinates: object = None
    node_ids: tuple = ()
    node_index_by_id: Mapping = field(default_factory=lambda: MappingProxyType({}))
    max_link_km_by_node: Mapping = field(default_factory=lambda: MappingProxyType({}))
    updated_at: float = 0.0
    source_signature: object = None


class RfRefreshState:
    def __init__(self):
        self.calibration = CalibrationSnapshot()
        self.support = SupportSnapshot()
        self.support_tree = None
        self.radio_updated_at = 0.0

    def install_calibration(self, values):
        self.calibration = CalibrationSnapshot(
            **{
                **values,
                "signal_thresholds_db": MappingProxyType(
                    dict(values["signal_thresholds_db"])
                ),
            }
        )

    def install_support(
        self, coordinates, node_ids, radii, updated_at, source_signature=None
    ):
        if coordinates is not None:
            coordinates.setflags(write=False)
        self.support = SupportSnapshot(
            coordinates,
            tuple(node_ids),
            MappingProxyType({key: i for i, key in enumerate(node_ids)}),
            MappingProxyType(dict(radii)),
            updated_at,
            source_signature,
        )
        self.support_tree = None


UNLOADED_MAINLAND = object()
