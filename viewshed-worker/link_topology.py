"""Graph-scoped topology snapshots; rebuild jobs never borrow the public graph."""

from dataclasses import dataclass
from types import MappingProxyType
from collections.abc import Mapping
import time
from link_repository import load_positioned_repeaters, GraphRepository
from worker_settings import DEFAULT_SETTINGS


def link_pair_key(a_id: str, b_id: str) -> tuple[str, str]:
    return (a_id, b_id) if a_id < b_id else (b_id, a_id)


@dataclass(frozen=True)
class TopologySnapshot:
    nodes: Mapping
    physical_pairs: frozenset
    updated_at: float


class TopologyCache:
    def __init__(self, scope="public", settings=DEFAULT_SETTINGS):
        self.settings = settings
        self.scope = scope
        self.graph = GraphRepository("core" if scope == "public" else scope)
        self.snapshot = TopologySnapshot(MappingProxyType({}), frozenset(), 0.0)

    def refresh(self, db, force=False):
        now = time.time()
        if (
            not force
            and now - self.snapshot.updated_at < self.settings.LINK_TOPOLOGY_REFRESH_S
        ):
            return
        nodes = load_positioned_repeaters(db, settings=self.settings)
        with db.cursor() as cur:
            self.graph.execute(
                cur,
                "SELECT node_a_id, node_b_id FROM {node_links} WHERE itm_viable = true OR force_viable = true",
            )
            pairs = frozenset(link_pair_key(a, b) for a, b in cur.fetchall())
        immutable_nodes = MappingProxyType(
            {key: MappingProxyType(value) for key, value in nodes.items()}
        )
        self.snapshot = TopologySnapshot(immutable_nodes, pairs, now)

    def get(self, db, required_node_ids=()):
        self.refresh(db)
        if any(
            node_id and node_id not in self.snapshot.nodes
            for node_id in required_node_ids
        ):
            self.refresh(db, force=True)
        return self.snapshot.nodes, self.snapshot.physical_pairs

    def include_pair(self, a, b):
        self.snapshot = TopologySnapshot(
            self.snapshot.nodes,
            self.snapshot.physical_pairs | {link_pair_key(a, b)},
            self.snapshot.updated_at,
        )

    def update_node(self, node_id, node):
        nodes = dict(self.snapshot.nodes)
        nodes[node_id] = MappingProxyType(dict(node))
        self.snapshot = TopologySnapshot(
            MappingProxyType(nodes),
            self.snapshot.physical_pairs,
            self.snapshot.updated_at,
        )
