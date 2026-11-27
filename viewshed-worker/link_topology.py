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
        self._pair_changes = {}
        self._committed = None

    def fork(self):
        # A sequential worker owns the committed cache. A job borrows the
        # immutable base and committed additions without cloning the graph.
        staged = TopologyCache(self.scope, self.settings)
        staged.snapshot = self.snapshot
        staged._committed = self
        return staged

    def commit(self, staged):
        if staged.snapshot.physical_pairs is not self.snapshot.physical_pairs:
            self._pair_changes.clear()
        self.snapshot = staged.snapshot
        for pair, present in staged._pair_changes.items():
            if present == (pair in self.snapshot.physical_pairs):
                self._pair_changes.pop(pair, None)
            else:
                self._pair_changes[pair] = present

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
        self._pair_changes.clear()

    def get_nodes(self, db, required_node_ids=()):
        if self.scope != "public" and self.snapshot.updated_at > 0:
            # Rebuild jobs have always used current node eligibility, positions
            # and antenna data. Only the independently versioned pairs are reused.
            nodes = load_positioned_repeaters(db, settings=self.settings)
            self.snapshot = TopologySnapshot(
                MappingProxyType({key: MappingProxyType(value) for key, value in nodes.items()}),
                self.snapshot.physical_pairs,
                self.snapshot.updated_at,
            )
        else:
            self.refresh(db)
        if any(
            node_id and node_id not in self.snapshot.nodes
            for node_id in required_node_ids
        ):
            self.refresh(db, force=True)
        return self.snapshot.nodes

    def has_pair(self, a, b):
        pair = link_pair_key(a, b)
        if pair in self._pair_changes:
            # A job began with this pair visible. Apply any removal only to
            # the committed view, as the next full graph read used to do.
            return self._committed is not None or self._pair_changes[pair]
        if self._committed is not None and self._committed.snapshot.physical_pairs is self.snapshot.physical_pairs:
            return self._committed.has_pair(*pair)
        return pair in self.snapshot.physical_pairs

    def include_pair(self, a, b):
        pair = link_pair_key(a, b)
        if self._pair_changes.get(pair) is False or not self.has_pair(a, b):
            self._pair_changes[pair] = True

    def note_measured_pair(self, db, a, b, itm_viable):
        if itm_viable:
            self.include_pair(a, b)
        elif self.scope != "public" and self.has_pair(a, b):
            # Preserve a force override when a recomputation changes viability.
            pair = link_pair_key(a, b)
            with db.cursor() as cur:
                self.graph.execute(cur, """SELECT 1 FROM {node_links}
                    WHERE node_a_id = %s AND node_b_id = %s
                      AND (itm_viable = true OR force_viable = true)""", pair)
                if cur.fetchone() is None:
                    self._pair_changes[pair] = False

    def update_node(self, node_id, node):
        nodes = dict(self.snapshot.nodes)
        nodes[node_id] = MappingProxyType(dict(node))
        self.snapshot = TopologySnapshot(
            MappingProxyType(nodes),
            self.snapshot.physical_pairs,
            self.snapshot.updated_at,
        )
