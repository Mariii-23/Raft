from __future__ import annotations
from abc import ABC
from typing import Any
from key_value_store import KeyValueStore

NodeID = str


class Entry:
    term: int
    command: Any  # for now


class Node(ABC):
    _node_id: NodeID
    _node_ids: list[NodeID]
    _store: KeyValueStore

    # Raft vars
    _current_term: int
    _voted_for: NodeID | None
    _log: list[Entry]
    _commit_index = 0
    _last_applied = 0

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]) -> None:
        self._node_id = node_id
        self._node_ids = node_ids
        self._store = KeyValueStore()

        self._current_term = 0
        self._voted_for = None
        self._log = []
        self._commit_index = 0
        self._last_applied = 0
