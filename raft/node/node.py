from __future__ import annotations
from abc import ABC
from typing import Any
from utils.ms import reply
import logging
from key_value_store import KeyValueStore

NodeID = str


class Entry:
    term: int
    command: Any  # for now

    def __init__(self, term: int, command: Any) -> None:
        self.term = term
        self.command = command


class Node(ABC):
    _node_id: NodeID
    _node_ids: list[NodeID]
    _store: KeyValueStore
    _valid_state: bool

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
        self._valid_state = True

        self._current_term = 0
        self._voted_for = None
        self._log = []
        self._commit_index = 0
        self._last_applied = 0

    @classmethod
    def transition_from(cls, node: Node):
        node._valid_state = False
        new_state = cls(node._node_id, node._node_ids)
        new_state._store = node._store
        new_state._current_term = node._current_term
        new_state._voted_for = node._voted_for
        new_state._log = node._log
        new_state._commit_index = node._commit_index
        new_state._last_applied = node._last_applied
        return new_state

    def is_from_client(self, msg) -> bool:
        return msg.src not in [self._node_id] + self._node_ids

    def handle(self, msg) -> Node:
        # If RPC request or response contains term T > currentTerm:
        #   set currentTerm = T, convert to follower (§5.1)
        if not self.is_from_client(msg):
            if msg.body.term > self._current_term:
                self._valid_state = False
                self._current_term = msg.body.term
                self._voted_for = None
                return Follower.transition_from(self).handle(msg)

        match msg.body.type:
            case "read" | "write" | "cas":
                return self.handle_kvs_op(msg)

            case "append_entries":
                # reply false if term < currentTerm (§5.1)
                return self.handle_append_entries(msg)

            case "append_entries_response":
                return self.handle_append_entries_response(msg)

            case _:
                logging.warning("unknown message type %s", msg.body.type)
                return self

    def handle_kvs_op(self, msg) -> Node:
        reply(msg, type="error", code=11, text="only the leader can handle requests")
        return self


from node.follower import Follower
