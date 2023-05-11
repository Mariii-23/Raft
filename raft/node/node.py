from __future__ import annotations
from abc import ABC
from typing import Any
from utils.ms import reply
from utils.random_timer import RandomTimer
from multitimer import MultiTimer
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
    _timer: RandomTimer | MultiTimer

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

    @classmethod
    def transition_from(cls, node: Node):
        node._timer.stop()
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

    def is_from_self(self, msg) -> bool:
        return msg.src == self._node_id

    def handle(self, msg) -> Node:
        # If RPC request or response contains term T > currentTerm:
        #   set currentTerm = T, convert to follower
        if not self.is_from_client(msg) and not self.is_from_self(msg):
            # Every message that is not from a client nor from the node itself
            # is a RAFT RPC call, that has a term
            if msg.body.term > self._current_term:
                self._current_term = msg.body.term
                self._voted_for = None
                return Follower.transition_from(self).handle(msg)

        match msg.body.type:
            case "read" | "write" | "cas":
                return self.handle_kvs_op(msg)

            case _:
                try:
                    return getattr(self, "handle_" + msg.body.type)(msg)

                except AttributeError:
                    logging.warning("unknown message type %s", msg.body.type)
                    return self

    def handle_request_vote(self, msg):
        grant_vote: bool = False
        if (
            msg.body.term >= self._current_term
            # hasn't voted for a different candidate
            and self._voted_for in [None, msg.body.candidate_id]
            # candidate's log is at least as up-to-date as receiver's log
            and self.log_is_up_to_date(msg.body.last_log_index, msg.body.last_log_term)
        ):
            grant_vote = True
            self._voted_for = msg.body.candidate_id

        reply(
            msg,
            type="request_vote_response",
            term=self._current_term,
            vote_granted=grant_vote,
        )
        return self

    def handle_kvs_op(self, msg) -> Node:
        reply(msg, type="error", code=11, text="only the leader can handle requests")
        return self

    def apply(self) -> None:
        """
        Apply commited messages that still weren't applied
        """
        for entry in self._log[self._last_applied + 1 : self._commit_index + 1]:
            match entry.command.body.type:
                case "read":
                    self.apply_read(entry.command)

                case "write":
                    self.apply_write(entry.command)

                case "cas":
                    self.apply_cas(entry.command)

    def apply_read(self, msg) -> None:
        pass

    def apply_write(self, msg) -> None:
        self._store.write(msg.body.key, msg.body.value)

    def apply_cas(self, msg) -> None:
        value = self._store.read(msg.body.key)

        if value is None:
            return
        elif value != msg.body.__dict__["from"]:
            return
        else:
            self._store.write(msg.body.key, msg.body.to)

    def log_contains(self, index: int, term: int) -> bool:
        return index == 0 or (
            len(self._log) >= index and self._log[index - 1].term == term
        )

    # Check if received log is at least as up-to-date to the current log
    def log_is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        my_term = self._log[-1].term if len(self._log) > 0 else 0
        if my_term != last_log_term:
            return last_log_term > my_term
        return last_log_index >= len(self._log)


from node.follower import Follower
