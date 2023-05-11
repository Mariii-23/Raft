from __future__ import annotations
from math import ceil
from node.node import Node, NodeID, Entry
from utils.ms import send, reply
import logging
from multitimer import MultiTimer
from config import HEARTBIT_RATE


class Leader(Node):
    # Next log entry to be sent to each server
    _next_index: dict[NodeID, int]
    # Index of highest log entry known to be replicated on server
    _match_index: dict[NodeID, int]

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]):
        super().__init__(node_id, node_ids)
        self._timer = MultiTimer(HEARTBIT_RATE, self.heartbeat, runonstart=False)
        self._timer.start()
        self._next_index = dict.fromkeys(node_ids, 0)
        self._match_index = dict.fromkeys(node_ids, 1)
        self._voted_for = node_id
        logging.info("Leader %s initialized", node_id)

    @classmethod
    def transition_from(cls, node: Node) -> Leader:
        logging.info(f"Transitioning from {node} to Leader")
        new_state: Leader = super().transition_from(node)
        new_state._next_index = dict.fromkeys(
            new_state._node_ids, len(new_state._log) + 1
        )
        return new_state

    def heartbeat(self):
        send(self._node_id, self._node_ids, type="heartbeat")

    # Message handlers

    def handle_heartbeat(self, msg) -> Leader:
        self.append_empty_entries_to_all()

        return self

    def handle_kvs_op(self, msg) -> Leader:
        self._log.append(Entry(self._current_term, msg))

        if len(self._node_ids) > 0:
            self.append_entries_to_all()
        else:
            self.try_commit()

        return self

    def handle_append_entries_response(self, msg) -> Leader:
        if msg.body.success:
            # If successful:
            #   update nextIndex and matchIndex for follower
            self._match_index[msg.src] = msg.body.last_index
            self._next_index[msg.src] = msg.body.last_index + 1
            self.try_commit()

        else:
            # If AppendEntries fails because of log inconsistency:
            #   decrement nextIndex and retry
            self._next_index[msg.src] = max(1, self._next_index[msg.src] - 1)
            self.append_entries(msg.src)

        return self

    # AppendEntries RPC

    def append_entries(self, node: NodeID, empty_entries=False) -> None:
        """
        Append entries to a node.
        """
        prev_log_idx = self._next_index[node] - 1

        prev_log_term = 0
        if prev_log_idx > 0:
            prev_log_term = self._log[prev_log_idx - 1].term

        entries = self._log[prev_log_idx:] if not empty_entries else []

        send(
            self._node_id,
            node,
            type="append_entries",
            term=self._current_term,
            leader_id=self._node_id,
            prev_log_idx=prev_log_idx,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self._commit_index,
        )

    def append_entries_to_all(self) -> None:
        """
        Append entries to all nodes that aren't updated.
        """
        for node in self._node_ids:
            if self._next_index[node] <= len(self._log):
                self.append_entries(node)

    def append_empty_entries_to_all(self) -> None:
        """
        Append empty entry to all nodes, used as heartbeat.
        """
        for node in self._node_ids:
            self.append_entries(node, empty_entries=True)

    # KeyValueStore ops

    def apply_read(self, msg) -> None:
        value = self._store.read(msg.body.key)
        if value:
            reply(msg, type="read_ok", value=value)
        else:
            reply(msg, type="error", code=20, text="key not found")

    def apply_write(self, msg) -> None:
        self._store.write(msg.body.key, msg.body.value)
        reply(msg, type="write_ok")

    def apply_cas(self, msg) -> None:
        value = self._store.read(msg.body.key)

        if value is None:
            reply(msg, type="error", code=20, text="key not found")
        elif value != msg.body.__dict__["from"]:
            reply(msg, type="error", code=22, text='"from" is different')
        else:
            self._store.write(msg.body.key, msg.body.to)
            reply(msg, type="cas_ok")

    def try_commit(self) -> None:
        # If there exists an N such that N > commitIndex,
        # a majority of matchIndex[i] >= N,
        # and log[N].term == currentTerm:
        #   set commitIndex = N
        log_indexes = [i for i in self._match_index.values() if i > self._commit_index]
        majority = ceil(len(self._node_ids) / 2)

        if len(log_indexes) > majority:
            next_commit_index = min(log_indexes, default=len(self._log))
            if self._log[next_commit_index - 1].term == self._current_term:
                self._commit_index = next_commit_index
                self.apply()
                self.append_entries_to_all()
