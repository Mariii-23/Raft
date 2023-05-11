from __future__ import annotations
import logging
from node.node import Node, NodeID
from utils.constants import LOWER_TIMEOUT, UPPER_TIMEOUT
from time import time
from math import ceil
from leader import Leader
from time import time, sleep
from random import uniform
from raft.utils.random_timer import RandomTimer
from utils.ms import send, reply


class Candidate(Node):

    _voters: set[NodeID]
    _majority: int

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]) -> None:
        super().__init__(node_id, node_ids)
        self._timer = RandomTimer(LOWER_TIMEOUT, UPPER_TIMEOUT, self.start_new_election)
        self._timer.start()
        self._voters = set()
        self._voters.add(self._node_id)
        self._majority = ceil(len(node_ids) / 2)

        self._voted_for = node_id

    @classmethod
    def transition_from(cls, node: Node) -> Candidate:
        logging.info(f"Transitioning from {node} to Candidate")
        new_state: Candidate = super().transition_from(node)
        new_state._current_term += 1
        new_state.request_vote()
        return new_state

    def request_vote(self) -> None:
        log_size = len(self._log)
        last_log_term = self._current_term if log_size == 0 else self._log[-1].term
        for dest_id in self._node_ids:
            send(
                self._node_id,
                dest_id,
                type="request_vote",
                term=self._current_term,
                candidate_id=self._node_id,
                last_log_index=log_size,
                last_log_term=last_log_term,
            )

    def check_if_can_became_leader(self) -> Leader | Candidate:
        if len(self._voters) >= self._majority:
            return Leader.transition_from(self)
        return self

    # handlers
    def handle_request_vote_response(self, msg) -> Leader | Candidate:
        if (
            msg.body.vote_granted
            and msg.body.term == self._current_term
            and msg.src not in self._voters
        ):

            self._voters.add(msg.src)

        return self.check_if_can_became_leader()

    def handle_new_election(self, msg) -> Candidate:
        return Candidate.transition_from(self)

    def start_new_election(self) -> None:
        send(self._node_id, self._node_id, type="new_election")
