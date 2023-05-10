import logging
from node import Node, NodeID
from time import time
from math import ceil
from leader import Leader
from time import time, sleep
from random import uniform
from utils.ms import send
from follower import Follower
from threading import Thread

# TODO: change
ELECTION_TIMEOUT = 0.3  # seconds


class Candidate(Node):

    _voters: set[NodeID]
    _majority: int

    _next_election_timer: float
    _election_timer: float

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]) -> None:
        super().__init__(node_id, node_ids)

        self._voters = set()
        self._voters.add(self._node_id)
        self._majority = ceil(len(node_ids) / 2)

        Thread(target=self.check_election_timer).start()

    @classmethod
    def transition_from(cls, node: Node):

        logging.info(f"Transitioning from {node} to Candidate")
        state = super().transition_from(node)

        state._current_term += 1
        state._valid_state = True
        state.reset_election_timer()

        return state

    def reset_election_timer(self) -> None:
        self._election_timer = uniform(ELECTION_TIMEOUT / 2, ELECTION_TIMEOUT)
        self._next_election_timer = time() + self._election_timer

    def check_timeout(self):
        #  Waits until the node reaches the election timeout
        while self._valid_state and time() < self._next_election_timer:
            sleep(self._election_timer)

        # If election timeout elapses: start new election
        if self._valid_state:
            self._valid_state = False
            # Sends a message to itself to trigger a new election
            send(self._node_id, self._node_id, type='start_new_election')

    def request_vote(self):
        log_size = len(self._log)
        last_log_term = self._current_term if log_size == 0 else self._log[-1].term
        for dest_id in self._node_ids:
            send(self._node_id,
                 dest_id,
                 type="request_vote",
                 term=self._current_term,
                 candidate_id=self._node_id,
                 last_log_index=log_size,
                 last_log_term=last_log_term
                 )

    def check_if_can_became_leader(self) -> Node:
        if len(self._voters) >= self._majority:
            self._valid_state = False
            return Leader.transition_from(self)
        return self

    # handlers
    def handle_request_vote_response(self, msg) -> Node:
        if msg.body.vote_granted and \
                msg.body.term == self._current_term and \
                msg.src not in self._voters:

            self.reset_election_timer()
            self._voters.add(msg.src)

        return self.check_if_can_became_leader()

    def handle_start_new_election(self, msg) -> Node:
        if msg.src == self._node_id:
            self._valid_state = False
            return Candidate.transition_from(self)

        return self
