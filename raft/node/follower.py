from node.candidate import Candidate
import logging
from node.node import Node, NodeID
from time import time
from utils.ms import reply
from random import uniform


class Follower(Node):
    _next_timeout: float

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]) -> None:
        super().__init__(node_id, node_ids)
        self.reset_timeout()

    @classmethod
    def transition_from(cls, node: Node):
        logging.info(f"Transitioning from {node} to Follower")
        return super().transition_from(node)

    def reset_timeout(self) -> None:
        self._next_timeout = time()  # + uniform() TODO

    def handle_append_entries(self, msg) -> Node:
        self.reset_timeout()

        # 1 . Older term &&
        # 2. Log doesn't contain entry at prev_log_index which matches prev term
        if msg.body.term < self._current_term or not self.log_contains(
            msg.body.prev_log_index, msg.body.prev_log_term
        ):
            reply(
                msg,
                type="append_entries_response",
                term=self._current_term,
                success=False,
            )
        else:
            # 3. Delete conflicting entry and all that follow it
            # &&
            # 4. Append any new entries not already in the log
            self._log[msg.body.prev_log_index:] = msg.body.entries

            # 5. If leader commit > commit index, set commit index to min(leader commit, index of last new entry)
            if msg.body.leader_commit > self._commit_index:
                self._commit_index = min(
                    msg.body.leader_commit, len(self._log))
                self.apply()

            # TODO averiguar se é importante responder ao heartbeat
            # Responde se não for heartbeat
            if len(msg.body.entries) > 0:
                reply(
                    msg,
                    type="append_entries_response",
                    term=self._current_term,
                    success=True,
                    last_index=len(self._log),
                )

        return self

    # Handle message sent to self
    def handle_turn_candidate(self, msg):
        return Candidate.transition_from(self)

    def handle_request_vote(self, msg):
        self.reset_timeout()
        return super().handle_request_vote(msg)
