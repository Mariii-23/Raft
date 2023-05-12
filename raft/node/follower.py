from __future__ import annotations
from raft.node.candidate import Candidate
import logging
from raft.node.node import Node, NodeID
from raft.utils.ms import reply, send
from raft.utils.random_timer import RandomTimer
from raft.config import LOWER_TIMEOUT, UPPER_TIMEOUT


class Follower(Node):
    _timer: RandomTimer

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]) -> None:
        super().__init__(node_id, node_ids)
        self._timer = RandomTimer(LOWER_TIMEOUT, UPPER_TIMEOUT, self.handle_timeout)
        self._timer.start()
        logging.info("init follower")

    @classmethod
    def transition_from(cls, node: Node) -> Follower:
        logging.info(f"Transitioning from {node} to Follower")
        return super().transition_from(node)

    def handle_timeout(self) -> None:
        send(self._node_id, self._node_id, type="turn_candidate")

    def handle_append_entries(self, msg) -> Follower:
        self._timer.reset()

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
            self._log[msg.body.prev_log_index :] = msg.body.entries

            # 5. If leader commit > commit index, set commit index to min(leader commit, index of last new entry)
            if msg.body.leader_commit > self._commit_index:
                self._commit_index = min(msg.body.leader_commit, len(self._log))
                self.apply()

            reply(
                msg,
                type="append_entries_response",
                term=self._current_term,
                success=True,
                last_index=len(self._log),
            )

        return self

    # Handle message sent to self
    def handle_turn_candidate(self, msg) -> Candidate:
        return Candidate.transition_from(self)

    def handle_request_vote(self, msg):
        self._timer.reset()
        return super().handle_request_vote(msg)
