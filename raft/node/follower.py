import logging
from node import Node, NodeID
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

        if msg.body.term < self._current_term:
            reply(
                msg,
                type="append_entries_response",
                term=self._current_term,
                success=False,
            )
        # TODO
