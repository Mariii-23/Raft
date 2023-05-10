from math import ceil
from node.node import Node, NodeID, Entry
from utils.ms import reply
import logging


class Leader(Node):
    # Next log entry to be sent to each server
    _next_index: dict[NodeID, int]
    # Index of highest log entry known to be replicated on server
    _match_index: dict[NodeID, int]

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]):
        super().__init__(node_id, node_ids)
        self._next_index = dict.fromkeys(node_ids, 0)
        self._match_index = dict.fromkeys(node_ids, 1)
        logging.info("Leader %s initialized", node_id)

    def handle_kvs_op(self, msg) -> Node:
        self._logs.append(
            Entry(self._current_term, msg)
        )  # TODO: convert this msg to a class
        self.try_commit()

    def try_commit(self) -> None:
        majority = ceil(len(self._node_ids) / 2)
        # TODO
