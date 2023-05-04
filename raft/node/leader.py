from node import Node, NodeID
from typing import Type
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
