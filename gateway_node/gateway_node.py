from __future__ import annotations
from raft.node.node import Node, NodeID
from raft.node.follower import Follower
import math
from random import uniform
from typing import Tuple, Any


MsgID = int
ClientID = int


class GatewayNode:
    _raft_node: Node
    _node_id: NodeID
    _node_ids: list[NodeID]
    _quorum_read_fraction: float
    _quorum_responses: dict[MsgID, QuorumReadState]

    def __init__(self, node_id: NodeID, node_ids: list[NodeID]):
        self._node_id = node_id
        self._node_ids = node_ids
        self._raft_node = Follower(node_id, node_ids)
        self._quorum_read_fraction = self.compute_quorum_read_fraction()

    def compute_quorum_read_fraction(self):
        # Current node id was already removed
        n = len(self._node_ids) + 1
        p = self.prob_included_majority()
        return 1 - ((p * (n - 2)) / (n + p * (n - 2)))

    def prob_included_majority(self):
        n = len(self._node_ids) + 1
        if n == 3:
            return 1
        else:
            return math.comb(n - 3, math.ceil(n / 2) - 1) / math.comb(
                n - 2, math.ceil(n / 2)
            )

    def handle(self, msg):

        match msg.body.type:
            case "read":
                self.handle_read(msg)
            case "quorum_read":
                pass
            case "quorum_read_response":
                pass
            case "leaseholder_read":
                pass
            case "leaseholder_read_response":
                pass
            case _:
                self._raft_node = self._raft_node.handle(msg)

    def handle_read(self, msg):
        if self.is_leaseholder():
            pass
        else:
            chosen_value = uniform(0, 1)
            if chosen_value <= self._quorum_read_fraction:
                self.quorum_read(msg)
            else:
                self.leaseholder_read(msg)

    def handle_quorum_read(self, msg):
        pass

    def handle_quorum_read_response(self, msg):
        pass

    def handle_leaseholder_read(self, msg):
        pass

    def handle_leaseholder_read_response(self, msg):
        pass

    def leaseholder_read(self, msg):
        pass

    def quorum_read(self, msg):
        pass


class QuorumReadResponse:
    _timestamp: int
    _data: Any

    def __init__(self, timestamp: int, data: Any):
        self._timestamp = timestamp
        self._data = data


class QuorumReadState:
    _client_id: ClientID
    _number_responses: int
    _most_updated_response: QuorumReadResponse | None

    def __init__(self, client_id: ClientID):
        self._client_id = client_id
        self._number_responses = 0
        self._most_updated_response = None
