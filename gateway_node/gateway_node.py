from __future__ import annotations
from raft.node.node import Node, NodeID
from raft.node.follower import Follower
from raft.utils.ms import send, reply
import math
from random import uniform, sample
from typing import Any


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
        self._quorum_responses = {}

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
        quorum_read_response = self.build_quorum_read_response(msg.body.key)
        reply(
            msg,
            type="quorum_read_response",
            client_req_id=msg.body.client_req_id,
            **quorum_read_response.__dict__,
        )

    def handle_quorum_read_response(self, msg):
        pass

    def handle_leaseholder_read(self, msg):
        pass

    def handle_leaseholder_read_response(self, msg):
        pass

    def leaseholder_read(self, msg):
        pass

    def quorum_read(self, msg):
        # majority of all nodes, not counting with self
        majority = math.ceil(len(self._node_ids) / 2)
        quorum = sample(self._node_ids, majority)
        for node_id in quorum:
            send(
                self._node_id,
                node_id,
                type="quorum_read",
                key=msg.key,
                client_req_id=msg.id,
            )
        my_quorum_response = self.build_quorum_read_response(msg.key)
        self._quorum_responses[msg.id] = QuorumReadState(msg.id, my_quorum_response)
        # esperar até ter maioria + 1 no dicionario
        # fazer reset ao dicionario

    def has_conflict(self, key) -> bool:
        log = self._raft_node.get_log()
        last_applied = self._raft_node.get_last_applied()

        for entry in log[last_applied + 1 - 1 :]:
            match entry.command.body.type:
                case "write" | "cas":
                    if key == entry.command.body.key:
                        return True

                case _:
                    pass

        return False

    def build_quorum_read_response(self, key) -> QuorumReadResponse:
        has_conflict = self.has_conflict(key)
        read = self._raft_node.direct_read(key) if not has_conflict else None

        return QuorumReadResponse(
            self._raft_node.get_last_applied(),
            read,
            has_conflict,
        )


class QuorumReadResponse:
    _timestamp: int
    # None if the key does not exist
    _data: Any | None
    # True if there is no write intent for that key
    _has_conflict: bool

    def __init__(self, timestamp: int, data: Any, has_conflict: bool):
        self._timestamp = timestamp
        self._data = data
        self._has_conflict = has_conflict


class QuorumReadState:
    _client_id: ClientID
    _number_responses: int
    _most_updated_response: QuorumReadResponse

    def __init__(self, client_id: ClientID, most_updated_response: QuorumReadResponse):
        self._client_id = client_id
        self._number_responses = 1
        self._most_updated_response = most_updated_response
