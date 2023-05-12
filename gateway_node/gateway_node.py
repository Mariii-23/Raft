from __future__ import annotations
from raft.node.node import Node, NodeID
from raft.node.follower import Follower
import logging
from raft.utils.ms import send, reply
import math
from random import uniform, sample
from typing import Any
from threading import Timer
from raft.config import HEARTBIT_RATE


MsgID = int
ClientID = int


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

    def has_conflict(self) -> bool:
        return self.has_conflict()

    def get_data(self) -> Any:
        return self._data

    def get_timestamp(self) -> int:
        return self._timestamp


class QuorumReadState:
    _client_id: ClientID
    _number_responses: int
    _most_updated_response: QuorumReadResponse

    def __init__(self, client_id: ClientID, most_updated_response: QuorumReadResponse):
        self._client_id = client_id
        self._number_responses = 1
        self._most_updated_response = most_updated_response

    def update(self, response: QuorumReadResponse):
        if response._timestamp > self._most_updated_response._timestamp:
            self._most_updated_response = response
            self._number_responses += 1

    def get_number_responses(self):
        return self._number_responses

    def get_most_updated(self):
        return self._most_updated_response


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

    def compute_quorum_read_fraction(self) -> float:
        # Current node id was already removed
        n = len(self._node_ids) + 1
        p = self.prob_included_majority()
        return 1 - ((p * (n - 2)) / (n + p * (n - 2)))

    def prob_included_majority(self) -> float:
        n = len(self._node_ids) + 1
        if n == 3:
            return 1
        else:
            return math.comb(n - 3, math.ceil(n / 2) - 1) / math.comb(
                n - 2, math.ceil(n / 2)
            )

    def compute_excluding_majority(self) -> int:
        return math.ceil(len(self._node_ids) / 2)

    def handle(self, msg):
        match msg.body.type:
            case "write" | "cas":
                self._raft_node = self._raft_node.handle(msg)
            case _:
                getattr(self, "handle_" + msg.body.type, self.handle_unknown_message)(
                    msg
                )

    def handle_unknown_message(self, msg):
        logging.warning(f"Unknown message type {msg.body.type}")

    def handle_read(self, msg) -> None:
        if self.is_leaseholder():
            pass
        else:
            chosen_value = uniform(0, 1)
            if chosen_value <= self._quorum_read_fraction:
                self.quorum_read(msg)
            else:
                self.leaseholder_read(msg)

    def handle_quorum_read(self, msg) -> None:
        quorum_read_response = self.build_quorum_read_response(msg.body.key)
        reply(
            msg,
            type="quorum_read_response",
            client_req_id=msg.body.client_req_id,
            **quorum_read_response.__dict__,
        )

    def handle_quorum_read_response(self, msg) -> None:
        client_req_id = msg.body.client_req_id

        if msg.body.client_req_id in self._quorum_responses:
            quorum_read_state = self._quorum_responses[msg.body.client_req_id]
            response = QuorumReadResponse(
                msg.body.timestamp, msg.body.data, msg.body.has_conflict
            )
            quorum_read_state.update(response)
            number_responses = quorum_read_state.get_number_responses()
            if self.has_quorum_responses(number_responses):
                self.reply_to_client_read(client_req_id, quorum_read_state)
                self._quorum_responses.pop(msg.body.client_req_id)

    def has_quorum_responses(self, number_responses: int) -> bool:
        return number_responses > self.compute_excluding_majority()

    def reply_to_client_read(
        self, client_req_id: MsgID, quorum_read_state: QuorumReadState
    ) -> None:
        most_updated_response = quorum_read_state.get_most_updated()
        if most_updated_response.has_conflict():
            send(
                self._node_id,
                quorum_read_state._client_id,
                in_reply_to=client_req_id,
                type="error",
                code="11",
                text="Write conflict for key",
            )
        else:
            value = most_updated_response.get_data()
            if value:
                send(
                    self._node_id,
                    quorum_read_state._client_id,
                    in_reply_to=client_req_id,
                    type="read_ok",
                    value=value,
                )
            else:
                send(
                    self._node_id,
                    quorum_read_state._client_id,
                    in_reply_to=client_req_id,
                    type="error",
                    code="20",
                    text="key not found",
                )

    def handle_leaseholder_read(self, msg) -> None:
        if self.is_leaseholder():
            reply(
                msg,
                type="leaseholder_read_response",
                success=True,
                value=self._raft_node.direct_read(msg.body.key),
                client_id=msg.body.client_id,
                in_reply_to=msg.body.in_reply_to,
            )
        else:
            reply(
                msg,
                type="leaseholder_read_response",
                success=False,
                client_id=msg.body.client_id,
                in_reply_to=msg.body.in_reply_to,
            )

    def handle_leaseholder_read_response(self, msg) -> None:
        if msg.body.success:
            if msg.body.value:
                send(
                    self._node_id,
                    msg.body.client_id,
                    in_reply_to=msg.body.in_reply_to,
                    type="read_ok",
                    value=msg.body.value,
                )
            else:
                send(
                    self._node_id,
                    msg.body.client_id,
                    in_reply_to=msg.body.in_reply_to,
                    type="error",
                    code=20,
                    text="key not found",
                )
        else:
            send(
                self._node_id,
                msg.body.client_id,
                in_reply_to=msg.body.in_reply_to,
                type="error",
                code=11,
                text="outdated leaseholder",
            )

    def handle_delete_quorum_state(self, msg) -> None:
        self._quorum_responses.pop(msg.body.msg_id, None)

    def leaseholder_read(self, msg) -> None:
        if value := self._raft_node.direct_read(msg.body.key):
            reply(msg, type="read_ok", value=value)
        else:
            reply(msg, type="error", code=20, text="key not found")

    def quorum_read(self, msg) -> None:
        # majority of all nodes, not counting with self
        majority = self.compute_excluding_majority()
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
        Timer(HEARTBIT_RATE * 2, lambda: self.delete_quorum_state(msg.id)).start()

    def delete_quorum_state(self, msg_id: MsgID) -> None:
        send(
            self._node_id,
            self._node_id,
            type="delete_quorum_state",
            msg_id=msg_id,
        )

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

    def is_leaseholder(self) -> bool:
        return self._raft_node.is_leader()

    def build_quorum_read_response(self, key) -> QuorumReadResponse:
        has_conflict = self.has_conflict(key)
        data = self._raft_node.direct_read(key) if not has_conflict else None

        return QuorumReadResponse(
            self._raft_node.get_last_applied(),
            data,
            has_conflict,
        )
