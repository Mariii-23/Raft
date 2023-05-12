#!/usr/bin/env python

import logging
from raft.utils.ms import receive_all, reply, exit_on_error
from concurrent.futures import ThreadPoolExecutor
from gateway_node.gateway_node import GatewayNode


logging.getLogger().setLevel(logging.DEBUG)
executor = ThreadPoolExecutor(max_workers=1)

node: GatewayNode


def handle_init(msg):
    global handler, node

    if msg.body.type != "init":
        logging.warning("System is still being initialized")
        return

    node_id = msg.body.node_id
    node_ids = msg.body.node_ids
    node_ids.remove(node_id)
    node = GatewayNode(node_id, node_ids)

    logging.info("node %s initialized", node_id)

    reply(msg, type="init_ok")

    handler = handle_rest


def handle_rest(msg):
    global node
    node.handle(msg)


handler = handle_init
executor.map(lambda msg: exit_on_error(handler, msg), receive_all())
