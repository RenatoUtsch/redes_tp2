#!/usr/bin/env python3
#
# Copyright 2017 Renato Utsch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Implements the server."""

import argparse
import logging
import sys

import messageio
import message_manager
import message_types
import message_utils


def main(argv):
    """Entry point of the server."""
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    args = parse_args(argv[1:])
    with message_utils.server_listener(args.address) as listener:
        event_loop = messageio.EventLoop()
        event_loop.add_resource(listener, listener_loop(listener, event_loop))
        event_loop.run_forever()


async def listener_loop(listener, event_loop):
    """Listener loop, adds new client connections to event_loop."""
    while True:
        connection = await listener.accept()
        manager = message_manager.ServerManager(connection)
        event_loop.add_resource(manager, server_loop(manager, event_loop))


async def server_loop(manager, event_loop):
    """Server loop, manages one client connection."""
    # First message needs to be a new client.
    message = await manager.recv_message()
    if message.header.type != message_types.MessageType.NEW_CLIENT:
        logging.error('Invalid NEW_CLIENT message: %s', repr(message))
        raise RuntimeError()

    if message.header.origin == message_types.NEW_RECEIVER_ORIGIN:
        destination = event_loop.next_receiver_id
        forward_id = destination
        receiver = True
    else:
        destination = event_loop.next_sender_id
        forward_id = None
        receiver = False

        if (message.header.origin >= message_types.FIRST_RECEIVER_ID and
                message.header.origin < message_types.END_RECEIVER_ID and
                event_loop.has_client(message.header.origin)):
            forward_id = message.header.origin

    manager.destination = destination
    event_loop.add_client(manager, forward_id)
    await manager.send_ack_message(message, destination)

    try:
        if receiver:
            await receiver_loop(manager, event_loop)
        else:
            await sender_loop(manager, event_loop)
    except KeyboardInterrupt:
        event_loop.close_server()


async def sender_loop(manager, event_loop):
    """Server sender loop, manages one sender client."""
    while True:
        message = await manager.recv_message()

        if message.header.type == message_types.MessageType.MSG:
            try:
                event_loop.forward_msg(message)
                await manager.send_ack_message(message)
            except KeyError:  # Client not found
                await manager.send_error_message(message)
        elif message.header.type == message_types.MessageType.CREQ:
            clist_message = manager.create_clist_message(
                message.header.destination, event_loop.client_list())
            try:
                event_loop.forward_msg(clist_message)
                await manager.send_ack_message(message)
            except KeyError:  # Client not found
                await manager.send_error_message(message)
        else:
            logging.error("Invalid sender message received: %s", repr(message))
            raise RuntimeError()


async def receiver_loop(manager, unused_event_loop):
    """Server receiver loop, manages one receiver client."""
    while True:
        message = await manager.recv_forwarded_message()

        if message.header.type == message_types.MessageType.CLOSE:
            await manager.send_ack_message(message)
        elif message.header.type == message_types.MessageType.MSG:
            await manager.send_msg_message(message)
            await manager.recv_ack_message(message)
        elif message.header.type == message_types.MessageType.CLIST:
            message = await manager.send_clist_message(message)
            await manager.recv_ack_message(message)
        else:
            logging.error("Invalid receiver message received: %s",
                          repr(message))
            raise RuntimeError()


def parse_args(argv):
    """Parses args specifying an address in the port format."""
    parser = argparse.ArgumentParser(
        description="Servidor TP2 Redes de Computadores")

    parser.add_argument(
        'address',
        metavar='PORT',
        action=message_utils.make_address_parser(port_only=True),
        help="Port to use.")

    return parser.parse_args(argv)


if __name__ == '__main__':
    main(sys.argv)
