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
"""Implements the receiver."""

import argparse
import logging
import sys

import messageio
import message_types
import message_utils


def main(argv):
    """Entry point of the receiver."""
    args = parse_args(argv[1:])
    with message_utils.receiver_manager(args.address) as manager:
        event_loop = messageio.EventLoop()
        event_loop.add_resource(manager, receiver_loop(manager))
        event_loop.add_client(manager, message_types.SERVER_ID)
        event_loop.run_forever()


async def receiver_loop(manager):
    """Loop executed by the receiver."""
    await manager.send_new_client_message()
    message = await manager.recv_message()

    if message.header.type != message_types.MessageType.ACK:
        logging.error("Invalid message received: %s", repr(message))
        raise RuntimeError()

    manager.origin = message.header.destination
    print('Registered as client number {}'.format(manager.origin))

    while True:
        message = await manager.recv_message()
        if message.header.type == message_types.MessageType.MSG:
            print('Message from {}: {}'.format(message.header.origin,
                                               message.data))
            await manager.send_ack_message(message)
        elif message.header.type == message_types.MessageType.CLIST:
            print('List of clients: {}'.format(message.data))
            await manager.send_ack_message(message)
        else:
            logging.error("Invalid message received: %s", repr(message))
            raise RuntimeError()


def parse_args(argv):
    """Parses args specifying an address in the port format."""
    parser = argparse.ArgumentParser(
        description="Recebedor TP2 Redes de Computadores")

    parser.add_argument(
        'address',
        metavar='IP:PORT',
        action=message_utils.make_address_parser(),
        help='IP and port to connect to.')

    return parser.parse_args(argv)


if __name__ == '__main__':
    main(sys.argv)
