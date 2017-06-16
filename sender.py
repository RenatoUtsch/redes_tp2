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
"""Implements the sender."""

import argparse
import logging
import sys

import messageio
import message_types
import message_utils


def main(argv):
    """Entry point of the receiver."""
    args = parse_args(argv[1:])
    with message_utils.sender_manager(args.address,
                                      args.receiver_id) as manager:
        event_loop = messageio.EventLoop()
        event_loop.add_resource(manager, sender_loop(manager))
        event_loop.run_forever()


async def sender_loop(manager):
    """Loop executed by the sender."""
    print('Requesting client number')
    await manager.send_new_client_message()
    message = await manager.recv_message()

    if message.header.type != message_types.MessageType.ACK:
        logging.error('Invalid initial ack: %s', repr(message))
        raise RuntimeError()

    manager.origin = message.header.destination
    print('Registered as client number {}'.format(manager.origin))

    while True:
        message_type = input('{MSG,CREQ}: ')

        if message_type.upper() == 'MSG':
            destination = int(
                input('Insert a destination (0 for broadcast): '))
            message_data = input('Insert a new message to send: ')
            await manager.send_msg_message(message_data, destination)
            await manager.recv_ack_message()
        elif message_type.upper() == 'CREQ':
            destination = int(input('Insert a destination: '))
            await manager.send_creq_message(destination)
            await manager.recv_ack_message()
        else:
            print('Invalid input. Please try again.\n')


def parse_args(argv):
    """Parses args specifying an address in the port format."""
    parser = argparse.ArgumentParser(
        description="Emissor TP2 Redes de Computadores")

    parser.add_argument(
        'address',
        metavar='IP:PORT',
        action=message_utils.make_address_parser(),
        help='IP and port to connect to.')

    parser.add_argument(
        'receiver_id',
        metavar='RECEIVER_ID',
        nargs='?',
        default=1,
        type=int,
        help='ID of the receiver that will be linked to the sender.')

    return parser.parse_args(argv)


if __name__ == '__main__':
    main(sys.argv)
