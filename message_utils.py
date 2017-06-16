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
"""Utilities for using the message libraries."""

import argparse
import contextlib
import socket

import message_manager


@contextlib.contextmanager
def server_listener(address):
    """Manages a message_manager.ServerListener for the given address."""
    server_socket = socket.socket()
    server_socket.setblocking(False)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(address)
    server_socket.listen()

    try:
        yield message_manager.ServerListener(server_socket)
    finally:
        server_socket.close()


@contextlib.contextmanager
def receiver_manager(address):
    """Manages a message_manager.ReceiverManager for the given address."""
    client_socket = _create_client_socket(address)

    try:
        yield message_manager.ReceiverManager(client_socket)
    finally:
        client_socket.close()


@contextlib.contextmanager
def sender_manager(address, receiver_id):
    """Manages a message_manager.SenderManager for the given address."""
    client_socket = _create_client_socket(address)

    try:
        yield message_manager.SenderManager(client_socket, receiver_id)
    finally:
        client_socket.close()


def _create_client_socket(address):
    """Creates a connected client socket for address."""
    client_socket = socket.socket()
    client_socket.connect(address)
    return client_socket


def make_address_parser(port_only=False):
    """Makes an address parser, parsing only port or not."""

    class IpParser(argparse.Action):
        """Parses an ip address with argparse."""

        def __call__(self, parser, namespace, value, option_string=None):
            ip, port = (('', value) if port_only else value.split(':'))
            setattr(namespace, self.dest, (ip, int(port)))

    return IpParser
