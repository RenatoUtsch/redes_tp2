"""Defines the manager that sends and receives messages."""

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

import logging

import messageio
import message_types

# Maximum message id before it overflows into 0.
_MAX_MESSAGE_ID = 65535

# Initial id for receivers.
_INITIAL_RECEIVER_ID = 0


class ServerListener:
    """Class that listens for new connections and returns new sockets.

    This manager should be used together with the event loop from messageio.
    """

    def __init__(self, socket):
        """Initializes the listener with a non-blocking server socket."""
        self._socket = socket

    def fileno(self):
        """Returns the socket fd."""
        return self._socket.fileno()

    @property
    def origin(self):
        """Origin of this listener. Always the server."""
        return message_types.SERVER_ID

    async def accept(self):
        """Returns a new connection. Waits until there is a new one."""
        await messageio.wait_for_read()
        connection, client_address = self._socket.accept()

        logging.info('New client connected: %s', client_address)
        connection.setblocking(False)

        return connection


class MessageManager:
    """Abstract message manager that sends and receives messages.

    The manager manages a direct connection from one origin (the current program
    being executed) to one destination (either the server or one of the clients,
    depending on whether the current program is the server or a client).

    This manager should be used together with the event loop from messageio.
    """

    def __init__(self, socket, origin, destination):
        """Initializes the manager."""
        self._socket = socket
        self._origin = origin
        self._destination = destination
        self.__next_message_id = 0

    def fileno(self):
        """The file descriptor of the socket."""
        return self._socket.fileno()

    @property
    def origin(self):
        """Origin id of this manager."""
        return self._origin

    @origin.setter
    def origin(self, new_origin):
        """New origin of this manager."""
        self._origin = new_origin

    @property
    def destination(self):
        """Destination id of this manager."""
        return self._destination

    @destination.setter
    def destination(self, new_destination):
        """New destination of this manager."""
        self._destination = new_destination

    @property
    def _next_message_id(self):
        """Returns the next message ID to be used in new messages.

        This property is to be used by derived classes, to get a proper new
        message ID.
        """
        next_id = self.__next_message_id
        self.__next_message_id = (self.__next_message_id + 1) % _MAX_MESSAGE_ID
        return next_id

    async def _send_bytes(self, data):
        """Sends bytes through the socket."""
        size = len(data)
        total_sent = 0
        while total_sent < size:
            await messageio.wait_for_write()
            try:
                sent = self._socket.send(data[total_sent:])
            except ConnectionResetError:  # Disconnected
                logging.error('Connection reset')
                raise RuntimeError()
            if sent == 0:
                logging.error('Connection broken')
                raise RuntimeError()
            total_sent += sent

    async def _recv_bytes(self, size):
        """Receives bytes through the socket."""
        chunks = []
        total_received = 0
        while total_received < size:
            await messageio.wait_for_read()
            try:
                chunk = self._socket.recv(size - total_received)
            except ConnectionResetError:  # Disconnected
                logging.error('Connection reset')
                raise RuntimeError()
            if not chunk:
                logging.error('Connection broken')
                raise RuntimeError()
            chunks.append(chunk)
            total_received += len(chunk)
        return b''.join(chunks)

    async def _recv_entire_message(self):
        """Receives an entire message and returns it."""
        packed_header = await self._recv_bytes(message_types.HEADER_SIZE)
        header = message_types.unpack_header(packed_header)

        if (header.type == message_types.MessageType.MSG or
                header.type == message_types.MessageType.CLIST):
            packed_size = await self._recv_bytes(message_types.SHORT_SIZE)
            size = message_types.unpack_short(packed_size)

            if header.type == message_types.MessageType.MSG:
                packed_data = await self._recv_bytes(size)
                data = packed_data.decode()
            else:
                packed_data = await self._recv_bytes(
                    size * message_types.SHORT_SIZE)
                data = message_types.unpack_shorts(size, packed_data)
        else:
            data = None

        return message_types.Message(header, data)

    async def _send_message(self, message):
        """Sends a message. To be used by this and derived classes."""
        logging.info('send: %s', repr(message))
        await self._send_bytes(message_types.pack_message(message))

    async def _accept_recv_message(self, message):
        """Accepts a new recv message. To be used by derived classes."""
        logging.info('recv: %s', repr(message))

        if message.header.type == message_types.MessageType.CLOSE:
            await self.send_ack_message(message)
            raise messageio.ResourceClosed()

        return message

    async def recv_message(self):
        """Receives a valid message. To be used by this and derived classes."""
        message = await self._recv_entire_message()
        return await self._accept_recv_message(message)

    async def recv_ack_message(self, original_message):
        """Receives an ack message for the message. Logs if it is an error."""
        message = await self.recv_message()
        if (message.header.type != message_types.MessageType.ACK and
                message.header.type != message_types.MessageType.ERROR):
            logging.error('Invalid message, expected ack: %s', repr(message))
        elif message.header.id != original_message.header.id:
            logging.error('Ack for message with wrong id: %s', repr(message))
        elif message.header.type == message_types.MessageType.ERROR:
            logging.error('Message returned as an error: %s', repr(message))

    async def recv_forwarded_message(self):
        """Receives a forwarded message"""
        return await messageio.wait_for_forward()

    async def send_ack_message(self, original_message, destination=None):
        """Sends a new message.MessageType.ACK message.

        Args:
            original_message: the message to be acknowledged by the ack message.
            destination: if specified, the destination of the ack. If not, will
                use the original_message's origin.
        """
        if destination is None:
            destination = original_message.header.origin

        message = message_types.new_message(
            type=message_types.MessageType.ACK,
            origin=self.origin,
            destination=destination,
            id=original_message.header.id)

        await self._send_message(message)

    async def send_error_message(self, original_message):
        """Sends a new message.MessageType.ERROR message.

        Args:
            original_message: the message that contained an error.
        """
        message = message_types.new_message(
            type=message_types.MessageType.ERROR,
            origin=self.origin,
            destination=original_message.header.origin,
            id=original_message.header.id)

        await self._send_message(message)

    async def send_close_message(self):
        """Sends a new message.MessageType.CLOSE message."""
        message = message_types.new_message(
            type=message_types.MessageType.CLOSE,
            origin=self.origin,
            destination=self.destination,
            id=self._next_message_id)

        await self._send_message(message)
        return message


class ServerManager(MessageManager):
    """Message manager used on the server."""

    def __init__(self, socket):
        """Initializes the manager."""
        super().__init__(socket, message_types.SERVER_ID, None)

    async def recv_message(self):
        """Receives a valid message from the given client."""
        while True:
            message = await self._recv_entire_message()
            if (self.destination is None or
                    message.header.origin == self.destination):
                return await self._accept_recv_message(message)
            else:
                logging.error('reject: %s', repr(message))
                await self.send_error_message(message)

    async def send_msg_message(self, message):
        """Sends a msg message."""
        assert message.header.type == message_types.MessageType.MSG
        await self._send_message(message)

    def create_clist_message(self, destination, client_list):
        """Creates and returns a new message.MessageType.CLIST message.

        This message will not have an id and the receiver will have to give it
        an id.

        Args:
            destination: destination of the message.
            client_list: list of client ids to send.
        """
        return message_types.new_message(
            type=message_types.MessageType.CLIST,
            origin=self.origin,
            destination=destination,
            id=None,
            data=client_list)

    async def send_clist_message(self, message):
        """Sends a new clist message to be sent with a proper id."""
        message = message_types.new_message(
            type=message.header.type,
            origin=message.header.origin,
            destination=message.header.destination,
            id=self._next_message_id,
            data=message.data)

        await self._send_message(message)
        return message


class ClientManager(MessageManager):
    """Message manager used on the client."""

    async def send_new_client_message(self):
        """Sends a new message.MessageType.NEW_CLIENT message.

        This sends the origin as is. Derived classes should take care of
        initializing the origin properly.
        """
        message = message_types.new_message(
            type=message_types.MessageType.NEW_CLIENT,
            origin=self.origin,
            destination=self.destination,
            id=self._next_message_id)

        await self._send_message(message)


class ReceiverManager(ClientManager):
    """Client manager specific for the receiver."""

    def __init__(self, socket):
        """Initializes the receiver."""
        super().__init__(
            socket,
            origin=_INITIAL_RECEIVER_ID,
            destination=message_types.SERVER_ID)


class SenderManager(ClientManager):
    """Client manager specific for the sender."""

    def __init__(self, socket, receiver_id):
        """Initializes the sender."""
        super().__init__(
            socket, origin=receiver_id, destination=message_types.SERVER_ID)

    async def send_msg_message(self, msg, destination):
        """Sends a new message.MessageType.MSG message.

        Args:
            msg: the message string to be sent.
            destination: the destination of the message. Should be either a
                receiver id or 0 to broadcast to all receivers.
        Returns:
            The message that was sent.
        """
        message = message_types.new_message(
            type=message_types.MessageType.MSG,
            origin=self.origin,
            destination=destination,
            id=self._next_message_id,
            data=msg)

        await self._send_message(message)
        return message

    async def send_creq_message(self, destination):
        """Sends a new message.MessageType.CREQ message.

        Args:
            destination: receiver id of where the CLIST message should be sent
                to.
        Returns:
            The message that was sent.
        """
        message = message_types.new_message(
            type=message_types.MessageType.CREQ,
            origin=self.origin,
            destination=destination,
            id=self._next_message_id)

        await self._send_message(message)
        return message
