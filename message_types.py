"""Defines the message types and enums used to communicate."""

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

import enum
import collections
import struct

# Broadcast message destination.
BROADCAST_DESTINATION = 0

# Size of the header in bytes.
HEADER_SIZE = 8

# Size of an integer in the message in bytes.
SHORT_SIZE = 2

# Initial origin for new receivers.
NEW_RECEIVER_ORIGIN = 0

# First id for senders.
FIRST_SENDER_ID = 1

# One after last id for senders.
END_SENDER_ID = 4096

# First id for receivers.
FIRST_RECEIVER_ID = 4096

# One after last id for receivers.
END_RECEIVER_ID = 8192

# Server ID. This is predefined in the specification.
SERVER_ID = 65535

# Format of the header.
HEADER_FORMAT = struct.Struct('!4H')

# Format of one short.
SHORT_FORMAT = struct.Struct('!H')


class MessageType(enum.Enum):
    """Possible types of message."""

    # Message received successfully.
    ACK = 1

    # Error when receiving message.
    ERROR = 2

    # First message sent by a client, identifying itself.
    NEW_CLIENT = 3

    # Last message sent by a server/client, should be acknowledged.
    CLOSE = 4

    # Message with text sent by a sender.
    MSG = 5

    # Request to a server asking to send the list of connected clients.
    CREQ = 6

    # Message containing the list of connected clients.
    CLIST = 7


class Header(
        collections.namedtuple('Header',
                               ['type', 'origin', 'destination', 'id'])):
    """Represents the header of a message to be sent or received.

    For both origin and destination, the following rules should be followed for
    origin/destination IDs:
    - 65535 identifies the server.
    - From 1 to 4095 identifies a sender.
    - From 4096 to 8191 identifies a receiver.
    - On MessageType.NEW_CLIENT messages:
        - origin 0 means the new client is a receiver, and origin != 0 means the
            new client is a sender.
        - destination is always 65535 (server).
    - On MessageType.MSG messages:
        - destination 0 means broadcast the message to all receivers.
        - destination from 4096 to 8191 sends a message to a particular receiver.

    Fields:
        type: instance of MessageType, represents the type of the message.
        origin: a number from 0 to 65535, represents where the message came
            from.
        destination: a number from 0 to 65535, represents where the message
            should go to.
        id: a number from 0 to 65535, identifies a message sent from origin.
            Every new message sent from origin should have a new id. Ack
            messages (ACK or ERROR) should reply with the same id as the sent
            message.
    """


class Message(collections.namedtuple('Message', ['header', 'data'])):
    """Represents a message to be sent or received.

    Instead of building this message directly, use the new_message function.

    Fields:
        header: instance of Header, represents the header received with each
            message.
        data: on MessageType.MSG messages, is the string to be sent to the
            receiver. On MessageType.CLIST messages, is an array of integers,
            representing the ID of each client connected to the server. None for
            all other types of message.
    """


def new_message(type, origin, destination, id, data=None):
    """Constructs a new Message from the arguments and returns it."""
    return Message(header=Header(type, origin, destination, id), data=data)


def pack_message(message):
    """Packs the given Message to a binary format."""
    header = message.header
    data = message.data
    packed_header = HEADER_FORMAT.pack(header.type.value, header.origin,
                                       header.destination, header.id)

    if header.type == MessageType.MSG:
        encoded_data = data.encode()
        return packed_header + pack_short(len(encoded_data)) + encoded_data
    elif header.type == MessageType.CLIST:
        return packed_header + pack_short(len(data)) + pack_shorts(data)

    return packed_header


def unpack_header(packed_header):
    """Unpacks the given packed header to a Header namedtuple."""
    type_id, origin, destination, message_id = HEADER_FORMAT.unpack(
        packed_header)

    return Header(MessageType(type_id), origin, destination, message_id)


def pack_short(short):
    """Packs the given short."""
    return SHORT_FORMAT.pack(short)


def unpack_short(packed_short):
    """Unpacks the given short and returns only it."""
    short, = SHORT_FORMAT.unpack(packed_short)
    return short


def pack_shorts(shorts):
    """Packs the given shorts list."""
    print(shorts)
    return struct.pack('!{}H'.format(len(shorts)), *shorts)


def unpack_shorts(count, packed_shorts):
    """Unpacks the given count of shorts from packed_shorts as a list."""
    return list(struct.unpack('!{}H'.format(count), packed_shorts))
