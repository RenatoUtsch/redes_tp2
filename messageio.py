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
"""Implements the event loops that manage sending and receiving messages."""

import enum
import itertools
import logging
import queue
import select
import types

import message_types


class Handler:
    """Handles one resource in the event loop.

    This resource must have a fileno() function that returns the file descriptor
    of the resource being handled and an destination property that represents
    the id of the resource's destination.
    """

    def __init__(self, resource, coroutine):
        """Initializes the handler."""
        self._resource = resource
        self._coroutine = coroutine
        self._status = None
        self._forward_queue = queue.Queue()

    def fileno(self):
        """Returns the file descriptor of the resource."""
        return self._resource.fileno()

    @property
    def resource(self):
        """Returns the resource."""
        return self._resource

    @property
    def coroutine(self):
        """Returns the coroutine."""
        return self._coroutine

    @property
    def status(self):
        """Returns the wait status of the handler."""
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status of the handler."""
        self._status = status

    @property
    def waiting_for_forward(self):
        """Returns if the handler is waiting for forwards."""
        return self._status == _HandlerStatus.WAITING_FORWARD

    @property
    def forward_queue(self):
        """Returns the forward queue."""
        return self._forward_queue


class EventLoop:
    """Event loop that uses select to send/recv messages."""

    def __init__(self):
        """Initializes the event loop."""
        # Map from file descriptor to the handler of that resource. Handlers in
        # this dictionary are already initialized.
        self._handlers = {}

        # Map from destination id to the handler. Handlers registered in this
        # map can be used when forwarding messages.
        self._clients = {}

        # Next sender id.
        self._next_sender_id = message_types.FIRST_SENDER_ID

        # Next receiver id.
        self._next_receiver_id = message_types.FIRST_RECEIVER_ID

    @property
    def next_sender_id(self):
        """Returns the next sender id."""
        next_id = self._next_sender_id
        self._next_sender_id += 1

        assert next_id < message_types.END_SENDER_ID
        return next_id

    @property
    def next_receiver_id(self):
        """Returns the next receiver id."""
        next_id = self._next_receiver_id
        self._next_receiver_id += 1

        assert next_id < message_types.END_RECEIVER_ID
        return next_id

    def client_list(self):
        """Returns a list with all client origins."""
        return [
            handler.resource.destination for handler in self._clients.values()
        ]

    def client(self, destination):
        """Returns the resource of the client with the given destination."""
        return self._clients[destination]

    def add_resource(self, resource, coroutine):
        """Adds a new resource to be selected by the event loop.

        This resource must have a fileno() function that returns the file
        descriptor of the resource being handled and an destination property
        that represents the id of the resource's destination.

        This function will not immediately start the coroutine. Instead, it will
        be done when the event loop regains control of the flow and finishes
        processing the pending resources.

        Args:
            resource: the resource to register and start it's coroutine.
            coroutine: the coroutine responsible for managing the resource. When
                the coroutine ends, the resource is removed from the event loop.
        """
        fileno = resource.fileno()
        logging.info('New handler: %d', fileno)

        if fileno in self._handlers:
            raise ResourceAlreadyRegisteredError(resource)

        handler = Handler(resource, coroutine)

        try:
            handler.status = handler.coroutine.send(None)
        except StopIteration:  # Handler failed to start.
            logging.error('Handler failed to start: %d', fileno)
        except RuntimeError:
            logging.error('Error in handler: %d', fileno)

        self._handlers[fileno] = handler

    def add_client(self, resource):
        """Makes an existing resource available for forwarding messages.

        This is done by getting an already registered resource and saving it's
        destination. Messages can then be forwarded by the resource'
        destination.

        This function should only be called after the client's id has already
        been decided.

        The resource object will *not* be updated. This resource *must* be the
        same one that was registered by add_resource.

        Args:
            resource: the resource to be made available for forwarding. Must
                already have been registered by add_resource().
        """
        fileno = resource.fileno()
        if fileno not in self._handlers:
            raise ResourceNotRegisteredError(resource)

        handler = self._handlers[fileno]

        destination = handler.resource.destination
        logging.info('New client: %d', destination)
        if destination in self._clients:
            raise ClientAlreadyRegisteredError(resource)

        self._clients[destination] = handler

    def forward_msg(self, message):
        """Forwards the given message to the correct destination handlers."""
        if message.header.destination == message_types.BROADCAST_DESTINATION:
            for destination in self.client_list():
                self._clients[destination].forward_queue.put_nowait(message)
        else:
            self._clients[message.header.destination].forward_queue.put_nowait(
                message)

    def run_forever(self):
        """Runs forever, calling the registered handlers one after the other.

        Only stops if all handlers terminate.
        """
        # Handlers that should be deleted at the end of each iteration.
        handlers_to_delete = []

        while self._handlers:
            readable, writable, forwardable, exceptional = self._select()

            for handler in itertools.chain(readable, writable, forwardable):
                try:
                    # Resume coroutine, get return status for next wait.
                    handler.status = handler.coroutine.send(handler)
                    assert handler.status is not None
                except StopIteration:  # Handler finished.
                    handlers_to_delete.append(handler)
                except RuntimeError:  # Error in handler.
                    logging.error('Error in handler: %d', handler.fileno())
                    handlers_to_delete.append(handler)
                except AssertionError:  # Invalid handler.status
                    logging.exception('Invalid handler.status (None)')
                    handlers_to_delete.append(handler)

            for handler in exceptional:
                logging.error('Exceptional handler: %d; removing it.',
                              handler.resource.destination)
                handlers_to_delete.append(handler)

            if handlers_to_delete:
                self._delete_handlers(handlers_to_delete)
                handlers_to_delete = []

    def _select(self):
        """Selects the handlers available for continuation."""
        waiting_read = list(self._handlers_waiting_for_read())
        waiting_write = list(self._handlers_waiting_for_write())
        if waiting_read or waiting_write:
            readable, writable, exceptional = select.select(
                waiting_read, waiting_write, waiting_read + waiting_write)

        forwardable = [
            handler for handler in self._handlers.values()
            if handler.waiting_for_forward and
            not handler.forward_queue.empty()
        ]

        return readable, writable, forwardable, exceptional

    def _delete_handlers(self, handlers):
        """Deletes the given handlers."""
        for handler in handlers:
            fileno = handler.fileno()
            logging.info('Bye bye handler: %d', fileno)
            if fileno in self._handlers:
                del self._handlers[fileno]

            destination = handler.resource.destination
            if destination in self._clients:
                logging.info('Bye bye client: %d', destination)
                del self._clients[destination]

    def _handlers_waiting_for_read(self):
        """Generator that yields the handlers that are waiting for read."""
        for handler in self._handlers.values():
            if handler.status == _HandlerStatus.WAITING_READ:
                yield handler

    def _handlers_waiting_for_write(self):
        """Generator that yields the handlers that are waiting for write."""
        for handler in self._handlers.values():
            if handler.status == _HandlerStatus.WAITING_WRITE:
                yield handler


@types.coroutine
def wait_for_read():
    """Returns control to the event loop until handler is available for read."""
    yield _HandlerStatus.WAITING_READ


@types.coroutine
def wait_for_write():
    """Returns control to the event loop until buffer is available for write."""
    yield _HandlerStatus.WAITING_WRITE


@types.coroutine
def wait_for_forward():
    """Returns control to the event loop until buffer is available for forward.

    Returns the forwarded message.
    """
    handler = yield _HandlerStatus.WAITING_FORWARD
    assert not handler.forward_queue.empty()
    return handler.forward_queue.get()


class _HandlerStatus(enum.Enum):
    """Represents the current status of the handler."""

    # Waiting for new reads.
    WAITING_READ = 1

    # Waiting for new writes.
    WAITING_WRITE = 2

    # Waiting for server forwards.
    WAITING_FORWARD = 3


class Error(Exception):
    """Errors raised by this module."""


class ResourceError(Error):
    """Error that logs the resource."""

    def __init__(self, resource):
        """Prints the resource information.."""
        super().__init__('resource fd: {} | destination: {}'.format(
            resource.fileno(), resource.destination))


class ResourceNotRegisteredError(ResourceError):
    """Raised when the resource is not registered by the event loop."""


class ResourceAlreadyRegisteredError(ResourceError):
    """Raised when the resource is already registered by the event loop."""


class ClientAlreadyRegisteredError(ResourceError):
    """Raised when a client for the resource is already registered."""
