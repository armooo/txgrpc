from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import struct
import time

from h2 import events
from h2.connection import H2Connection
from h2.errors import ErrorCodes
from twisted.internet import defer
from twisted.internet import protocol

from txgrpc.errors import GRPCError


MESSAGE_HEADER = struct.Struct('!?L')


class UnaryResult(object):
    def __init__(self, defered):
        self._defered = defered

    def message_received(self, message):
        self._message = message

    def connection_lost(self, headers, reason):
        if reason:
            self._defered.errback(reason)
        else:
            self._defered.callback(self._message)


class StreamingResult(object):
    def __init__(self):
        self._messages = []
        self._defered = None
        self._error = None
        self._finished = False
        self._defered = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._messages:
            return defer.succeed(self._messages.pop(0))
        elif self._error:
            error = self._error
            self._error = None
            return defer.fail(error)
        elif self._finished:
            # Should we make sure to always have a Deferred with a None before
            # the StopIteration? Currently it depends if you call next before
            # or after connection_lost is called.
            raise StopIteration
        elif not self._defered:
            self._defered = defer.Deferred()
            self._defered.addBoth(self._clear_defered)
            return self._defered
        else:
            raise GRPCError('You can not see the future')

    next = __next__

    def _clear_defered(self, result):
        self._defered = None
        return result

    def message_received(self, message):
        if self._defered:
            self._defered.callback(message)
        else:
            self._messages.append(message)

    def connection_lost(self, headers, reason):
        self._finished = True
        if self._defered:
            if reason:
                self._defered.errback(reason)
            else:
                self._defered.callback(None)
        elif reason:
            self._error = reason


class GRPCResult(object):
    """
    Accumulates results for a single gRPC call

    Headers and data from a HTTP2 stream sent to the set_headers and add_data
    methods of this class. When each a message is fully received
    protocol.message_received is called with the bytes payload. When the stream
    is closed protocol.connection_lost(headers, reason) is called. If there was
    an error with the connection reason will be a subclass of GRPCError
    otherwise reason will be None.

    This works much like a LineReceiver.
    """
    def __init__(self, protocol, stats):
        self._protocol = protocol
        self._stats = stats
        self._headers = {}
        self._data = bytearray()
        self._start = time.time()
        self._msg_size = None
        self._done = False

    def set_headers(self, headers):
        self._headers.update(headers)

    def _error(self, error):
        self._stats.log_error(time.time() - self._start)
        self._protocol.connection_lost(self._headers, error)
        self._done = True

    def add_data(self, data):
        self._data.extend(data)

        while len(self._data) >= MESSAGE_HEADER.size:
            if self._msg_size is None:
                msg_header = self._data[:MESSAGE_HEADER.size]
                compressed, self._msg_size = MESSAGE_HEADER.unpack(msg_header)
                del self._data[:MESSAGE_HEADER.size]
                # TODO support compressed messages
                if compressed:
                    self._error(GRPCError('Compression not supported'))
                    return

            if len(self._data) < self._msg_size:
                return

            self._protocol.message_received(self._data[:self._msg_size])
            del self._data[:self._msg_size]
            self._msg_size = None

    def _check_errors(self):
        if self._headers[':status'] != '200':
            return GRPCError(
                'Non-200 status code. %r' % (self._headers[':status'])
            )

        if self._headers['grpc-status'] != '0':
            return GRPCError('Non-0 grpc-status code. %r %r' % (
                self._headers['grpc-status'],
                self._headers['grpc-message'],
            ))

    def end(self):
        if self._done:
            return
        error = self._check_errors()
        if error is not None:
            self._error(error)
        elif self._data:
            self._error(GRPCError('Extra data in stream'))
        else:
            self._stats.log_success(time.time() - self._start)
            self._protocol.connection_lost(self._headers, None)

    def reset(self):
        error = GRPCError('Stream reset')
        self._stats.log_error(time.time() - self._start)
        self._protocol.connection_lost(self._headers, error)


class GRPCClientProtocol(protocol.Protocol):
    """
    Manages a HTTP2 connection to a gRPC server

    Mutable requests can be in progress over a single connection. The protocol
    keeps track of a HTTP2 stream_id to GRPCResult instances.
    """
    def __init__(self, clock, authority, stats):
        self._clock = clock
        self._conn = H2Connection()
        self._authority = authority
        self._pending_results = {}
        self._stats = stats

    def connectionMade(self):
        self._conn.initiate_connection()
        self.transport.write(self._conn.data_to_send())

    def dataReceived(self, data):
        for event in self._conn.receive_data(data):
            if isinstance(event, events.ResponseReceived):
                self.h2_response_received(event)
            elif isinstance(event, events.DataReceived):
                self.h2_data_received(event)
            elif isinstance(event, events.TrailersReceived):
                self.h2_trailers_received(event)
            elif isinstance(event, events.StreamEnded):
                self.h2_stream_ended(event)
            elif isinstance(event, events.StreamReset):
                self.h2_stream_reset(event)

        data = self._conn.data_to_send()
        if data:
            self.transport.write(data)

    def h2_response_received(self, event):
        self._pending_results[event.stream_id].set_headers(event.headers)

    def h2_data_received(self, event):
        self._pending_results[event.stream_id].add_data(event.data)
        self._conn.acknowledge_received_data(
            event.flow_controlled_length,
            event.stream_id,
        )

    def h2_trailers_received(self, event):
        self._pending_results[event.stream_id].set_headers(event.headers)

    def h2_stream_ended(self, event):
        pending_result = self._pending_results.pop(event.stream_id)
        pending_result.end()

    def h2_stream_reset(self, event):
        pending_result = self._pending_results.pop(event.stream_id)
        pending_result.reset()

    def _convert_timeout(self, timeout):
        # TODO support more units
        return '{}m'.format(int(timeout * 1000))

    def _canceller(self, stream_id, defered):
        self._conn.reset_stream(stream_id, ErrorCodes.CANCEL)
        del self._pending_results[stream_id]

    def _call_unary_rpc(self, stream_id, method, data, timeout, result):
        headers = (
            (':method', 'POST'),
            (':scheme', 'http'),
            (':path', method),
            (':authority', self._authority),
            ('grpc-timeout', self._convert_timeout(timeout)),
            ('te', 'trailers'),
            ('content-type', 'application/grpc+proto'),
        )
        self._conn.send_headers(stream_id, headers)
        self._conn.send_data(stream_id, MESSAGE_HEADER.pack(False, len(data)))
        self._conn.send_data(stream_id, data, end_stream=True)
        self.transport.write(self._conn.data_to_send())
        self._pending_results[stream_id] = GRPCResult(
            result,
            self._stats,
        )

    def call_rpc(self, method, data, timeout):
        # TODO: fail on max id
        stream_id = self._conn.get_next_available_stream_id()

        # Push the timeout by 50ms to allow the server to send a reset first
        # and allow for a bit of latency.
        # TODO: tune this with pings
        timeout = timeout + .05

        defered = defer.Deferred(functools.partial(self._canceller, stream_id))
        defered.addTimeout(timeout, self._clock)
        result = UnaryResult(defered)

        self._call_unary_rpc(stream_id, method, data, timeout, result)

        return defered

    def call_unary_streaming_rpc(self, method, data, timeout):
        # TODO: fail on max id
        stream_id = self._conn.get_next_available_stream_id()

        # Push the timeout by 50ms to allow the server to send a reset first
        # and allow for a bit of latency.
        # TODO: tune this with pings
        timeout = timeout + .05

        result = StreamingResult()
        self._call_unary_rpc(stream_id, method, data, timeout, result)
        return result
