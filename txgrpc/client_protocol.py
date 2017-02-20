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


class GRPCUnaryResult(object):
    def __init__(self, defered, stats):
        self._defered = defered
        self._stats = stats
        self._headers = {}
        self._data = []
        self._start = time.time()

    def set_headers(self, headers):
        self._headers.update(headers)

    def add_data(self, data):
        self._data.append(data)

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
        error = self._check_errors()
        if error is not None:
            self._stats.log_error(time.time() - self._start)
            self._defered.errback(error)
        else:
            data = ''.join(self._data)
            msg_header = data[:MESSAGE_HEADER.size]
            compressed, msg_len = MESSAGE_HEADER.unpack(msg_header)
            # TODO support compressed messages
            if compressed:
                self._defered.errback(GRPCError('Compression not suported'))
                return
            if msg_len + MESSAGE_HEADER.size != len(data):
                self._defered.errback(GRPCError('Result the wrong size'))
                return
            msg = data[MESSAGE_HEADER.size:]
            self._stats.log_success(time.time() - self._start)
            self._defered.callback(msg)

    def reset(self):
        error = GRPCError('Stream reset')
        self._stats.log_error(time.time() - self._start)
        self._defered.errback(error)


class GRPCClientProtocol(protocol.Protocol):
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
        pending__result = self._pending_results.pop(event.stream_id)
        pending__result.end()

    def h2_stream_reset(self, event):
        pending__result = self._pending_results.pop(event.stream_id)
        pending__result.reset()

    def _convert_timeout(self, timeout):
        # TODO support more units
        return '{}m'.format(int(timeout * 1000))

    def _canceller(self, stream_id, defered):
        self._conn.reset_stream(stream_id, ErrorCodes.CANCEL)
        del self._pending_results[stream_id]

    def call_rpc(self, method, data, timeout):
        # Push the timeout by 50ms to allow the server to send a reset first
        # and allow for a bit of latency.
        # TODO: tune this with pings
        timeout = timeout + .05

        # TODO: fail on max id
        stream_id = self._conn.get_next_available_stream_id()

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

        defered = defer.Deferred(functools.partial(self._canceller, stream_id))
        defered.addTimeout(timeout, self._clock)
        self._pending_results[stream_id] = GRPCUnaryResult(
            defered,
            self._stats,
        )
        return defered
