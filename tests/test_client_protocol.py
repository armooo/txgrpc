from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from h2.connection import H2Connection
from twisted.internet import task
from twisted.internet.defer import TimeoutError
from twisted.test import proto_helpers
import h2.events
import pytest

from txgrpc.errors import GRPCError
from txgrpc.client_protocol import GRPCClientProtocol
from txgrpc.stats import LocalStats


@pytest.fixture
def transport():
    return proto_helpers.StringTransportWithDisconnection()


@pytest.fixture
def clock():
    return task.Clock()


@pytest.fixture
def stats():
    return LocalStats()


@pytest.fixture
def proto(transport, clock, stats):
    proto = GRPCClientProtocol(clock, 'localhost:8080', stats)
    transport.protocol = proto
    proto.makeConnection(transport)
    return proto


def next_event(events, event_type):
    while events:
        e = events.pop(0)
        if isinstance(e, event_type):
            return e, events
    return None, events


def collect_data(events, stream_id):
    data = []
    while events and isinstance(events[0], h2.events.DataReceived):
        e = events.pop(0)
        data.append(e.data)
    return b''.join(data), events


@pytest.inlineCallbacks
def test_rpc_success(transport, proto):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()

    req, events = next_event(events, h2.events.RequestReceived)
    data, events = collect_data(events, req.stream_id)
    end, events = next_event(events, h2.events.StreamEnded)

    assert req.headers[0] == (':method', 'POST')
    assert req.headers[1] == (':scheme', 'http')
    assert req.headers[2] == (':path', 'foo')
    assert req.headers[3] == (':authority', 'localhost:8080')
    assert req.headers[4] == ('grpc-timeout', '5050m')
    assert req.headers[5] == ('te', 'trailers')
    assert req.headers[6] == ('content-type', 'application/grpc+proto')

    # No compression
    assert data[0:1] == b'\x00'
    # Length of 'payload' network byte order
    assert data[1:5] == b'\x00\x00\x00\x07'
    assert data[5:] == b'payload'

    assert end.stream_id == req.stream_id

    conn.send_headers(req.stream_id, (
        (':status', '200'),
        ('grpc-status', '0'),
        ('grpc-message', 'OK'),
    ))

    # No compression
    conn.send_data(req.stream_id, b'\x00')
    # Length of 'result' network byte order
    conn.send_data(req.stream_id, b'\x00\x00\x00\x06')
    conn.send_data(req.stream_id, b'result', end_stream=True)

    proto.dataReceived(conn.data_to_send())

    assert b'result' == (yield d)


@pytest.inlineCallbacks
def test_timeout(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    clock.advance(6)

    with pytest.raises(TimeoutError):
        yield d


@pytest.inlineCallbacks
def test_compression(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()
    req, events = next_event(events, h2.events.RequestReceived)

    conn.send_headers(req.stream_id, (
        (':status', '200'),
    ))
    # Compression
    conn.send_data(req.stream_id, b'\x01')
    # Length of 'result' network byte order
    conn.send_data(req.stream_id, b'\x00\x00\x00\x06')
    conn.send_data(req.stream_id, b'result')
    conn.send_headers(
        req.stream_id,
        (
            ('grpc-status', '0'),
            ('grpc-message', 'OK'),
        ),
        end_stream=True
    )

    proto.dataReceived(conn.data_to_send())

    with pytest.raises(GRPCError) as excinfo:
        yield d
    assert str(excinfo.value) == 'Compression not suported'


@pytest.inlineCallbacks
def test_size_too_small(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()
    req, events = next_event(events, h2.events.RequestReceived)

    conn.send_headers(req.stream_id, (
        (':status', '200'),
        ('grpc-status', '0'),
        ('grpc-message', 'OK'),
    ))
    # Compression
    conn.send_data(req.stream_id, b'\x00')
    # Length of 'result' network byte order
    conn.send_data(req.stream_id, b'\x00\x00\x00\x04')
    conn.send_data(req.stream_id, b'result', end_stream=True)

    proto.dataReceived(conn.data_to_send())

    with pytest.raises(GRPCError) as excinfo:
        yield d
    assert str(excinfo.value) == 'Result the wrong size'


@pytest.inlineCallbacks
def test_size_too_large(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()
    req, events = next_event(events, h2.events.RequestReceived)

    conn.send_headers(req.stream_id, (
        (':status', '200'),
        ('grpc-status', '0'),
        ('grpc-message', 'OK'),
    ))
    # Compression
    conn.send_data(req.stream_id, b'\x00')
    # Length of 'result' network byte order
    conn.send_data(req.stream_id, b'\x00\x00\x00\x09')
    conn.send_data(req.stream_id, b'result', end_stream=True)

    proto.dataReceived(conn.data_to_send())

    with pytest.raises(GRPCError) as excinfo:
        yield d
    assert str(excinfo.value) == 'Result the wrong size'


@pytest.inlineCallbacks
def test_http_error(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()
    req, events = next_event(events, h2.events.RequestReceived)

    conn.send_headers(req.stream_id, (
        (':status', '500'),
    ), end_stream=True)

    proto.dataReceived(conn.data_to_send())

    with pytest.raises(GRPCError) as excinfo:
        yield d
    value = str(excinfo.value).replace('u\'', '\'')
    assert value == 'Non-200 status code. \'500\''


@pytest.inlineCallbacks
def test_http_reset(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()
    req, events = next_event(events, h2.events.RequestReceived)

    conn.reset_stream(req.stream_id)

    proto.dataReceived(conn.data_to_send())

    with pytest.raises(GRPCError) as excinfo:
        yield d
    assert str(excinfo.value) == 'Stream reset'


@pytest.inlineCallbacks
def test_grpc_error(transport, proto, clock):
    d = proto.call_rpc('foo', b'payload', 5)
    conn = H2Connection(client_side=False)
    conn.initiate_connection()
    proto.dataReceived(conn.data_to_send())

    events = conn.receive_data(transport.value())
    transport.clear()
    req, events = next_event(events, h2.events.RequestReceived)

    conn.send_headers(req.stream_id, (
        (':status', '200'),
    ))
    conn.send_headers(
        req.stream_id,
        (
            ('grpc-status', '1'),
            ('grpc-message', 'I am sad'),
        ),
        end_stream=True
    )

    proto.dataReceived(conn.data_to_send())

    with pytest.raises(GRPCError) as excinfo:
        yield d
    value = str(excinfo.value).replace('u\'', '\'')
    assert value == 'Non-0 grpc-status code. \'1\' \'I am sad\''
