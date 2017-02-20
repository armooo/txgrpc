from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from mock import Mock, sentinel
import pytest

from txgrpc.client import GRPCClient
from txgrpc.errors import GRPCError


@pytest.inlineCallbacks
def test_no_endpoints():
    client = GRPCClient(None)
    with pytest.raises(GRPCError) as exc_info:
        yield client.call_rpc('foo', 'proto', None, 5)
    assert str(exc_info.value) == 'No servers configured'


@pytest.inlineCallbacks
def test_call_rpc_pass_in():
    protocol = Mock(name='protocol', spec=['call_rpc'])
    ep = Mock(name='endpoint', spec=['connect'])
    ep.connect.return_value = protocol

    req_proto = Mock(name='proto', spec=['SerializeToString'])
    req_proto.SerializeToString.return_value = sentinel.req_proto_bytes

    client = GRPCClient(sentinel.reactor, [ep])

    yield client.call_rpc(
        method=sentinel.method,
        proto=req_proto,
        result_cls=Mock(name='result_cls'),
        timeout=sentinel.timeout,
    )

    protocol.call_rpc.assert_called_once_with(
        method=sentinel.method,
        data=sentinel.req_proto_bytes,
        timeout=sentinel.timeout,
    )


@pytest.inlineCallbacks
def test_call_rpc_return():
    protocol = Mock(name='protocol', spec=['call_rpc'])
    protocol.call_rpc.return_value = sentinel.result_bytes
    ep = Mock(name='endpoint', spec=['connect'])
    ep.connect.return_value = protocol

    proto = Mock(name='proto', spec=['ParseFromString'])
    proto.ParseFromString.return_value = None
    proto_cls = Mock(name='proto_cls', return_value=proto)

    client = GRPCClient(sentinel.reactor, [ep])

    result = yield client.call_rpc(
        method=sentinel.method,
        proto=Mock(name='proto'),
        result_cls=proto_cls,
        timeout=sentinel.timeout,
    )

    assert result == proto
    proto.ParseFromString.assert_called_once_with(sentinel.result_bytes)


@pytest.inlineCallbacks
def test_connect_once():
    protocol = Mock(name='protocol', spec=['call_rpc'])
    protocol.call_rpc.return_value = sentinel.result_bytes
    ep = Mock(name='endpoint', spec=['connect'])

    client = GRPCClient(sentinel.reactor, [ep])

    yield client.call_rpc(
        method=sentinel.method,
        proto=Mock(name='proto'),
        result_cls=Mock(name='result_cls'),
        timeout=sentinel.timeout,
    )
    yield client.call_rpc(
        method=sentinel.method,
        proto=Mock(name='proto'),
        result_cls=Mock(name='result_cls'),
        timeout=sentinel.timeout,
    )

    assert ep.connect.call_count == 1


@pytest.inlineCallbacks
def test_connect_args():
    protocol_factory = Mock(name='protocol_factory')

    client = GRPCClient(sentinel.reactor, [Mock()], sentinel.stats)
    client.PROTOCOL_FACTORY = protocol_factory

    yield client.call_rpc(
        method=sentinel.method,
        proto=Mock(name='proto'),
        result_cls=Mock(name='result_cls'),
        timeout=sentinel.timeout,
    )

    protocol_factory.assert_called_once_with(
        clock=sentinel.reactor,
        authority='localhost:1234',
        stats=sentinel.stats,
    )
