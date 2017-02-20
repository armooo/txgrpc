from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from twisted.internet import defer
from twisted.internet import endpoints

from txgrpc.stats import NullStats
from txgrpc.client_protocol import GRPCClientProtocol
from txgrpc.errors import GRPCError


class GRPCClient(object):
    PROTOCOL_FACTORY = GRPCClientProtocol

    def __init__(self, reactor, endpoints=[], stats=None):
        self._reactor = reactor
        self._endpoints = endpoints
        self._stats = stats or NullStats()
        self._protocol = None
        self._lock = defer.DeferredLock()

    @defer.inlineCallbacks
    def _get_protocol(self):
        # TODO: something smart to kickout bad servers and use ones with lower
        # pings
        if not self._endpoints:
            raise GRPCError('No servers configured')
        yield self._lock.acquire()
        if not self._protocol:
            self._protocol = yield endpoints.connectProtocol(
                self._endpoints[0],
                self.PROTOCOL_FACTORY(
                    clock=self._reactor,
                    authority='localhost:1234',
                    stats=self._stats,
                ),
            )
        self._lock.release()
        defer.returnValue(self._protocol)

    @defer.inlineCallbacks
    def call_rpc(self, method, proto, result_cls, timeout):
        protocol = yield self._get_protocol()
        result = yield protocol.call_rpc(
            method=method,
            data=proto.SerializeToString(),
            timeout=timeout,
        )
        proto = result_cls()
        proto.ParseFromString(result)
        defer.returnValue(proto)
