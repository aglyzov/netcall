# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Client classes to talk to a NetCall service.

Authors:

* Brian Granger
* Alexander Glyzov
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop    import IOLoop, DelayedCallback

from tornado.concurrent import Future

from ..base_client import RPCClientBase
from ..utils       import RemoteMethodBase, get_zmq_classes
from ..errors      import RPCTimeoutError


#-----------------------------------------------------------------------------
# Tornado RPC Client
#-----------------------------------------------------------------------------
class TornadoRPCClient(RPCClientBase):  #{
    """An asynchronous service proxy (based on Tornado IOLoop)"""

    def __init__(self, context=None, ioloop=None, **kwargs):  #{
        """
        Parameters
        ==========
        ioloop : IOLoop
            An existing IOLoop instance, if not passed, zmq.IOLoop.instance()
            will be used.
        context : Context
            An existing Context instance, if not passed, zmq.Context.instance()
            will be used.
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        Context, _ = get_zmq_classes()

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        self.ioloop   = IOLoop.instance() if ioloop is None else ioloop
        self._futures = {}  # {<req_id> : <Future>}

        super(TornadoRPCClient, self).__init__(**kwargs)
    #}
    def _create_socket(self):  #{
        super(TornadoRPCClient, self)._create_socket()
        self.socket = ZMQStream(self.socket, self.ioloop)
        self.socket.on_recv(self._handle_reply)
    #}
    def _handle_reply(self, msg_list):  #{
        self.logger.debug('received %r', msg_list)
        reply = self._parse_reply(msg_list)

        if reply is None:
            return

        req_id   = reply['req_id']
        msg_type = reply['type']
        result   = reply['result']

        if msg_type == b'ACK':
            return

        future_tout = self._futures.pop(req_id, None)

        if future_tout is None:
            return

        future, tout_cb = future_tout

        # stop the timeout if there is one
        if tout_cb is not None:
            tout_cb.stop()

        if msg_type == b'OK':
            future.set_result(result)
        else:
            future.set_exception(result)
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def __getattr__(self, name):  #{
        return AsyncRemoteMethod(self, name)
    #}
    def call(self, proc_name, args=[], kwargs={}, result='async', timeout=None):  #{
        """
        Call the remote method with *args and **kwargs.

        Parameters
        ----------
        proc_name : <str>   name of the remote procedure to call
        args      : <tuple> positional arguments of the procedure
        kwargs    : <dict>  keyword arguments of the procedure
        ignore    : <bool>  whether to ignore result or wait for it
        timeout   : <float> | None
            Number of seconds to wait for a reply.
            RPCTimeoutError is set as the future result in case of timeout.
            Set to None, 0 or a negative number to disable.

        Returns
        -------
        <Future> if result is 'async'
        None     if result is 'ignore'

        If remote call fails:
        - sets <RemoteRPCError> into the <Future> if result is 'async'
        """
        assert result in ('async', 'ignore'), \
            'expected any of "async", "ignore" -- got %r' % result

        if not (timeout is None or isinstance(timeout, (int, float))):
            raise TypeError("timeout param: <float> or None expected, got %r" % timeout)

        ignore = result == 'ignore'
        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)
        self.socket.send_multipart(msg_list)

        if ignore:
            return None

        # The following logic assumes that the reply won't come back too
        # quickly, otherwise the callbacks won't be in place in time. It should
        # be fine as this code should run very fast. This approach improves
        # latency we send the request ASAP.
        def _abort_request():
            future_tout = self._futures.pop(req_id, None)
            if future_tout:
                future, _ = future_tout
                tout_msg  = "Request %s timed out after %s sec" % (req_id, timeout)
                #self.logger.debug(tout_msg)
                future.set_exception(RPCTimeoutError(tout_msg))

        timeout = timeout or 0

        if timeout > 0:
            tout_cb = DelayedCallback(_abort_request, int(timeout*1000), self.ioloop)
            tout_cb.start()
        else:
            tout_cb = None

        future = Future()
        self._futures[req_id] = (future, tout_cb)

        return future
    #}
#}

class AsyncRemoteMethod(RemoteMethodBase):  #{

    def __call__(self, callback, *args, **kwargs):
        return self.client.call(self.method, args, kwargs, callback=callback)
#}
