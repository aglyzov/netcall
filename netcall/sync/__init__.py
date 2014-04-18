# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

from time import time

import zmq

from ..base_client import RPCClientBase
from ..errors      import RPCTimeoutError
from ..utils       import get_zmq_classes


#-----------------------------------------------------------------------------
# Synchronous RPC Client
#-----------------------------------------------------------------------------

class SyncRPCClient(RPCClientBase):
    """A synchronous RPC client (blocking, not thread-safe)"""

    def __init__(self, context=None, **kwargs):
        """
        Parameters
        ==========
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

        self._gen_queues = None  # for compatibility with tests

        super(SyncRPCClient, self).__init__(**kwargs)

    def call(self, proc_name, args=[], kwargs={}, result='sync', timeout=None):
        """
        Call the remote method with *args and **kwargs
        (may raise exception)

        Parameters
        ----------
        proc_name : <bytes> name of the remote procedure to call
        args      : <tuple> positional arguments of the remote procedure
        kwargs    : <dict>  keyword arguments of the remote procedure
        result    : 'sync' | 'ignore'
        timeout   : <float> | None
            Number of seconds to wait for a reply.
            RPCTimeoutError will be raised if no reply is received in time.
            Set to None, 0 or a negative number to disable.

        Returns
        -------
        <result:object> if result is 'sync'
        None            if result is 'ignore'

        If remote call fails:
        - raises <RemoteRPCError> if result is 'sync'
        """
        assert result in ('sync', 'ignore'), \
            'expected any of "sync", "ignore" -- got %r' % result

        if not (timeout is None or isinstance(timeout, (int, float))):
            raise TypeError("timeout param: <float> or None expected, got %r" % timeout)

        if not self._ready:
            raise RuntimeError('bind or connect must be called first')

        ignore = result == 'ignore'
        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)

        self._send_request(msg_list)

        if timeout and timeout > 0:
            poller = zmq.Poller()
            poller.register(self.socket, zmq.POLLIN)
            start_t    = time()
            deadline_t = start_t + timeout

            def recv_multipart():
                timeout_ms = int((deadline_t - time())*1000)  # in milliseconds
                #logger.debug('polling with timeout_ms=%s', timeout_ms)
                if timeout_ms > 0 and poller.poll(timeout_ms):
                    return self.socket.recv_multipart()
                else:
                    raise RPCTimeoutError("Request %s timed out after %s sec" % (req_id, timeout))
        else:
            recv_multipart = self.socket.recv_multipart

        def get_result_pair(recv=recv_multipart, first=False):
            logger = self.logger
            while True:
                msg_list = recv()
                logger.debug('received %r', msg_list)
                reply = self._parse_reply(msg_list)

                if reply is None \
                or reply['req_id'] != req_id:
                      continue

                msg_type = reply['type']

                if msg_type == b'ACK':
                    if ignore:
                        return None, None
                    else:
                        continue

                result = reply['result']
                recv = self.socket.recv_multipart

                if msg_type == b'OK':
                    return result, None

                elif msg_type == b'FAIL':
                    return None, result

                elif msg_type == b'YIELD':
                    if first:
                        return self._generator(req_id, get_result_pair), None
                    else:
                        return result, None

        res, exc = get_result_pair(first=True)
        if exc is None:
            return res
        else:
            raise exc

