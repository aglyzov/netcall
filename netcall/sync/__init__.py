# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from time import time

import zmq

from ..base_client import RPCClientBase
from ..errors      import RPCTimeoutError
from ..utils       import get_zmq_classes


#-----------------------------------------------------------------------------
# Synchronous RPC Client
#-----------------------------------------------------------------------------

class SyncRPCClient(RPCClientBase):  #{
    """A synchronous RPC client (blocking, not thread-safe)"""

    def __init__(self, context=None, **kwargs):  #{
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

        super(SyncRPCClient, self).__init__(**kwargs)
    #}
    
    def _get_tools(self):  #{
        "Returns a tuple (Event, Queue, Future, TimeoutError)"
        pass # Not needed in this implementation
    #}
    
    def call(self, proc_name, args=[], kwargs={}, ignore=False, timeout=None):  #{
        """
        Call the remote method with *args and **kwargs
        (may raise exception)

        Parameters
        ----------
        proc_name : <bytes> name of the remote procedure to call
        args      : <tuple> positional arguments of the remote procedure
        kwargs    : <dict>  keyword arguments of the remote procedure
        timeout   : <float> | None
            Number of seconds to wait for a reply.
            RPCTimeoutError will be raised if no reply is received in time.
            Set to None, 0 or a negative number to disable.

        Returns
        -------
        <object>
            If the call succeeds, the result of the call will be returned.
            If the call fails, `RemoteRPCError` will be raised.
        """
        if not (timeout is None or isinstance(timeout, (int, float))):
            raise TypeError("timeout param: <float> or None expected, got %r" % timeout)

        if not self._ready:
            raise RuntimeError('bind or connect must be called first')

        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)

        self.socket.send_multipart(msg_list)

        if timeout and timeout > 0:
            poller = zmq.Poller()
            poller.register(self.socket, zmq.POLLIN)
            start_t    = time()
            deadline_t = start_t + timeout

            def recv_multipart():
                timeout_ms = int((deadline_t - time())*1000)  # in milliseconds
                #logger.debug('polling with timeout_ms=%s' % timeout_ms)
                if timeout_ms > 0 and poller.poll(timeout_ms):
                    msg = self.socket.recv_multipart()
                    return msg
                else:
                    raise RPCTimeoutError("Request %s timed out after %s sec" % (req_id, timeout))
        else:
            recv_multipart = self.socket.recv_multipart

        def recv_yielder():  #{
            while True:
                msg_list = recv_multipart()
                self.logger.debug('received %r' % msg_list)
                reply = self._parse_reply(msg_list)

                if reply is None \
                or reply['req_id'] != req_id:
                      continue

                if reply['type'] == b'ACK':
                    if ignore:
                        yield b'OK', None
                        return
                    else:
                        continue

                if reply['type'] == b'FAIL':
                    raise reply['result']

                yield reply['type'], reply['result']
                if reply['type'] == b'OK':
                    return
        #}

        recv_gen = recv_yielder()
        reply_type, result = next(recv_gen)

        if reply_type == b'OK':
            return result
        else:
            return self._yielder(recv_gen, req_id)
    #}
#}

