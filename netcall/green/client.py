# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Gevent version of the RPC client

Authors:

* Brian Granger
* Alexander Glyzov
* Axel Voitier

"""
#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov,
#  Axel Voitier
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from gevent         import spawn, spawn_later
from gevent.event   import Event, AsyncResult
from gevent.queue   import Queue
from gevent.timeout import Timeout

from ..base   import RPCClientBase
from ..utils  import get_zmq_classes
from ..errors import RPCTimeoutError


#-----------------------------------------------------------------------------
# Gevent RPC client
#-----------------------------------------------------------------------------

class GeventRPCClient(RPCClientBase):  #{
    """ An asynchronous service proxy whose requests will not block.
        Uses the Gevent compatibility layer of pyzmq (zmq.green).
    """

    def __init__(self, context=None, **kwargs):  #{
        """
        Parameters
        ==========
        context : Context
            An existing Context instance, if not passed, green.Context.instance()
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

        super(GeventRPCClient, self).__init__(**kwargs)  # base class

        self._ready_ev = Event()
        self._exit_ev  = Event()
        self.greenlet  = spawn(self._reader)
        self._results  = {}  # {<msg-id> : <_ReturnOrYieldResult>}
    #}
    def _create_socket(self):  #{
        super(GeventRPCClient, self)._create_socket()
    #}
    def bind(self, *args, **kwargs):  #{
        result = super(GeventRPCClient, self).bind(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def bind_ports(self, *args, **kwargs):  #{
        result = super(GeventRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def connect(self, *args, **kwargs):  #{
        result = super(GeventRPCClient, self).connect(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def _reader(self):  #{
        """ Reader greenlet

            Waits for a socket to become ready (._ready_ev), then reads incoming replies and
            fills matching async results thus passing control to waiting greenlets (see .call)
        """
        ready_ev = self._ready_ev
        exit_ev  = self._exit_ev
        socket   = self.socket
        results  = self._results
        running  = True
        logger   = self.logger

        while running:
            ready_ev.wait()  # block until socket is bound/connected
            self._ready_ev.clear()

            while self._ready:
                try:
                    msg_list = socket.recv_multipart()
                except:
                    # the socket must have been closed
                    #logger.warning("socket error", exc_info=True)
                    break

                logger.debug('received: %r' % msg_list)

                reply = self._parse_reply(msg_list)

                if reply is None:
                    logger.debug('skipping invalid reply')
                    continue

                req_id   = reply['req_id']
                msg_type = reply['type']
                result   = reply['result']

                if msg_type == b'ACK':
                    logger.debug('skipping ACK, req_id=%r' % req_id)
                    continue

                if msg_type == b'YIELD':
                    results_get_fn = results.get
                    async_init_fn = _ReturnOrYieldResult.init_as_yield
                else: # For OK and FAIL
                    results_get_fn = results.pop
                    async_init_fn = _ReturnOrYieldResult.init_as_return

                async = results_get_fn(req_id, None)
                if async is None:
                    # result is gone, must be a timeout
                    logger.debug('async result is gone (timeout?): req_id=%r' % req_id)
                    continue
                if not async.is_init():
                    async_init_fn(async)

                if msg_type in [b'OK', b'YIELD']:
                    logger.debug('async.set(result), req_id=%r' % req_id)
                    async.set(result)
                else:
                    logger.debug('async.set_exception(result), req_id=%r' % req_id)
                    async.set_exception(result)

            if exit_ev.is_set():
                logger.debug('_reader received an EXIT signal')
                break

        logger.debug('_reader exited')
    #}
    def shutdown(self):  #{
        """Close the socket and signal the reader greenlet to exit"""
        self.logger.debug('closing the socket')
        self._ready = False
        self._exit_ev.set()
        self._ready_ev.set()
        self.socket.close(0)
        self.greenlet.join()
        self.greenlet = None
        self._ready_ev.clear()
        self._exit_ev.clear()
    #}
    def call(self, proc_name, args=[], kwargs={}, ignore=False, timeout=None):  #{
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
        <object>
            If the call succeeds, the result of the call will be returned.
            If the call fails, `RemoteRPCError` will be raised.
        """
        if not (timeout is None or isinstance(timeout, (int, float))):
            raise TypeError("timeout param: <float> or None expected, got %r" % timeout)

        if not self._ready:
            raise RuntimeError('bind or connect must be called first')

        logger = self.logger

        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)

        logger.debug('send: %r' % msg_list)
        self.socket.send_multipart(msg_list)

        if ignore:
            return None

        if timeout and timeout > 0:
            def _abort_request():
                result = self._results.pop(req_id, None)
                if result:
                    tout_msg  = "Request %s timed out after %s sec" % (req_id, timeout)
                    logger.debug(tout_msg)
                    result.set_exception(RPCTimeoutError(tout_msg))
            spawn_later(timeout, _abort_request)

        result = _ReturnOrYieldResult(self, req_id)
        self._results[req_id] = result
        logger.debug('waiting for result=%r' % result)
        return result.get()  # block waiting for a reply passed by ._reader
    #}
#}

class _ReturnOrYieldResult(object):  #{

    def __init__(self, client, req_id):
        self.is_initialized = Event()
        self.return_or_except = AsyncResult()
        self.yield_queue = Queue(1)
        self.client = client
        self.req_id = req_id

    def init_as_return(self):
        assert(not self.is_init())
        self.is_yield = False
        self.is_initialized.set()

    def init_as_yield(self):
        assert(not self.is_init())
        self.is_yield = True
        self.is_initialized.set()

    def is_init(self):
        return self.is_initialized.is_set()

    def set(self, obj):
        assert(self.is_init())
        if self.is_yield:
            self.yield_queue.put(obj)
        else:
            self.return_or_except.set(obj)

    def set_exception(self, ex):
        assert(self.is_init())
        self.return_or_except.set_exception(ex)
        if self.is_yield:
            self.yield_queue.put(None)

    def _try_except(self):
        try:
            self.return_or_except.get_nowait()
        except Timeout:
            pass

    def get(self):
        self.is_initialized.wait()

        if not self.is_yield:
            return self.return_or_except.get()

        else:
            self._try_except()
            self.yield_queue.get()

            def recv_yielder():
                while True:
                    obj = self.yield_queue.get()
                    self._try_except()
                    yield None, obj

            return self.client._yielder(recv_yielder(), self.req_id)
#}
