# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Green version of the RPC client

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

from ..base    import RPCClientBase
from ..utils   import logger, get_zmq_classes, detect_green_env, get_green_tools
from ..errors  import RPCTimeoutError
from ..futures import Future, TimeoutError


#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class GreenRPCClient(RPCClientBase):  #{
    """ An asynchronous service proxy whose requests will not block.
        Uses the Green compatibility layer of pyzmq (zmq.green).
    """

    def __init__(self, green_env=None, context=None, **kwargs):  #{
        """
        Parameters
        ==========
        green_env  : None | 'gevent' | 'eventlet' | 'greenhouse'
        context    : <Context>
            An existing Context instance, if not passed, green.Context.instance()
            will be used.
        serializer : <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        self.green_env = green_env or detect_green_env() or 'gevent'

        Context, _ = get_zmq_classes(env=self.green_env)

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        super(GreenRPCClient, self).__init__(**kwargs)  # base class

        spawn, _, Event, _, _ = get_green_tools(env=self.green_env)

        self._ready_ev = Event()
        self._exit_ev  = Event()
        self.greenlet  = spawn(self._reader)
        self._futures  = {}  # {<msg-id> : <_ReturnOrYieldFuture>}
    #}
    def _create_socket(self):  #{
        super(GreenRPCClient, self)._create_socket()
    #}
    def bind(self, *args, **kwargs):  #{
        result = super(GreenRPCClient, self).bind(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def bind_ports(self, *args, **kwargs):  #{
        result = super(GreenRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def connect(self, *args, **kwargs):  #{
        result = super(GreenRPCClient, self).connect(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result
    #}
    def _reader(self):  #{
        """ Reader greenlet

            Waits for a socket to become ready (._ready_ev), then reads incoming replies and
            fills matching async results thus passing control to waiting greenlets (see .call)
        """
        ready_ev = self._ready_ev
        socket   = self.socket
        futures  = self._futures
        running  = True

        while running:
            ready_ev.wait()  # block until socket is bound/connected
            self._ready_ev.clear()

            while self._ready:
                try:
                    msg_list = socket.recv_multipart()
                except Exception, e:
                    # the socket must have been closed
                    logger.warning(e)
                    break

                logger.debug('received: %r' % msg_list)

                reply = self._parse_reply(msg_list)

                if reply is None:
                    #logger.debug('skipping invalid reply')
                    continue

                req_id   = reply['req_id']
                msg_type = reply['type']
                result   = reply['result']

                if msg_type == b'ACK':
                    #logger.debug('skipping ACK, req_id=%r' % req_id)
                    continue

                if msg_type == b'YIELD':
                    futures_get_fn = futures.get
                    future_init_fn = _ReturnOrYieldFuture.init_as_yield
                else: # For OK and FAIL
                    futures_get_fn = futures.pop
                    future_init_fn = _ReturnOrYieldFuture.init_as_return

                future = futures_get_fn(req_id, None)
                if future is None:
                    # result is gone, must be a timeout
                    #logger.debug('async result is gone (timeout?): req_id=%r' % req_id)
                    continue
                if not future.is_init():
                    future_init_fn(future)

                if msg_type in [b'OK', b'YIELD']:
                    logger.debug('future.set_result(result), req_id=%r' % req_id)
                    future.set_result(result)
                else:
                    logger.debug('future.set_exception(result), req_id=%r' % req_id)
                    future.set_exception(result)

            if self._exit_ev.is_set():
                logger.debug('_reader received an EXIT signal')
                break

        logger.debug('_reader exited')
    #}
    def shutdown(self):  #{
        """Close the socket and signal the reader greenlet to exit"""
        logger.debug('closing the socket')
        self._ready = False
        self._exit_ev.set()
        self._ready_ev.set()
        self.socket.close(0)
        logger.debug('waiting for the greenlet to exit')
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

        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)

        logger.debug('send: %r' % msg_list)
        self.socket.send_multipart(msg_list)

        if ignore:
            return None

        _, spawn_later, _, _, _ = get_green_tools(env=self.green_env)

        if timeout and timeout > 0:
            def _abort_request():
                future = self._futures.pop(req_id, None)
                if future is not None:
                    tout_msg  = "Request %s timed out after %s sec" % (req_id, timeout)
                    logger.debug(tout_msg)
                    future.set_exception(RPCTimeoutError(tout_msg))
            spawn_later(timeout, _abort_request)

        future = _ReturnOrYieldFuture(self, req_id)
        self._futures[req_id] = future
        logger.debug('waiting for future=%r' % future)
        return future.result()  # block waiting for a reply passed by ._reader
    #}
#}

class _ReturnOrYieldFuture(object):  #{

    def __init__(self, client, req_id):
        _, _, Event, _, Queue = get_green_tools(env=client.green_env)
        
        self.is_initialized = Event()
        self.return_or_except = Future()
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

    def set_result(self, obj):
        assert(self.is_init())
        if self.is_yield:
            self.yield_queue.put(obj)
        else:
            self.return_or_except.set_result(obj)

    def set_exception(self, ex):
        assert(self.is_init())
        self.return_or_except.set_exception(ex)
        if self.is_yield:
            self.yield_queue.put(None)

    def _try_except(self):
        try:
            self.return_or_except.result(timeout=0)
        except TimeoutError:
            pass

    def result(self):
        self.is_initialized.wait()

        if not self.is_yield:
            return self.return_or_except.result()

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
