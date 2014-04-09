# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

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

from ..base_client import RPCClientBase
from ..concurrency import get_tools
from ..utils       import get_zmq_classes, detect_green_env
from ..errors      import RPCTimeoutError


#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class GreenRPCClient(RPCClientBase):
    """ An asynchronous RPC client that sends requests over a DEALER socket.
        Using green threads for concurrency.
        Green environment is provided by either Gevent, Eventlet or Greenhouse
        and can be autodetected.
    """
    CONCURRENCY = 1024

    def __init__(self, green_env=None, context=None, executor=None, **kwargs):
        """
        Parameters
        ==========
        green_env  : None | 'gevent' | 'eventlet' | 'greenhouse'
        context    : optional ZMQ <Context>
        executor   : optional task <Executor>
        serializer : optional <Serializer> that will be used to serialize
                     and deserialize args, kwargs and results
        """
        self.green_env = green_env or detect_green_env() or 'gevent'

        Context, _ = get_zmq_classes(env=self.green_env)

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        self._tools    = get_tools(env=self.green_env)
        self._executor = executor or self._tools.Executor(limit=self.CONCURRENCY)
        self._ext_exec = bool(executor)

        super(GreenRPCClient, self).__init__(**kwargs)

        Event = self._tools.Event

        self._ready_ev  = Event()
        self._exit_ev   = Event()
        self._recv_task = self._executor.submit(self._reader)
        self._futures   = {}  # {<msg-id> : <_ReturnOrYieldFuture>}

    def _create_socket(self):
        super(GreenRPCClient, self)._create_socket()

    def bind(self, *args, **kwargs):
        result = super(GreenRPCClient, self).bind(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result

    def bind_ports(self, *args, **kwargs):
        result = super(GreenRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result

    def connect(self, *args, **kwargs):
        result = super(GreenRPCClient, self).connect(*args, **kwargs)
        self._ready_ev.set()  # wake up _reader
        return result

    def _reader(self):
        """ Reader greenlet

            Waits for a socket to become ready (._ready_ev), then reads incoming replies and
            fills matching async results thus passing control to waiting greenlets (see .call)
        """
        logger   = self.logger
        ready_ev = self._ready_ev
        socket   = self.socket
        futures  = self._futures

        while True:
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
                    get_future  = futures.get
                    init_future = self._ReturnOrYieldFuture.init_as_yield
                else:
                    # OK or FAIL
                    get_future  = futures.pop
                    init_future = self._ReturnOrYieldFuture.init_as_return

                future = get_future(req_id, None)

                if future is None:
                    # result is gone, must be a timeout
                    #logger.debug('async result is gone (timeout?): req_id=%r' % req_id)
                    continue

                if not future.is_init():
                    init_future(future)

                if msg_type in (b'OK', b'YIELD'):
                    logger.debug('future.set_result(result), req_id=%r' % req_id)
                    future.set_result(result)
                else:
                    logger.debug('future.set_exception(result), req_id=%r' % req_id)
                    future.set_exception(result)

            if self._exit_ev.is_set():
                logger.debug('_reader received an EXIT signal')
                break

        logger.debug('_reader exited')

    def shutdown(self):
        """Close the socket and signal the reader greenlet to exit"""
        self.logger.debug('closing the socket')
        self._ready = False
        self._exit_ev.set()
        self._ready_ev.set()
        self.socket.close(0)
        self.logger.debug('waiting for the greenlet to exit')
        self._recv_task.exception(timeout=0.3)
        self._recv_task.cancel()
        self._recv_task = None
        self._ready_ev.clear()
        self._exit_ev.clear()

        if not self._ext_exec:
            self.logger.debug('shutting down the executor')
            self._executor.shutdown(cancel=True)

    def call(self, proc_name, args=[], kwargs={}, ignore=False, timeout=None):
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

        logger = self.logger
        logger.debug('send: %r' % msg_list)

        self.socket.send_multipart(msg_list)

        if ignore:
            return None

        if timeout and timeout > 0:
            def _abort_request():
                future = self._futures.pop(req_id, None)
                if future is not None:
                    tout_msg  = "Request %s timed out after %s sec" % (req_id, timeout)
                    logger.debug(tout_msg)
                    future.set_exception(RPCTimeoutError(tout_msg))
            timer = self._tools.Timer(timeout, _abort_request)
            timer.start()
        else:
            timer = None

        future = self._ReturnOrYieldFuture(self, req_id)
        self._futures[req_id] = future
        #logger.debug('waiting for future=%r' % future)
        try:
            result = future.result()  # block waiting for a reply passed by _read_task
        finally:
            timer and timer.cancel()
        return result

