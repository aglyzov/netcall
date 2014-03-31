# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
An RPC client class using ZeroMQ as a transport and
the standard Python threading API for concurrency.

Authors
-------
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

from Queue     import Queue
from random    import randint
from threading import Event, Timer

try:
    from concurrent.futures import Future, TimeoutError
except ImportError:
    from ..futures import Future, TimeoutError

import zmq

from ..base_client import RPCClientBase
from ..utils       import get_zmq_classes, ThreadPool
from ..errors      import RPCTimeoutError


#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class ThreadingRPCClient(RPCClientBase):  #{
    """ An asynchronous RPC client whose requests will not block.
        Uses the standard Python threading API for concurrency.
    """
    def __init__(self, context=None, pool=None, **kwargs):  #{
        """
        Parameters
        ==========
        context    : <Context>
            An existing ZMQ Context instance, if not passed get_zmq_classes()
            will be used to obtain a compatible Context class.
        pool       : <ThreadPool>
            A thread pool to run handlers in.
        serializer : <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        Context, _ = get_zmq_classes()

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        super(ThreadingRPCClient, self).__init__(**kwargs)  # base class

        if pool is None:
            self.pool      = ThreadPool(128)
            self._ext_pool = False
        else:
            self.pool      = pool
            self._ext_pool = True

        self._ready_ev = Event()
        self._futures  = {}  # {<msg-id> : <_ReturnOrYieldFuture>}

        # request drainage
        self._sync_ev  = Event()
        self.req_queue = Queue(maxsize=self.pool._workers)
        self.req_pub   = self.context.socket(zmq.PUB)
        self.req_addr  = 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
        )
        self.req_pub.bind(self.req_addr)

        # maintaining threads
        self.io_thread  = self.pool.schedule(self._io_thread)
        self.req_thread = self.pool.schedule(self._req_thread)
    #}
    def bind(self, *args, **kwargs):  #{
        result = super(ThreadingRPCClient, self).bind(*args, **kwargs)
        self._ready_ev.set()  # wake up the io_thread
        return result
    #}
    def bind_ports(self, *args, **kwargs):  #{
        result = super(ThreadingRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up the io_thread
        return result
    #}
    def connect(self, *args, **kwargs):  #{
        result = super(ThreadingRPCClient, self).connect(*args, **kwargs)
        self._ready_ev.set()  # wake up the io_reader
        return result
    #}
    def _send_request(self, request):  #{
        """ Send a multipart request to a service.
            Here we send the request down the internal req_pub socket
            so that an io_thread could send it back to the service.

            Notice: request is a list produced by self._build_request()
        """
        self.req_queue.put(request)
    #}
    def _req_thread(self):  #{
        """ Forwards results from req_queue to the req_pub socket
            so that an I/O thread could send them forth to a service
        """
        logger      = self.logger
        rcv_request = self.req_queue.get
        fwd_request = self.req_pub.send_multipart

        try:
            # synchronizing with the I/O thread
            sync = self._sync_ev
            while not sync.is_set():
                fwd_request([b'SYNC'])
                sync.wait(0.05)
            logger.debug('REQ thread is synchronized')

            while True:
                request = rcv_request()
                #logger.debug('req_thread received %r' % request)
                if request is None:
                    logger.debug('req_thread received an EXIT signal')
                    fwd_request([''])  # pass the EXIT signal to the io_thread
                    break              # and exit
                fwd_request(request)
        except Exception, e:
            logger.error(e, exc_info=True)

        logger.debug('req_thread exited')
    #}
    def _io_thread(self):  #{
        """ I/O thread

            Waits for a ZMQ socket to become ready (._ready_ev), then processes incoming requests/replies
            filling result futures thus passing control to waiting threads (see .call)
        """
        logger   = self.logger
        ready_ev = self._ready_ev
        futures  = self._futures

        srv_sock = self.socket
        req_sub = self.context.socket(zmq.SUB)
        req_sub.connect(self.req_addr)
        req_sub.setsockopt(zmq.SUBSCRIBE, '')

        _, Poller = get_zmq_classes()
        poller = Poller()
        poller.register(srv_sock, zmq.POLLIN)
        poller.register(req_sub,  zmq.POLLIN)
        poll = poller.poll

        try:
            # synchronizing with the req_thread
            sync = req_sub.recv_multipart()
            assert sync[0] == 'SYNC'
            logger.debug('I/O thread is synchronized')
            self._sync_ev.set()
            running = True
        except Exception, e:
            running = False
            logger.error(e, exc_info=True)

        while running:
            ready_ev.wait()  # block until socket is bound/connected
            self._ready_ev.clear()

            if not self._ready:
                break  # shutdown was called before connect/bind

            while self._ready:
                try:
                    reply_list = None

                    for socket, _ in poll():
                        if socket is srv_sock:
                            reply_list = srv_sock.recv_multipart()
                        elif socket is req_sub:
                            request = req_sub.recv_multipart()
                            if not request[0]:
                                logger.debug('io_thread received an EXIT signal')
                                running = False
                                break
                            logger.debug('io_thread sending %r' % request)
                            srv_sock.send_multipart(request)

                    if reply_list is None:
                        continue
                except Exception, e:
                    # the socket must have been closed
                    logger.warning(e)
                    break

                logger.debug('io_thread received %r' % reply_list)

                reply = self._parse_reply(reply_list)

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
                    future_init_fn = self._ReturnOrYieldFuture.init_as_yield
                else: # For OK and FAIL
                    futures_get_fn = futures.pop
                    future_init_fn = self._ReturnOrYieldFuture.init_as_return

                future = futures_get_fn(req_id, None)
                if future is None:
                    # result is gone, must be a timeout
                    #logger.debug('future result is gone (timeout?): req_id=%r' % req_id)
                    continue
                if not future.is_init():
                    future_init_fn(future)

                if msg_type in [b'OK', b'YIELD']:
                    logger.debug('future.set_result(result), req_id=%r' % req_id)
                    future.set_result(result)
                else:
                    logger.debug('future.set_exception(result), req_id=%r' % req_id)
                    future.set_exception(result)

        # -- cleanup --
        req_sub.close(0)

        logger.debug('io_thread exited')
    #}
    def _get_tools(self):  #{
        "Returns a tuple (Event, Queue, Future, TimeoutError)"
        return Event, Queue, Future, TimeoutError
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

        self.req_queue.put(msg_list)

        if ignore:
            return None

        if timeout and timeout > 0:
            def _abort_request():
                result = self._futures.pop(req_id, None)
                not result.is_init() and result.init_as_return()
                if result is not None:
                    tout_msg  = "Request %s timed out after %s sec" % (req_id, timeout)
                    self.logger.debug(tout_msg)
                    result.set_exception(RPCTimeoutError(tout_msg))
            timer = Timer(timeout, _abort_request)
            timer.start()
        else:
            timer = None

        future = self._ReturnOrYieldFuture(self, req_id)
        self._futures[req_id] = future
        #logger.debug('waiting for future=%r' % future)
        try:
            result = future.result()  # block waiting for a reply passed by the io_thread
        finally:
            timer and timer.cancel()
        return result
    #}
    def shutdown(self):  #{
        """Close the socket and signal the io_thread to exit"""
        self._ready = False
        self._ready_ev.set()

        self.logger.debug('signaling the threads to exit')
        self.req_queue.put(None)  # signal the req and io threads to exit
        self.io_thread.wait()
        self.req_thread.wait()

        self._ready_ev.clear()

        self.logger.debug('closing the sockets')
        self.socket.close(0)
        self.req_pub.close(0)

        if not self._ext_pool:
            self.logger.debug('stopping the pool')
            self.pool.close()
            self.pool.stop()
            self.pool.join()
    #}
#}
