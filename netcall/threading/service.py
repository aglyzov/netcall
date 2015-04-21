# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
An RPC service class using ZeroMQ as a transport and
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

from random import randint

import zmq

from ..base_service import RPCServiceBase
from ..concurrency  import get_tools, TimeoutError
from ..utils        import get_zmq_classes


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class ThreadingRPCService(RPCServiceBase):
    """ An asynchronous RPC service that serves requests over a ROUTER socket.
        Using the standard Python threading API for concurrency.
    """
    CONCURRENCY = 128

    def __init__(self, context=None, executor=None, **kwargs):  #{
        """
        Parameters
        ==========
        context    : optional ZMQ <Context>
        executor   : optional task <Executor>
        serializer : optional <Serializer> that will be used to serialize
                     and deserialize args, kwargs and results
        """
        Context, _ = get_zmq_classes()  # auto detect green env

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        self._tools    = get_tools(env=None)  # force threading API
        self._executor = executor or self._tools.Executor(limit=self.CONCURRENCY)
        self._ext_exec = bool(executor)

        super(ThreadingRPCService, self).__init__(**kwargs)

        self.io_thread  = None
        self.res_thread = None

        if executor:
            if hasattr(executor, '_limit'):
                limit = executor._limit
            elif hasattr(executor, '_max_workers'):
                limit = executor._max_workers
            else:
                limit = None
        else:
            limit = self.CONCURRENCY

        # result drainage
        self._sync_ev  = self._tools.Event()
        self.res_queue = self._tools.Queue(maxsize=limit)
        self.res_pub   = self.context.socket(zmq.PUB)
        self.res_addr  = 'inproc://%s-%s' % (
            self.__class__.__name__,
            ('%08x' % randint(0, 0xFFFFFFFF)).encode()
        )
        self.res_pub.bind(self.res_addr)
    #}
    def _create_socket(self):  #{
        super(ThreadingRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)
    #}
    def _send_reply(self, reply):  #{
        """ Send a multipart reply to a caller.
            Here we send the reply down the internal res_pub socket
            so that an io_thread could send it back to the caller.

            Notice: reply is a list produced by self._build_reply()
        """
        self.res_queue.put(reply)
    #}
    def start(self):  #{
        """ Start the RPC service (non-blocking).

            Spawns two threads:
            - an I/O thread sends/receives ZMQ messages and passes requests to
              the thread pool of handlers
            - a result thread forwards results from req_queue to the I/O thread
              which sends them back to a caller
        """
        assert self.bound or self.connected, 'not bound/connected'
        assert self.io_thread is None and self.res_thread is None, 'already started'

        logger = self.logger

        def res_thread():  #{
            """ Forwards results from res_queue to the res_pub socket
                so that an I/O thread could send them back to a caller
            """
            rcv_result = self.res_queue.get
            fwd_result = self.res_pub.send_multipart

            try:
                # synchronizing with the I/O thread
                sync = self._sync_ev
                while not sync.is_set():
                    fwd_result([b'SYNC'])
                    sync.wait(0.05)
                logger.debug('RES thread is synchronized')

                while True:
                    result = rcv_result()
                    logger.debug('received %r', result)
                    if result is None:
                        logger.debug('res_thread received an EXIT signal')
                        fwd_result([b''])  # pass the EXIT signal to the io_thread
                        break
                    else:
                        fwd_result(result)
            except Exception as e:
                logger.error(e, exc_info=True)

            logger.debug('res_thread exited')
        #}
        def io_thread():  #{
            task_sock = self.socket
            res_sub   = self.context.socket(zmq.SUB)
            res_sub.connect(self.res_addr)
            res_sub.setsockopt(zmq.SUBSCRIBE, '')

            _, Poller = get_zmq_classes()
            poller = Poller()
            poller.register(task_sock, zmq.POLLIN)
            poller.register(res_sub,   zmq.POLLIN)
            poll = poller.poll

            handle_request = self._handle_request
            running        = True
            spawn          = self._executor.submit

            try:
                # synchronizing with the res_thread
                sync = res_sub.recv_multipart()
                assert sync[0] == 'SYNC'
                logger.debug('I/O thread is synchronized')
                self._sync_ev.set()

                while running:
                    for socket, _ in poll():
                        if socket is task_sock:
                            request = task_sock.recv_multipart()
                            logger.debug('received %r', request)
                            # handle request in a thread-pool
                            spawn(handle_request, request)
                        elif socket is res_sub:
                            result = res_sub.recv_multipart()
                            if not result[0]:
                                logger.debug('io_thread received an EXIT signal')
                                running = False
                                break
                            else:
                                task_sock.send_multipart(result)
            except Exception as e:
                logger.error(e, exc_info=True)
            finally:
                # -- cleanup --
                res_sub.close(0)

            logger.debug('io_thread exited')
        #}

        self.res_thread = self._executor.submit(res_thread)
        self.io_thread  = self._executor.submit(io_thread)

        return self.res_thread, self.io_thread
    #}
    def stop(self):  #{
        """ Stop the RPC service (semi-blocking) """
        if not self.res_thread and not self.io_thread:
            return

        bound     = self.bound
        connected = self.connected

        self.logger.debug('signaling the threads to exit')

        self.res_queue.put(None)
        self.reset()

        if self.res_thread:
            self.res_thread.exception(timeout=0.3)
            self.res_thread.cancel()
            self.res_thread = None

        if self.io_thread:
            self.io_thread.exception(timeout=0.3)
            self.io_thread.cancel()
            self.io_thread = None

        # restore bindings/connections
        self.bind(bound)
        self.connect(connected)
    #}
    def shutdown(self):  #{
        """ Signal the threads to exit and close all sockets """
        self.stop()

        self.logger.debug('closing the sockets')
        self.socket.close(0)
        self.res_pub.close(0)

        if not self._ext_exec:
            self.logger.debug('shutting down the executor')
            self._executor.shutdown(cancel=True)
    #}
    def serve(self):  #{
        """ Serve RPC requests (blocking)

            Simply waits for self.res_thread and self.io_thread to exit
        """
        if self.res_thread is None and self.io_thread is None:
            self.start()

        while True:
            try:
                self._executor.wait(timeout=1)
                break
            except TimeoutError:
                pass
    #}

