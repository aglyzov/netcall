# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

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

try:
    from Queue import Queue
except ImportError: # For python 3
    from queue import Queue
from random  import randint
from weakref import WeakValueDictionary

import zmq

from ..base_client import RPCClientBase
from ..concurrency import get_tools
from ..utils       import get_zmq_classes


#-----------------------------------------------------------------------------
# RPC Service Proxy
#-----------------------------------------------------------------------------

class ThreadingRPCClient(RPCClientBase):
    """ An asynchronous RPC client whose requests will not block.
        Uses the standard Python threading API for concurrency.
    """
    CONCURRENCY = 128

    def __init__(self, context=None, executor=None, **kwargs):
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
            assert isinstance(context, Context), repr(context)
            self.context = context

        self._tools    = get_tools(env=None)  # force threading API
        self._executor = executor or self._tools.Executor(limit=self.CONCURRENCY)
        self._ext_exec = bool(executor)

        super(ThreadingRPCClient, self).__init__(**kwargs)

        Event = self._tools.Event

        self._ready_ev   = Event()
        self._exit_ev    = Event()
        self._futures    = {}                     # {<req_id> : <Future>}
        self._gen_queues = WeakValueDictionary()  # {<req_id> : <Queue>}

        # request drainage
        self._sync_ev  = Event()
        self.req_queue = Queue(maxsize=getattr(self._executor, '_limit', self.CONCURRENCY))
        self.req_pub   = self.context.socket(zmq.PUB)
        self.req_addr  = 'inproc://%s-%s' % (
            self.__class__.__name__,
            '%08x' % randint(0, 0xFFFFFFFF)
        )
        self.req_pub.bind(self.req_addr)

        # maintaining threads
        self.io_thread  = self._executor.submit(self._io_thread)
        self.req_thread = self._executor.submit(self._req_thread)

    def bind(self, *args, **kwargs):
        result = super(ThreadingRPCClient, self).bind(*args, **kwargs)
        self._ready_ev.set()  # wake up the io_thread
        return result

    def bind_ports(self, *args, **kwargs):
        result = super(ThreadingRPCClient, self).bind_ports(*args, **kwargs)
        self._ready_ev.set()  # wake up the io_thread
        return result

    def connect(self, *args, **kwargs):
        result = super(ThreadingRPCClient, self).connect(*args, **kwargs)
        self._ready_ev.set()  # wake up the io_reader
        return result

    def _send_request(self, request):
        """ Send a multipart request to a service.
            Here we send the request down the internal req_pub socket
            so that an io_thread could send it back to the service.

            Notice: request is a list produced by self._build_request()
        """
        self.req_queue.put(request)

    def _req_thread(self):
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
                logger.debug('received %r', request)
                if request is None:
                    logger.debug('req_thread received an EXIT signal')
                    fwd_request([b''])  # pass the EXIT signal to the io_thread
                    break              # and exit
                fwd_request(request)
        except Exception as e:
            logger.error(e, exc_info=True)

        logger.debug('req_thread exited')

    def _io_thread(self):
        """ I/O thread

            Waits for a ZMQ socket to become ready (._ready_ev), then processes incoming requests/replies
            filling result futures thus passing control to waiting threads (see .call)
        """
        logger   = self.logger
        ready_ev = self._ready_ev
        futures  = self._futures
        g_queues = self._gen_queues

        srv_sock = self.socket
        req_sub = self.context.socket(zmq.SUB)
        req_sub.connect(self.req_addr)
        req_sub.setsockopt(zmq.SUBSCRIBE, '')

        _, Poller = get_zmq_classes()  # auto detect green env
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
        except Exception as e:
            running = False
            logger.error(e, exc_info=True)

        while running:
            ready_ev.wait()  # block until socket is bound/connected
            self._ready_ev.clear()

            #if not self._ready:
            #    break  # shutdown was called before connect/bind

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
                            srv_sock.send_multipart(request)

                    if reply_list is None:
                        continue
                except Exception as e:
                    # the socket must have been closed
                    logger.warning(e)
                    break

                logger.debug('received %r', reply_list)

                reply = self._parse_reply(reply_list)

                if reply is None:
                    #logger.debug('skipping invalid reply')
                    continue

                req_id   = reply['req_id']
                msg_type = reply['type']
                result   = reply['result']

                if msg_type == b'ACK':
                    #logger.debug('skipping ACK, req_id=%r', req_id)
                    continue

                future = futures.pop(req_id, None)

                if future is None:
                    queue = g_queues.get(req_id, None)
                    if queue is not None:
                        # existing generator
                        if msg_type == b'YIELD':
                            queue.put((result, None))
                        elif msg_type == b'FAIL':
                            queue.put((None, result))
                    del queue  # IMPORTANT: clean up references so that
                               #            self._gen_queues empties properly
                    continue
                else:
                    if msg_type == b'OK':
                        # normal result
                        future.set_result(result)
                    elif msg_type == b'FAIL':
                        # exception
                        future.set_exception(result)
                    elif msg_type == b'YIELD':
                        # new generator
                        queue = self._tools.Queue(1)
                        g_queues[req_id] = queue
                        future.set_result(self._generator(req_id, queue.get))

            if self._exit_ev.is_set():
                logger.debug('io_thread received an EXIT signal')
                break

        # -- cleanup --
        req_sub.close(0)

        logger.debug('io_thread exited')

    def shutdown(self):
        """Close the socket and signal the io_thread to exit"""
        self._ready = False
        self._exit_ev.set()
        self._ready_ev.set()

        self.logger.debug('signaling the threads to exit')
        self.req_queue.put(None)  # signal the req and io threads to exit
        self._sync_ev.set()

        if self.io_thread:
            self.io_thread.exception()

        if self.req_thread:
            self.req_thread.exception()

        self._ready_ev.clear()
        self._exit_ev.clear()

        self.logger.debug('closing the sockets')
        self.socket.close(0)
        self.req_pub.close(0)

        if not self._ext_exec:
            self.logger.debug('shutting down the executor')
            self._executor.shutdown(cancel=True)

