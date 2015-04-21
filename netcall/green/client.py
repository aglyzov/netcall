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

from weakref import WeakValueDictionary

from ..base_client import RPCClientBase
from ..concurrency import get_tools
from ..utils       import get_zmq_classes, detect_green_env


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

        self._ready_ev   = Event()
        self._exit_ev    = Event()
        self._recv_task  = self._executor.submit(self._reader)
        self._futures    = {}                     # {<req_id> : <Future>}
        self._gen_queues = WeakValueDictionary()  # {<req_id> : <Queue>}

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
        g_queues = self._gen_queues

        while True:
            ready_ev.wait()  # block until socket is bound/connected
            self._ready_ev.clear()

            while self._ready:
                try:
                    msg_list = socket.recv_multipart()
                except Exception as e:
                    # the socket must have been closed
                    logger.warning(e)
                    break

                logger.debug('received %r', msg_list)

                reply = self._parse_reply(msg_list)

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

                queue = None  # IMPORTANT: clean up references so that
                              #            self._gen_queues empties properly

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


