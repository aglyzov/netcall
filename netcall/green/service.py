# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

"""
Green version of the RPC service

Authors:

* Brian Granger
* Alexander Glyzov

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

import zmq

from ..base_service import RPCServiceBase
from ..concurrency  import get_tools
from ..utils        import get_zmq_classes, detect_green_env


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class GreenRPCService(RPCServiceBase):
    """ An asynchronous RPC service that serves requests over a ROUTER socket.
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
        self._task     = None

        super(GreenRPCService, self).__init__(**kwargs)

    def _create_socket(self):
        super(GreenRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)

    def start(self):
        """ Start the RPC service (non-blocking).

            Spawns a main-loop task that serves requests on this socket.
            Returns spawned greenlet instance.
        """
        assert self.bound or self.connected, 'not bound/connected?'
        assert self._task is None,           'already started'

        logger = self.logger
        spawn  = self._executor.submit

        def main_loop():
            recv_multipart = self.socket.recv_multipart
            handle_request = self._handle_request

            while True:
                try:
                    request = recv_multipart()
                except Exception, e:
                    logger.warning(e)
                    break
                spawn(handle_request, request)

            logger.debug('main_loop exited')

        self._task = spawn(main_loop)

        return self._task

    def stop(self, ):
        """ Stop the RPC service (non-blocking) """
        if self._task is None:
            return  # nothing to do
        bound     = self.bound
        connected = self.connected
        self.logger.debug('resetting the socket')
        self.reset()
        # wait for the greenlet to exit (closed socket)
        self._task.exception(timeout=0.3)
        self._task.cancel()
        self._task = None
        # restore bindings/connections
        self.bind(bound)
        self.connect(connected)

    def shutdown(self):
        """Close the socket and signal the reader greenlet to exit"""
        self.stop()

        self.logger.debug('closing the socket')
        self.socket.close(0)

        if not self._ext_exec:
            self.logger.debug('shutting down the executor')
            self._executor.shutdown(cancel=True)

    def serve(self):
        """ Serve RPC requests (blocking)

            Waits for the serving greenlet to exit.
        """
        if self._task is None:
            self.start()

        return self._task.result()

