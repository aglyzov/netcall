# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

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
from ..utils        import get_zmq_classes, detect_green_env, get_green_tools


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class GreenRPCService(RPCServiceBase):
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using green threads for concurrency.
        Green environment is provided by either Gevent, Eventlet or Greenhouse
        and can be autodetected.
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

        super(GreenRPCService, self).__init__(**kwargs)

        self.greenlet = None
        # Can also use collections.deque, append() and popleft() being thread safe
    #}
    def _create_socket(self):  #{
        super(GreenRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)
    #}
    def _get_tools(self):  #{
        "Returns a tuple (Queue, Empty)"
        _, _, _, _, Queue, Empty = get_green_tools(env=self.green_env)
        return Queue, Empty
    #}
    def start(self):  #{
        """ Start the RPC service (non-blocking).

            Spawns a receive-reply greenlet that serves this socket.
            Returns spawned greenlet instance.
        """
        assert self.bound or self.connected, 'not bound/connected?'
        assert self.greenlet is None, 'already started'

        logger = self.logger
        spawn  = get_green_tools(env=self.green_env)[0]

        def receive_reply():
            self.running = True
            while self.running:
                try:
                    request = self.socket.recv_multipart()
                except Exception, e:
                    logger.warning(e)
                    break
                spawn(self._handle_request, request)
            self.running = False
            logger.debug('receive_reply exited')

        self.greenlet = spawn(receive_reply)

        return self.greenlet
    #}
    def stop(self, ):  #{
        """ Stop the RPC service (non-blocking) """
        if self.greenlet is None:
            return  # nothing to do
        bound     = self.bound
        connected = self.connected
        self.logger.debug('resetting the socket')
        self.reset()
        # wait for the greenlet to exit (closed socket)
        self.greenlet.join()
        self.greenlet = None
        # restore bindings/connections
        self.bind(bound)
        self.connect(connected)
    #}
    def shutdown(self):  #{
        """Close the socket and signal the reader greenlet to exit"""
        self.stop()
        self.socket.close(0)
    #}
    def serve(self):  #{
        """ Serve RPC requests (blocking)

            Waits for the serving greenlet to exit.
        """
        if self.greenlet is None:
            self.start()

        return self.greenlet.join()
    #}

