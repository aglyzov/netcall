# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
An RPC service using ZeroMQ as a transport.

Authors:

* Brian Granger
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

import zmq

from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop    import IOLoop

from tornado.concurrent import Future

from ..base_service import RPCServiceBase

#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class TornadoRPCService(RPCServiceBase):  #{
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using Tornado compatible IOLoop and ZMQStream from PyZMQ.
    """

    def __init__(self, context=None, ioloop=None, **kwargs):  #{
        """
        Parameters
        ==========
        ioloop : IOLoop
            An existing IOLoop instance, if not passed, zmq.IOLoop.instance()
            will be used.
        context : Context
            An existing Context instance, if not passed, zmq.Context.instance()
            will be used.
        serializer : Serializer
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        """
        assert context is None or isinstance(context, zmq.Context)
        self.context = context if context is not None else zmq.Context.instance()
        self.ioloop  = IOLoop.instance() if ioloop is None else ioloop
        self._is_started = False
        super(TornadoRPCService, self).__init__(**kwargs)
    #}
    def _create_socket(self):  #{
        super(TornadoRPCService, self)._create_socket()
        socket = self.context.socket(zmq.ROUTER)
        self.socket = ZMQStream(socket, self.ioloop)
    #}
    def _handle_request(self, msg_list):  #{
        """
        Handle an incoming request.

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <ser_args>, <ser_kwargs>, <ignore>]

        First, the service sends back a notification that the message was
        indeed received:

        [<id>..<id>, b'|', req_id, b'ACK',  service_id]

        Next, the actual reply depends on if the call was successful or not:

        [<id>..<id>, b'|', req_id, b'OK',   <serialized result>]
        [<id>..<id>, b'|', req_id, b'FAIL', <JSON dict of ename, evalue, traceback>]

        Here the (ename, evalue, traceback) are utf-8 encoded unicode.
        """
        req = self._parse_request(msg_list)
        if req is None:
            return
        self._send_ack(req)

        ignore = req['ignore']

        try:
            # raise any parsing errors here
            if req['error']:
                raise req['error']
            # call procedure
            res = req['proc'](*req['args'], **req['kwargs'])
        except Exception:
            not ignore and self._send_fail(req)
        else:
            def send_future_result(fut):
                try:    res = fut.result()
                except: not ignore and self._send_fail(req)
                else:   not ignore and self._send_ok(req, res)

            if isinstance(res, Future):
                self.ioloop.add_future(res, send_future_result)
            else:
                not ignore and self._send_ok(req, res)
    #}
    def start(self):  #{
        """ Start the RPC service (non-blocking) """
        assert self._is_started == False, "already started"
        # register IOLoop callback
        self._is_started = True
        self.socket.on_recv(self._handle_request)
    #}
    def stop(self):  #{
        """ Stop the RPC service (non-blocking) """
        # register IOLoop callback
        self.socket.stop_on_recv()
        self._is_started = False
    #}
    def serve(self):  #{
        """ Serve RPC requests (blocking) """
        if not self._is_started:
            self.start()
        return self.ioloop.start()
    #}
#}
