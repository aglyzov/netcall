# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Gevent version of the RPC service

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

import exceptions
from types import GeneratorType

import gevent
from gevent.queue import Queue
import zmq

from ..base  import RPCServiceBase
from ..utils import get_zmq_classes


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class GeventRPCService(RPCServiceBase):
    """ An asynchronous RPC service that takes requests over a ROUTER socket.
        Using Gevent compatibility layer from PyZMQ (zmq.green).
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

        super(GeventRPCService, self).__init__(**kwargs)

        self.greenlet = None
        self.yield_send_queues = {}  # {<req_id> : <gevent.queue.Queue>}
        # Can also use collections.deque, append() and popleft() being thread safe
    #}
    def _create_socket(self):  #{
        super(GeventRPCService, self)._create_socket()
        self.socket = self.context.socket(zmq.ROUTER)
    #}
    def _handle_request(self, msg_list):  #{
        """Handle an incoming request.

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <ser_args>, <ser_kwargs>, <ignore>]

        First, the service sends back a notification that the message was
        indeed received:

        [<id>..<id>, b'|', req_id, b'ACK',  service_id]

        Next, the actual reply depends on if the call was successful or not:

        [<id>..<id>, b'|', req_id, b'OK',    <serialized result>]
        [<id>..<id>, b'|', req_id, b'YIELD', <serialized result>]*
        [<id>..<id>, b'|', req_id, b'FAIL',  <JSON dict of ename, evalue>]

        Here the (ename, evalue, traceback) are utf-8 encoded unicode.

        In case of a YIELD reply, the client can send a YIELD_SEND, YIELD_THROW or
        YIELD_CLOSE messages with the same req_id as in the first message sent.
        The first YIELD reply will contain no result to signal the client it is a
        yield-generator. The first message sent by the client to a yield-generator
        must be a YIELD_SEND with None as argument.

        [<id>..<id>, b'|', req_id, 'YIELD_SEND',  <serialized sent value>]
        [<id>..<id>, b'|', req_id, 'YIELD_THROW', <serialized ename, evalue>]
        [<id>..<id>, b'|', req_id, 'YIELD_CLOSE', <no args & kwargs>]

        The service will first send an ACK message. Then, it will send a YIELD
        reply whenever ready, or a FAIL reply in case an exception is raised.

        Termination of the yield-generator happens by throwing an exception.
        Normal termination raises a StopIterator. Termination by YIELD_CLOSE can
        raises a GeneratorExit or a StopIteration depending on the implementation
        of the yield-generator. Any other exception raised will also terminate
        the yield-generator.
        """
        req = self._parse_request(msg_list)
        if req is None:
            return
        self._send_ack(req)

        logger = self.logger
        ignore = req['ignore']

        try:
            # raise any parsing errors here
            if req['error']:
                raise req['error']

            if req['proc'] in ['YIELD_SEND', 'YIELD_THROW', 'YIELD_CLOSE']:
                if req['req_id'] not in self.yield_send_queues:
                    raise ValueError('req_id does not refer to a known generator')

                self.yield_send_queues[req['req_id']].put((req['proc'], req['args']))
                return
            else:
                # call procedure
                res = req['proc'](*req['args'], **req['kwargs'])
        except Exception:
            not ignore and self._send_fail(req)
        else:
            if ignore:
                return

            if isinstance(res, GeneratorType):
                self._handle_yield(req, res)
            else:
                self._send_ok(req, res)
    #}
    def _handle_yield(self, req, res):  #{
        logger = self.logger
        
        req_id = req['req_id']
        logger.debug('Adding reference to yield %s', req_id)
        input_queue = self.yield_send_queues[req_id] = Queue(1)

        self._send_yield(req, None)
        gene = res

        try:
            while True:
                proc, args = input_queue.get() # Will get stuck when shutdown()

                if proc == 'YIELD_SEND':
                    res = gene.send(args)
                    self._send_yield(req, res)

                elif proc == 'YIELD_THROW':
                    ex_class = getattr(exceptions, args[0], Exception)
                    eargs = args[:2]
                    eargs[0] = ex_class
                    res = gene.throw(*eargs)
                    self._send_yield(req, res)

                else:
                    gene.close()
                    self._send_ok(req, None)
                    break
        except:
            self._send_fail(req)
        finally:
            logger.debug('Removing reference to yield %s', req_id)
            del self.yield_send_queues[req_id]
    #}
    def start(self):  #{
        """ Start the RPC service (non-blocking).

            Spawns a receive-reply greenlet that serves this socket.
            Returns spawned greenlet instance.
        """
        assert self.bound or self.connected, 'not bound/connected?'
        assert self.greenlet is None, 'already started'

        def receive_reply():
            while True:
                try:
                    request = self.socket.recv_multipart()
                except:
                    #logger.warning('socket error', exc_info=True)
                    break
                gevent.spawn(self._handle_request, request)
            self.logger.debug('receive_reply exited')

        self.greenlet = gevent.spawn(receive_reply)
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
    def serve(self, greenlets=[]):  #{
        """ Serve RPC requests (blocking)

            Waits for specified greenlets or for this greenlet
        """
        if greenlets:
            return gevent.joinall(greenlets)
        else:
            if self.greenlet is None:
                self.start()
            return self.greenlet.join()
    #}

