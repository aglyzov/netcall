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

import exceptions
from random    import randint
from Queue     import Queue, Empty
from threading import Event
from types     import GeneratorType

import zmq

from ..base  import RPCServiceBase
from ..utils import get_zmq_classes, ThreadPool


#-----------------------------------------------------------------------------
# RPC Service
#-----------------------------------------------------------------------------

class ThreadingRPCService(RPCServiceBase):
    """ A threading RPC service that takes requests over a ROUTER socket.
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

        super(ThreadingRPCService, self).__init__(**kwargs)

        if pool is None:
            self.pool      = ThreadPool(128)
            self._ext_pool = False
        else:
            self.pool      = pool
            self._ext_pool = True

        self.io_thread  = None
        self.res_thread = None
        
        self.yield_send_queues = {}  # {<req_id> : <Queue>}

        # result drainage
        self._sync_ev  = Event()
        self.res_queue = Queue(maxsize=self.pool._workers)
        self.res_pub   = self.context.socket(zmq.PUB)
        self.res_addr  = 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
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
    def _handle_request(self, msg_list):  #{
        """Handle an incoming request.

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
                while self.running:
                    try:
                        proc, args = input_queue.get(True, 0.5)
                        break
                    except Empty:
                        pass
                if not self.running:
                    # Shutdown has been called. Clean-up.
                    gene.close()
                    return
                
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

            Spawns two threads:
            - an I/O thread sends/receives ZMQ messages and passes requests to
              the thread pool of handlers
            - a result thread forwards results from req_queue to the I/O thread
              which sends them back to a caller
        """
        assert self.bound or self.connected, 'not bound/connected'
        assert self.io_thread is None and self.res_thread is None, 'already started'

        def res_thread():  #{
            """ Forwards results from res_queue to the res_pub socket
                so that an I/O thread could send them back to a caller
            """
            logger     = self.logger
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
                    if result is None:
                        logger.debug('res_thread received an EXIT signal')
                        fwd_result([b''])  # pass the EXIT signal to the io_thread
                        break
                    else:
                        fwd_result(result)
            except Exception, e:
                logger.error(e, exc_info=True)

            logger.debug('res_thread exited')
        #}
        def io_thread():  #{
            logger    = self.logger
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

            try:
                # synchronizing with the res_thread
                sync = res_sub.recv_multipart()
                assert sync[0] == 'SYNC'
                logger.debug('I/O thread is synchronized')
                self._sync_ev.set()

                self.running = True

                while self.running:
                    for socket, _ in poll():
                        if socket is task_sock:
                            request = task_sock.recv_multipart()
                            logger.debug('io_thread received %r' % request)
                            # handle request in a thread-pool
                            self.pool.schedule(handle_request, args=(request,))
                        elif socket is res_sub:
                            result = res_sub.recv_multipart()
                            #logger.debug('received a result: %r' % result)
                            if not result[0]:
                                logger.debug('io_thread received an EXIT signal')
                                self.running = False
                                break
                            else:
                                logger.debug('io_thread sending %r' % result)
                                task_sock.send_multipart(result)
            except Exception, e:
                logger.error(e, exc_info=True)

            # -- cleanup --
            res_sub.close(0)

            logger.debug('io_thread exited')
        #}

        self.res_thread = self.pool.schedule(res_thread)
        self.io_thread  = self.pool.schedule(io_thread)

        return self.res_thread, self.io_thread
    #}
    def stop(self):  #{
        """ Stop the RPC service (semi-blocking) """
        if self.res_thread and not self.res_thread.ready:
            self.logger.debug('signaling the threads to exit')
            self.res_queue.put(None)
            self.res_thread.wait()
            self.io_thread.wait()
            self.res_thread = None
            self.io_thread  = None
    #}
    def shutdown(self):  #{
        """ Signal the threads to exit and close all sockets """
        self.stop()

        logger = self.logger
        logger.debug('closing the sockets')
        self.socket.close(0)
        self.res_pub.close(0)

        if not self._ext_pool:
            logger.debug('stopping the pool')
            self.pool.close()
            self.pool.stop()
            self.pool.join()
    #}
    def serve(self):  #{
        """ Serve RPC requests (blocking)

            Simply waits for self.res_thread and self.io_thread to exit
        """
        res, io = self.res_thread, self.io_thread
        assert res is not None and io is not None, 'not started'

        while not res.ready or not io.ready:
            res.wait(0.25)
            io.wait(0.25)
    #}

