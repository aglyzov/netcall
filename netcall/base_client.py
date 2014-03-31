# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Base RPC client class

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

from abc     import abstractmethod
from sys     import exc_info
from random  import randint
from logging import getLogger

import zmq
from zmq.utils import jsonapi

from .base   import RPCBase
from .errors import RemoteRPCError, RPCError
from .utils  import RemoteMethod


#-----------------------------------------------------------------------------
# RPC Client base
#-----------------------------------------------------------------------------

class RPCClientBase(RPCBase):  #{
    """A service proxy to for talking to an RPCService."""

    logger = getLogger('netcall.client')

    def _create_socket(self):  #{
        super(RPCClientBase, self)._create_socket()

        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.identity)
    #}
    def _build_request(self, method, args, kwargs, ignore=False, req_id=None):  #{
        req_id = req_id or b'%x' % randint(0, 0xFFFFFFFF)
        method = bytes(method)
        msg_list = [b'|', req_id, method]
        data_list = self._serializer.serialize_args_kwargs(args, kwargs)
        msg_list.extend(data_list)
        msg_list.append(bytes(int(ignore)))
        return req_id, msg_list
    #}
    def _send_request(self, request):  #{
        self.logger.debug('sending %r' % request)
        self.socket.send_multipart(request)
    #}
    def _parse_reply(self, msg_list):  #{
        """
        Parse a reply from service
        (should not raise an exception)

        The reply is received as a multipart message:

        [b'|', req_id, type, payload ...]

        Returns either None or a dict {
            'type'   : <message_type:bytes>       # ACK | OK | YIELD | FAIL
            'req_id' : <id:bytes>,                # unique message id
            'srv_id' : <service_id:bytes> | None  # only for ACK messages
            'result' : <object>
        }
        """
        logger = self.logger

        if len(msg_list) < 4 or msg_list[0] != b'|':
            logger.error('bad reply: %r' % msg_list)
            return None

        msg_type = msg_list[2]
        data     = msg_list[3:]
        result   = None
        srv_id   = None

        if msg_type == b'ACK':
            srv_id = data[0]
        elif msg_type in (b'OK', b'YIELD'):
            try:
                result = self._serializer.deserialize_result(data)
            except Exception, e:
                msg_type = b'FAIL'
                result   = e
        elif msg_type == b'FAIL':
            try:
                error  = jsonapi.loads(msg_list[3])
                if error['ename'] == 'StopIteration':
                    result = StopIteration()
                elif error['ename'] == 'GeneratorExit':
                    result = GeneratorExit()
                else:
                    result = RemoteRPCError(error['ename'], error['evalue'], error['traceback'])
            except Exception, e:
                logger.error('unexpected error while decoding FAIL', exc_info=True)
                result = RPCError('unexpected error while decoding FAIL: %s' % e)
        else:
            result = RPCError('bad message type: %r' % msg_type)

        return dict(
            type   = msg_type,
            req_id = msg_list[1],
            srv_id = srv_id,
            result = result,
        )
    #}

    def _yielder(self, recv_generator, req_id):  #{
        """Implements the yield-generator workflow.
        This function is made to be reused by synchronous subclasses.
        For asynchronous subclasses, use _ReturnOrYieldFuture class.
        recv_generator should be a generator yielding tuples of (_, data).
        That is, the first element of the tuple is ignored.
        recv_generator should NOT yield the data from the very first YIELD reply.
        """
        logger = self.logger

        def _send(method, args):
            _, msg_list = self._build_request(method, args, None, False, req_id=req_id)
            logger.debug('send: %r' % msg_list)
            self._send_request(msg_list)

        try:
            _send('YIELD_SEND', None)
            while True:
                _, obj = next(recv_generator)
                try:
                    to_send = yield obj
                except Exception, e:
                    logger.debug('generator.throw()')
                    etype, evalue, _ = exc_info()
                    _send('YIELD_THROW', [str(etype.__name__), str(evalue)])
                else:
                    _send('YIELD_SEND', to_send)
        except StopIteration:
            return
        except GeneratorExit, e:
            logger.debug('generator.close()')
            _send('YIELD_CLOSE', None)
            next(recv_generator)
            raise e
        finally:
            logger.debug('_yielder exits (req_id=%s)', req_id)
    #}

    def __getattr__(self, name):  #{
        return RemoteMethod(self, name)
    #}

    @abstractmethod
    def _get_tools(self):  #{
        "Returns a tuple (Event, Queue, Future, TimeoutError)"
        pass
    #}

    @abstractmethod
    def call(self, proc_name, args=[], kwargs={}, ignore=False, timeout=None):  #{
        """
        Call the remote method with *args and **kwargs
        (may raise exception)

        Parameters
        ----------
        proc_name : <bytes> name of the remote procedure to call
        args      : <tuple> positional arguments of the remote procedure
        kwargs    : <dict>  keyword arguments of the remote procedure
        ignore    : <bool>  whether to ignore result or wait for it

        Returns
        -------
        result : <object>
            If the call succeeds, the result of the call will be returned.
            If the call fails, `RemoteRPCError` will be raised.
        """
        pass
    #}

    class _ReturnOrYieldFuture(object):  #{

        def __init__(self, client, req_id):
            Event, Queue, Future, self.TimeoutError = client._get_tools()

            self.is_initialized = Event()
            self.return_or_except = Future()
            self.yield_queue = Queue(1)
            self.client = client
            self.req_id = req_id

        def init_as_return(self):
            assert(not self.is_init())
            self.is_yield = False
            self.is_initialized.set()

        def init_as_yield(self):
            assert(not self.is_init())
            self.is_yield = True
            self.is_initialized.set()

        def is_init(self):
            return self.is_initialized.is_set()

        def set_result(self, obj):
            assert(self.is_init())
            if self.is_yield:
                self.yield_queue.put(obj)
            else:
                self.return_or_except.set_result(obj)

        def set_exception(self, ex):
            assert(self.is_init())
            self.return_or_except.set_exception(ex)
            if self.is_yield:
                self.yield_queue.put(None)

        def _try_except(self):
            try:
                self.return_or_except.result(timeout=0)
            except self.TimeoutError:
                pass

        def result(self):
            self.is_initialized.wait()

            if not self.is_yield:
                return self.return_or_except.result()

            else:
                self._try_except()
                self.yield_queue.get()

                def recv_yielder():
                    while True:
                        obj = self.yield_queue.get()
                        self._try_except()
                        yield None, obj

                return self.client._yielder(recv_yielder(), self.req_id)
    #}
#}
