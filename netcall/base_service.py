# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

"""
Base RPC service class

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

try:
    import exceptions
except ImportError: # Fix for python 3
    exceptions = __builtins__
import sys

from sys       import exc_info
from abc       import abstractmethod
from types     import GeneratorType
from logging   import getLogger
from traceback import format_exc
from functools import partial
from itertools import chain

from zmq.utils import jsonapi

from .base import RPCBase


#-----------------------------------------------------------------------------
# RPC Service base
#-----------------------------------------------------------------------------

class RPCServiceBase(RPCBase):

    _GEN_PROTOCOL = set(('_SEND', '_THROW', '_CLOSE'))
    _RESERVED     = set((
        'register', 'register_object', 'proc', 'task',
        'start', 'stop', 'serve', 'shutdown', 'reset',
        'connect', 'bind', 'bind_ports',
    )) | _GEN_PROTOCOL

    logger = getLogger('netcall.service')

    def __init__(self, *args, **kwargs):
        """
        Parameters
        ==========
        serializer : [optional] <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.

        service_id : [optional] <bytes>
        """
        service_id = kwargs.pop('service_id', None)

        super(RPCServiceBase, self).__init__(*args, **kwargs)

        self.service_id = service_id \
                       or ('%s/%s' % (self.__class__.__name__, self.identity)).encode()
        self.procedures = {}  # {<name>   : <callable>}
        self.generators = {}  # {<req_id> : <Generator>}

        # register extra class methods as service procedures
        self.register_object(self, restricted=self._RESERVED)

    def _parse_request(self, msg_list):
        """
        Parse a request
        (should not raise an exception)

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <ser_args>, <ser_kwargs>, <ignore>]

        Returns either a None or a dict {
            'route'  : [<id:bytes>, ...],  # list of all dealer ids (a return path)
            'req_id' : <id:bytes>,         # unique message id
            'proc'   : <callable>,         # a task callable
            'args'   : [<arg1>, ...],      # positional arguments
            'kwargs' : {<kw1>, ...},       # keyword arguments
            'ignore' : <bool>,             # ignore result flag
            'error'  : None or <Exception>
        }
        """
        logger = self.logger

        if len(msg_list) < 6 or b'|' not in msg_list:
            logger.error('bad request %r', msg_list)
            return None

        error    = None
        args     = None
        kwargs   = None
        ignore   = None
        boundary = msg_list.index(b'|')
        
        if sys.version_info.major < 3:
            name = msg_list[boundary+2]
        else:
            name = msg_list[boundary+2].decode()

        if name in self._GEN_PROTOCOL:
            proc = name
        else:
            proc = self.procedures.get(name, None)

        try:
            data = msg_list[boundary+3:boundary+5]
            args, kwargs = self._serializer.deserialize_args_kwargs(data)
            ignore       = bool(int(msg_list[boundary+5]))
        except Exception as e:
            error = e

        if proc is None:
            error = NotImplementedError("Unregistered procedure %r" % name)

        return dict(
            route  = msg_list[0:boundary],
            req_id = msg_list[boundary+1],
            proc   = proc,
            args   = args,
            kwargs = kwargs,
            ignore = ignore,
            error  = error,
        )

    def _build_reply(self, request, typ, data):
        """Build a reply message for status and data.

        Parameters
        ----------
        typ : bytes
            Either b'ACK', b'OK', b'YIELD' or b'FAIL'.
        data : list of bytes
            A list of data frame to be appended to the message.
        """
        return list(chain(
            request['route'],
            [b'|', request['req_id'], typ],
            data,
        ))


    def _send_reply(self, reply):
        """ Send a multipart reply to the ZMQ socket.

            Notice: reply is a list produced by self._build_reply()
        """
        self.logger.debug('sending %r', reply)
        self.socket.send_multipart(reply)

    def _send_ack(self, request):
        "Send an ACK notification"
        reply = self._build_reply(request, b'ACK', [self.service_id])
        self._send_reply(reply)

    def _send_ok(self, request, result):
        "Send an OK reply"
        data_list = self._serializer.serialize_result(result)
        reply = self._build_reply(request, b'OK', data_list)
        self._send_reply(reply)

    def _send_yield(self, request, result):
        "Send a YIELD reply"
        data_list = self._serializer.serialize_result(result)
        reply = self._build_reply(request, b'YIELD', data_list)
        self._send_reply(reply)

    def _send_fail(self, request, with_tb=True):
        "Send a FAIL reply"
        # take the current exception implicitly
        etype, evalue, tb = exc_info()
        error_dict = {
            'ename'     : etype.__name__,
            'evalue'    : str(evalue),
            'traceback' : with_tb and format_exc() or None
        }
        data_list = [jsonapi.dumps(error_dict)]
        reply = self._build_reply(request, b'FAIL', data_list)
        self._send_reply(reply)


    def _handle_request(self, msg_list):
        """
        Handle an incoming request.

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <serialized (args, kwargs)>]

        First, the service sends back a notification that the message was
        indeed received:

        [<id>..<id>, b'|', req_id, b'ACK',  service_id]

        Next, the actual reply depends on if the call was successful or not:

        [<id>..<id>, b'|', req_id, b'OK',    <serialized result>]
        [<id>..<id>, b'|', req_id, b'YIELD', <serialized result>]*
        [<id>..<id>, b'|', req_id, b'FAIL',  <JSON dict of ename, evalue, traceback>]

        Here the (ename, evalue, traceback) are utf-8 encoded unicode.

        In case of a YIELD reply, the client can send a _SEND, _THROW or
        _CLOSE messages with the same req_id as in the first message sent.
        The first YIELD reply will contain no result to signal the client it is a
        yield-generator. The first message sent by the client to a yield-generator
        must be a _SEND with None as argument.

        [<id>..<id>, b'|', req_id, '_SEND',  <serialized (sent value, None)>]
        [<id>..<id>, b'|', req_id, '_THROW', <serialized ([ename, evalue], None)>]
        [<id>..<id>, b'|', req_id, '_CLOSE', <serialized (None, None)>]

        The service will first send an ACK message. Then, it will send a YIELD
        reply whenever ready, or a FAIL reply in case an exception is raised.

        Termination of the yield-generator happens by throwing an exception.
        Normal termination raises a StopIterator. Termination by _CLOSE can
        raises a GeneratorExit or a StopIteration depending on the implementation
        of the yield-generator. Any other exception raised will also terminate
        the yield-generator.

        Note: subclasses can override this method if necessary.
        """
        req = self._parse_request(msg_list)
        if req is None:
            return
        self.logger.debug(
            'CALL: %s, args=%s, kwargs=%s, ignore=%s',
            req['proc'], req['args'], req['kwargs'], req['ignore']
        )
        self._send_ack(req)

        ignore = req['ignore']
        proc   = req['proc']

        try:
            # raise any parsing errors here
            if req['error']:
                raise req['error']

            if proc in self._GEN_PROTOCOL:
                self._handle_gen_protocol(req)
                return
            else:
                # call procedure
                res = proc(*req['args'], **req['kwargs'])
        except Exception as e:
            self.logger.error(e, exc_info=True)
            not ignore and self._send_fail(req)
        else:
            if ignore:
                return

            if isinstance(res, GeneratorType):
                self.generators[req['req_id']] = res
                self._send_yield(req, None)
            else:
                self._send_ok(req, res)

    def _handle_gen_protocol(self, req):
        """ Handles generator commands (_SEND, _THROW, _CLOSE).
            May raise all sorts of exceptions.
        """
        req_id = req['req_id']
        gen    = self.generators.get(req_id, None)
        if gen is None:
            raise ValueError('no corresponding generator (req_id=%r)' % req_id)

        cmd  = req['proc']
        args = req['args']

        try:
            if cmd == '_SEND':
                res = gen.send(args)
                self._send_yield(req, res)

            elif cmd == '_THROW':
                ex_class = getattr(exceptions, args[0], Exception)
                res = gen.throw(ex_class, args[1])
                self._send_yield(req, res)

            else:
                res = gen.close()
                raise GeneratorExit

        except (GeneratorExit, StopIteration):
            self.generators.pop(req_id, None)
            self._send_fail(req, with_tb=False)


    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def register(self, func=None, name=None):
        """ A decorator to register a callable as a service task.

            Examples:

            service = TornadoRPCService()

            @service.task
            def echo(s):
                return s

            @service.proc(name='work')
            def do_nothing():
                pass

            service.register(lambda: None, name='dummy')
        """
        if func is None:
            if name is None:
                raise ValueError("at least one argument is required")

            return partial(self.register, name=name)
        else:
            if not callable(func):
                raise ValueError("func argument should be callable")
            if name is None:
                name = func.__name__
            if name in self._RESERVED:
                raise ValueError("{} is a reserved function name".format(name))

            self.procedures[name] = func

        return func


    task = register  # alias
    proc = register  # alias

    def register_object(self, obj, restricted=[], namespace=''):
        """
        Register public functions of a given object as service tasks.
        Give the possibility to not register some restricted functions.
        Give the possibility to prefix the service name with a namespace.

        Example:

        class MyObj(object):
            def __init__(self, value):
                self._value = value
            def value(self):
                return self._value

        first = MyObj(1)
        service.register_object(first)

        second = MyObj(2)
        service.register_object(second, namespace='second')

        third = MyObj(3)
        # Actually register nothing
        service.register_object(third, namespace='third', restricted=['value'])

        # Register a full module
        import random
        service.register_object(random, namespace='random')

        ...

        client.value() # Returns 1
        client.second.value() # Returns 2
        client.third.value() # Exception NotImplementedError
        client.random.randint(10, 30) # Returns an int
        """
        for name in dir(obj):
            if name.startswith('_') or (name in restricted) or (name in self._RESERVED):
                continue
            try:    proc = getattr(obj, name)
            except: continue
            if callable(proc):
                self.procedures['.'.join([namespace, name]).lstrip('.')] = proc


    @abstractmethod
    def start(self):
        """ Start the service (non-blocking) """
        pass


    @abstractmethod
    def stop(self):
        """ Stop the service (non-blocking) """
        pass


    @abstractmethod
    def serve(self):
        """ Serve RPC requests (blocking) """
        pass


