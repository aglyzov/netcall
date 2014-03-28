# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}
"""
A base class for RPC services and proxies.

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

from sys       import exc_info
from abc       import ABCMeta, abstractmethod
from random    import randint
from traceback import format_exc
from itertools import chain
from functools import partial

import zmq
from zmq.utils import jsonapi

from .serializer import PickleSerializer
from .errors     import RemoteRPCError, RPCError
from .utils      import logger, RemoteMethod


#-----------------------------------------------------------------------------
# RPC base
#-----------------------------------------------------------------------------

class RPCBase(object):  #{
    __metaclass__ = ABCMeta

    def __init__(self, serializer=None, identity=None):  #{
        """Base class for RPC service and proxy.

        Parameters
        ==========
        serializer : [optional] <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        identity   : [optional] <bytes>
        """
        self.identity    = identity or b'%08x' % randint(0, 0xFFFFFFFF)
        self.socket      = None
        self._ready      = False
        self._serializer = serializer if serializer is not None else PickleSerializer()
        self.bound       = set()
        self.connected   = set()
        self.reset()
    #}
    @abstractmethod
    def _create_socket(self):  #{
        "A subclass has to create a socket here"
        self._ready = False
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def reset(self):  #{
        """Reset the socket/stream."""
        if self.socket is not None:
            self.socket.close(linger=0)
        self._create_socket()
        self._ready    = False
        self.bound     = set()
        self.connected = set()
    #}

    def shutdown(self):  #{
        """ Deallocate resources (cleanup)
        """
        logger.debug('closing the socket')
        self.socket.close(0)
    #}

    def bind(self, urls, only=False):  #{
        """Bind the service to a number of urls of the form proto://address"""
        if isinstance(urls, basestring):
            urls = [urls]

        urls  = set(urls)
        bound = self.bound

        fresh = urls - bound
        for url in fresh:
            self.socket.bind(url)
            bound.add(url)

        if only:
            stale = bound - urls
            for url in stale:
                try:    self.socket.unbind(url)
                except: pass
                bound.remove(url)

        self._ready = bool(bound)
    #}
    def connect(self, urls, only=False):  #{
        """Connect the service to a number of urls of the form proto://address"""
        if isinstance(urls, basestring):
            urls = [urls]

        urls      = set(urls)
        connected = self.connected

        fresh = urls - connected
        for url in fresh:
            self.socket.connect(url)
            connected.add(url)

        if only:
            stale = connected - urls
            for url in stale:
                try:    self.socket.disconnect(url)
                except: pass
                connected.remove(url)

        self._ready = bool(connected)
    #}

    def bind_ports(self, ip, ports):  #{
        """Try to bind a socket to the first available tcp port.

        The ports argument can either be an integer valued port
        or a list of ports to try. This attempts the following logic:

        * If ports==0, we bind to a random port.
        * If ports > 0, we bind to port.
        * If ports is a list, we bind to the first free port in that list.

        In all cases we save the eventual url that we bind to.

        This raises zmq.ZMQBindError if no free port can be found.
        """
        if isinstance(ports, int):
            ports = [ports]
        for p in ports:
            try:
                if p==0:
                    port = self.socket.bind_to_random_port("tcp://%s" % ip)
                else:
                    self.socket.bind("tcp://%s:%i" % (ip, p))
                    port = p
            except zmq.ZMQError:
                # bind raises this if the port is not free
                continue
            except zmq.ZMQBindError:
                # bind_to_random_port raises this if no port could be found
                continue
            else:
                break
        else:
            raise zmq.ZMQBindError('Could not find an available port')

        url = 'tcp://%s:%i' % (ip, port)
        self.bound.add(url)
        self._ready = True

        return port
    #}
#}


#-----------------------------------------------------------------------------
# RPC Service base
#-----------------------------------------------------------------------------

class RPCServiceBase(RPCBase):  #{

    _RESERVED = [
        'register', 'register_object', 'proc', 'task',
        'start', 'stop', 'serve', 'shutdown', 'reset',
        'connect', 'bind', 'bind_ports',
        'YIELD_SEND', 'YIELD_THROW', 'YIELD_CLOSE'
    ]

    def __init__(self, *args, **kwargs):  #{
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
                       or b'%s/%s' % (self.__class__.__name__, self.identity)
        self.procedures = {}  # {<name> : <callable>}

        # register extra class methods as service procedures
        self.register_object(self, restricted=self._RESERVED)
    #}
    def _parse_request(self, msg_list):  #{
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
        if len(msg_list) < 6 or b'|' not in msg_list:
            logger.error('bad request: %r' % msg_list)
            return None

        error    = None
        args     = None
        kwargs   = None
        ignore   = None
        boundary = msg_list.index(b'|')
        name     = msg_list[boundary+2]

        if name in ['YIELD_SEND', 'YIELD_THROW', 'YIELD_CLOSE']:
            proc = name
        else:
            proc = self.procedures.get(name, None)

        try:
            data = msg_list[boundary+3:boundary+5]
            args, kwargs = self._serializer.deserialize_args_kwargs(data)
            ignore       = bool(int(msg_list[boundary+5]))
        except Exception, e:
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
    #}
    def _build_reply(self, request, typ, data):  #{
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
    #}

    def _send_reply(self, reply):  #{
        """ Send a multipart reply to the ZMQ socket.

            Notice: reply is a list produced by self._build_reply()
        """
        logger.debug('sending %r' % reply)
        self.socket.send_multipart(reply)
    #}
    def _send_ack(self, request):  #{
        "Send an ACK notification"
        reply = self._build_reply(request, b'ACK', [self.service_id])
        self._send_reply(reply)
    #}
    def _send_ok(self, request, result):  #{
        "Send an OK reply"
        data_list = self._serializer.serialize_result(result)
        reply = self._build_reply(request, b'OK', data_list)
        self._send_reply(reply)
    #}
    def _send_yield(self, request, result):  #{
        "Send a YIELD reply"
        data_list = self._serializer.serialize_result(result)
        reply = self._build_reply(request, b'YIELD', data_list)
        self._send_reply(reply)
    #}
    def _send_fail(self, request):  #{
        "Send a FAIL reply"
        # take the current exception implicitly
        etype, evalue, tb = exc_info()
        error_dict = {
            'ename'     : str(etype.__name__),
            'evalue'    : str(evalue),
            'traceback' : format_exc(tb)
        }
        data_list = [jsonapi.dumps(error_dict)]
        reply = self._build_reply(request, b'FAIL', data_list)
        self._send_reply(reply)
    #}

    @abstractmethod
    def _handle_request(self, msg_list):  #{
        """
        Handle an incoming request.

        The request is received as a multipart message:

        [<id>..<id>, b'|', req_id, proc_name, <serialized args & kwargs>]

        First, the service sends back a notification that the message was
        indeed received:

        [<id>..<id>, b'|', req_id, b'ACK',  service_id]

        Next, the actual reply depends on if the call was successful or not:

        [<id>..<id>, b'|', req_id, b'OK',    <serialized result>]
        [<id>..<id>, b'|', req_id, b'YIELD', <serialized result>]*
        [<id>..<id>, b'|', req_id, b'FAIL',  <JSON dict of ename, evalue, traceback>]

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

        Note: subclasses have to override this method
        """
        pass
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def register(self, func=None, name=None):  #{
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
    #}

    task = register  # alias
    proc = register  # alias

    def register_object(self, obj, restricted=[], namespace=''):  #{
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
    #}

    @abstractmethod
    def start(self):  #{
        """ Start the service (non-blocking) """
        pass
    #}

    @abstractmethod
    def stop(self):  #{
        """ Stop the service (non-blocking) """
        pass
    #}

    @abstractmethod
    def serve(self):  #{
        """ Serve RPC requests (blocking) """
        pass
    #}
#}


#-----------------------------------------------------------------------------
# RPC Client base
#-----------------------------------------------------------------------------

class RPCClientBase(RPCBase):  #{
    """A service proxy to for talking to an RPCService."""

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
        logger.debug('sending %r' % request)
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
        This function is made to be reused by subclasses.
        recv_generator should be a generator yielding tuples of (_, data).
        That is, the first element of the tuple is ignored.
        recv_generator should NOT yield the data from the very first YIELD reply.
        """

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
#}

