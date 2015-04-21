# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

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

class RPCClientBase(RPCBase):
    """An RPC Client (base class)"""

    logger = getLogger('netcall.client')

    def _create_socket(self):
        super(RPCClientBase, self)._create_socket()

        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, self.identity)

    def _build_request(self, method, args, kwargs, ignore=False, req_id=None):
        req_id = req_id or ('%x' % randint(0, 0xFFFFFFFF)).encode()
        method = method.encode()
        msg_list = [b'|', req_id, method]
        data_list = self._serializer.serialize_args_kwargs(args, kwargs)
        msg_list.extend(data_list)
        msg_list.append(str(int(ignore)).encode())
        return req_id, msg_list

    def _send_request(self, request):
        self.logger.debug('sending %r', request)
        self.socket.send_multipart(request)

    def _parse_reply(self, msg_list):
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
            logger.error('bad reply %r', msg_list)
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
            except Exception as e:
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
            except Exception as e:
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

    def _generator(self, req_id, get_val_exc):
        """ Mirrors a service generator on a client side
        """
        #logger = self.logger

        def _send_cmd(cmd, args):
            _, msg_list = self._build_request(
                cmd, args, None, ignore=False, req_id=req_id
            )
            self._send_request(msg_list)

        _send_cmd('_SEND', None)

        while True:
            val, exc = get_val_exc()
            if exc is not None:
                raise exc
            try:
                res = yield val
            except GeneratorExit:
                _send_cmd('_CLOSE', None)
            except:
                etype, evalue, _ = exc_info()
                _send_cmd('_THROW', [etype.__name__, evalue])
            else:
                _send_cmd('_SEND', res)

    def __getattr__(self, name):
        return RemoteMethod(self, name)

    def call(self, proc_name, args=[], kwargs={}, result='sync', timeout=None):
        """
        Call the remote method with *args and **kwargs
        (may raise an exception)

        Parameters
        ----------
        proc_name : <bytes> name of the remote procedure to call
        args      : <tuple> positional arguments of the remote procedure
        kwargs    : <dict>  keyword arguments of the remote procedure
        result    : 'sync' | 'async' | 'ignore'
        timeout   : <float> | None
            Number of seconds to wait for a reply.
            RPCTimeoutError is raised in case of timeout.
            Set to None, 0 or a negative number to disable.

        Returns
        -------
        <result:object> if result is 'sync'
        <Future>        if result is 'async'
        None            if result is 'ignore'

        If remote call fails:
        - raises <RemoteRPCError>                 if result is 'sync'
        - sets <RemoteRPCError> into the <Future> if result is 'async'
        """
        assert result in ('sync', 'async', 'ignore'), \
            'expected any of "sync", "async", "ignore" -- got %r' % result

        if not (timeout is None or isinstance(timeout, (int, float))):
            raise TypeError("timeout param: <float> or None expected, got %r" % timeout)

        if not self._ready:
            raise RuntimeError('bind or connect must be called first')

        ignore = result == 'ignore'
        req_id, msg_list = self._build_request(proc_name, args, kwargs, ignore)

        self._send_request(msg_list)

        if ignore:
            return None

        future = self._tools.Future()
        self._futures[req_id] = future

        if result == 'sync':
            # block waiting for a reply passed by _reader
            return future.result(timeout=timeout)
        else:
            # async
            return future

