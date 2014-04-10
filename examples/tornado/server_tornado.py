#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""A simple RPC server that show how to run multiple RPC services."""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file COPYING.BSD, distributed as part of this software.
#-----------------------------------------------------------------------------

from zmq.eventloop import ioloop
ioloop.install()

from tornado import gen
from netcall.tornado import TornadoRPCService, JSONSerializer


# Custom serializer/deserializer can be set up upon initialization.
# Obviously it must match on a client and server.
echo = TornadoRPCService(serializer=JSONSerializer())


@echo.task(name='echo')
def echo_echo(s):
    print "%r echo %r" % (echo.bound, s)
    return s

@echo.task(name='sleep')
@gen.coroutine
def echo_sleep(t):
    print "%r sleep %s" % (echo.bound, t)
    loop = echo.ioloop
    yield gen.Task(loop.add_timeout, loop.time()+t)
    print "end of sleep"
    raise gen.Return(t)

@echo.task(name='error')
def echo_error():
    raise ValueError('raising ValueError for fun!')


class Math(TornadoRPCService):

    def add(self, a, b):
        print "%r add %r %r" % (self.bound, a, b)
        return a+b

    def subtract(self, a, b):
        print "%r subtract %r %r" % (self.bound, a, b)
        return a-b

    def multiply(self, a, b):
        print "%r multiply %r %r" % (self.bound, a, b)
        return a*b

    def divide(self, a, b):
        print "%r divide %r %r" % (self.bound, a, b)
        return a/b


if __name__ == '__main__':
    # Multiple RPCService instances can be run in a single process
    # by registering async callbacks into a polling event loop (Tornado IOLoop).

    echo.bind('tcp://127.0.0.1:5555');  echo.start()

    # We create two Math services to simulate load balancing. A client can
    # connect to both of these services and requests will be load balanced.
    math1 = Math()
    math2 = Math()

    math1.bind('tcp://127.0.0.1:5556');  math1.start()
    math2.bind('tcp://127.0.0.1:5557');  math2.start()

    # another way of defining tasks
    math2.register(echo_error, name='error')
    math2.register(echo_sleep, name='sleep')

    echo.serve()  # this calls echo.ioloop.start() and since ioloop is
                  # the same for all services they all will be served
                  # cooperatively
