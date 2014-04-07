#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

""" A simple RPC server that shows how to run multiple RPC services
    concurrently using the standard Python threading API
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from time import sleep

from netcall import ThreadingRPCService, JSONSerializer


# Custom serializer/deserializer can be set up upon initialization.
# Obviously it must match on both client and server.
echo = ThreadingRPCService(serializer=JSONSerializer())


@echo.task(name='echo')
def echo_echo(s):
    print "%r echo %r" % (echo.bound, s)
    return s

@echo.task(name='sleep')
def echo_sleep(t):
    print "%r sleep %s" % (echo.bound, t)
    sleep(t)
    print "end of sleep"
    return t

@echo.task(name='error')
def echo_error():
    raise ValueError('raising ValueError for fun!')


class Math(ThreadingRPCService):

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
    echo.bind('tcp://127.0.0.1:5555')

    # We create two Math services to simulate load balancing. A client can
    # connect to both of these services and requests will be load balanced.
    math1 = Math()
    math2 = Math()

    math1.bind('tcp://127.0.0.1:5556')
    math2.bind('tcp://127.0.0.1:5557')

    # another way of defining tasks
    math2.register(echo_error, name='error')
    math2.register(echo_sleep, name='sleep')

    # now we spawn service threads and wait for them to exit
    echo.start()
    math1.start()
    math2.start()

    echo.serve()
