#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=indent

"""
A simple RPC server that shows how to:

* start several worker processes
* use zmq proxy device to load balance requests to the workers
* make each worker to serve multiple RPC services asynchronously
  using the Python Threading multitasking

"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE, distributed as part of this software.
#-----------------------------------------------------------------------------


from os              import getpid
from time            import sleep
from multiprocessing import Process, cpu_count

from zmq               import ROUTER, DEALER
from zmq.devices       import ThreadProxy
from netcall.threading import ThreadingRPCService, JSONSerializer
from netcall.utils     import get_zmq_classes


class EchoService(ThreadingRPCService):

    def echo(self, s):
        print "<pid:%s> %r echo %r" % (getpid(), self.connected, s)
        return s

    def sleep(self, t):
        print "<pid:%s> %r sleep %s" % (getpid(), self.connected, t)
        sleep(t)

    def error(self):
        raise ValueError('raising ValueError for fun!')

class MathService(ThreadingRPCService):

    def add(self, a, b):
        print "<pid:%s> %r add %r %r" % (getpid(), self.connected, a, b)
        return a+b

    def subtract(self, a, b):
        print "<pid:%s> %r subtract %r %r" % (getpid(), self.connected, a, b)
        return a-b

    def multiply(self, a, b):
        print "<pid:%s> %r multiply %r %r" % (getpid(), self.connected, a, b)
        return a*b

    def divide(self, a, b):
        print "<pid:%s> %r divide %r %r" % (getpid(), self.connected, a, b)
        return a/b

class Worker(Process):

    def run(self):
        # Multiple RPCService instances can be run in a single process
        # via Python Threads
        Context, _ = get_zmq_classes()
        context = Context()

        # Custom serializer/deserializer functions can be passed in. The server
        # side ones must match.
        echo = EchoService(context=context, serializer=JSONSerializer())
        echo.connect('ipc:///tmp/rpc-demo-echo.service')

        # We create two Math services to simulate load balancing. A client can
        # connect to both of these services and requests will be load balanced.
        math1 = MathService(context=context)
        math1.connect('ipc:///tmp/rpc-demo-math1.service')

        math2 = MathService(context=context)
        math2.connect('ipc:///tmp/rpc-demo-math2.service')

        # Next we spawn service greenlets and wait for them to exit
        echo  .start()
        math1 .start()
        math2 .start()

        echo  .serve()
        math1 .serve()
        math2 .serve()


if __name__ == '__main__':
    workers = [Worker() for _ in range(cpu_count())]
    for w in workers:
        w.start()

    echo_proxy  = ThreadProxy(ROUTER, DEALER)
    math1_proxy = ThreadProxy(ROUTER, DEALER)
    math2_proxy = ThreadProxy(ROUTER, DEALER)

    echo_proxy  .bind_in('tcp://127.0.0.1:5555')
    math1_proxy .bind_in('tcp://127.0.0.1:5556')
    math2_proxy .bind_in('tcp://127.0.0.1:5557')

    echo_proxy  .bind_out('ipc:///tmp/rpc-demo-echo.service')
    math1_proxy .bind_out('ipc:///tmp/rpc-demo-math1.service')
    math2_proxy .bind_out('ipc:///tmp/rpc-demo-math2.service')

    echo_proxy  .start()
    math1_proxy .start()
    math2_proxy .start()

    while True:
        echo_proxy  .join(0.1)
        math1_proxy .join(0.1)
        math2_proxy .join(0.1)

