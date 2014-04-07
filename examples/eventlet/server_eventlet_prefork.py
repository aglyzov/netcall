#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=indent

"""
A simple RPC server that shows how to:

* start several worker processes
* use zmq proxy device to load balance requests to the workers
* make each worker to serve multiple RPC services asynchronously
  using Eventlet cooperative multitasking

"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE, distributed as part of this software.
#-----------------------------------------------------------------------------

from eventlet import sleep as green_sleep, spawn

from os              import getpid
from multiprocessing import Process, cpu_count

from zmq           import ROUTER, DEALER
from netcall.green import GreenRPCService, JSONSerializer
from netcall.utils import get_zmq_classes, green_device


class EchoService(GreenRPCService):
    def __init__(self, **kwargs):
        kwargs['green_env'] = 'eventlet'
        super(EchoService, self).__init__(**kwargs)

    def echo(self, s):
        print "<pid:%s> %r echo %r" % (getpid(), self.connected, s)
        return s

    def sleep(self, t):
        print "<pid:%s> %r sleep %s" % (getpid(), self.connected, t)
        green_sleep(t)

    def error(self):
        raise ValueError('raising ValueError for fun!')

class MathService(GreenRPCService):
    def __init__(self, **kwargs):
        kwargs['green_env'] = 'eventlet'
        super(MathService, self).__init__(**kwargs)

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
        # via Greenlets (Eventlet cooperative multitasking)
        Context, _ = get_zmq_classes(env='eventlet')
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
        services = [
            echo.start(),
            math1.start(),
            math2.start(),
        ]
        for service in services:
            service.wait()


if __name__ == '__main__':
    from netcall.utils import setup_logger
    setup_logger()

    workers = [Worker() for _ in range(cpu_count())]
    for w in workers:
        w.start()

    Context, _ = get_zmq_classes(env='eventlet')
    context = Context()

    echo_inp  = context.socket(ROUTER)
    math1_inp = context.socket(ROUTER)
    math2_inp = context.socket(ROUTER)

    echo_out  = context.socket(DEALER)
    math1_out = context.socket(DEALER)
    math2_out = context.socket(DEALER)

    echo_inp  .bind('tcp://127.0.0.1:5555')
    math1_inp .bind('tcp://127.0.0.1:5556')
    math2_inp .bind('tcp://127.0.0.1:5557')

    echo_out  .bind('ipc:///tmp/rpc-demo-echo.service')
    math1_out .bind('ipc:///tmp/rpc-demo-math1.service')
    math2_out .bind('ipc:///tmp/rpc-demo-math2.service')

    devices = [
        spawn(green_device, echo_inp,  echo_out,  env='eventlet'),
        spawn(green_device, math1_inp, math1_out, env='eventlet'),
        spawn(green_device, math2_inp, math2_out, env='eventlet'),
    ]
    for device in devices:
        device.wait()

