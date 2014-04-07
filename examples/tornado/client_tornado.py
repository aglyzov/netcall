#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

""" A simple asynchronous RPC client that shows how to:

    * use specific serializer
    * make multiple asychronous RPC calls at the same time using IOLoop
    * handle remote exceptions
    * do load balancing
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from zmq.eventloop import ioloop
ioloop.install()

from tornado import gen
from netcall.tornado import TornadoRPCClient, JSONSerializer, RemoteRPCError


def spawn(coro, *args, **kwargs):
    """ Runs a coroutine, gets a <Future> as its result,
        registers this result in ioloop
    """
    loop = ioloop.IOLoop.instance()
    loop.add_future(coro(*args, **kwargs), lambda f: "ignore future result")

@gen.coroutine
def printer(client, name, *args, **kwargs):
    print '<request>', name, args, kwargs
    try:
        future = client.call(name, *args, **kwargs)
        if future is None:
            res = None
        else:
            res = yield future
    except RemoteRPCError, e:
        print "Got a remote exception:"
        print e.ename
        print e.evalue
        print e.traceback
    except Exception, e:
        print "Got an exception:"
        print repr(e)
    else:
        print '<reply>', name, args, kwargs, '-->', res

def print_error(err):
    print "Got error:", err

if __name__ == '__main__':
    #from logging import getLogger, DEBUG
    #logger = getLogger('netcall')
    #logger.setLevel(DEBUG)

    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = TornadoRPCClient(serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')

    spawn(printer, echo, 'echo', ["Hi there"])
    spawn(printer, echo, 'error')
    spawn(printer, echo, 'error', ignore=True)

    # Sleep for 2.3 sec but timeout after 1.1 sec.
    spawn(printer, echo, 'sleep', [2.3], timeout=1.1)

    math = TornadoRPCClient()
    # By connecting to two instances, requests are load balanced.
    math.connect('tcp://127.0.0.1:5556')
    math.connect('tcp://127.0.0.1:5557')

    for i in range(5):
        for j in range(5):
            spawn(printer, math, 'add', [i, j])

    loop = ioloop.IOLoop.instance()
    loop.add_timeout(loop.time()+3, loop.stop)
    loop.start()

