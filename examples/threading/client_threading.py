#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

""" A simple asynchronous RPC client that shows how to:

    * use specific serializer
    * make multiple RPC calls at the same time using the Python threading API
    * handle remote exceptions
    * do load balancing
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from netcall.threading import ThreadingRPCClient, ThreadPool
from netcall import RemoteRPCError, RPCTimeoutError, JSONSerializer

def printer(msg, func, *args):
    "run a function, print results"
    print msg, '<request>'
    res = func(*args)
    print msg, '<response>', res

if __name__ == '__main__':
    #from netcall import setup_logger
    #setup_logger()

    pool  = ThreadPool(128)
    spawn = lambda f, *ar, **kw: pool.schedule(f, args=ar, kwargs=kw)

    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = ThreadingRPCClient(pool=pool, serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')

    tasks = [spawn(printer, "[echo] Echoing \"Hi there\"", echo.echo, "Hi there")]

    try:
        print "Testing a remote exception...",
        echo.error()
        print "FAIL, no remote exception!"
    except RemoteRPCError, e:
        print "OK, got an expected remote exception:"
        #print e.ename
        print e.evalue
        print e.traceback

    try:
        print "Testing a timeout...",
        echo.call('sleep', args=[2.3], timeout=1.1)
        print "FAIL, timeout didn't work!"
    except RPCTimeoutError, e:
        print "OK, got an expected timeout:"
        print repr(e)
        print

    print 'Ignoring result... ',
    echo.call('error', ignore=True)
    print 'OK\n'

    tasks.append(spawn(printer, "[echo] Sleeping for 2 seconds...", echo.sleep, 2.0))

    math = ThreadingRPCClient(pool=pool)
    # By connecting to two instances, requests are load balanced.
    math.connect('tcp://127.0.0.1:5556')
    math.connect('tcp://127.0.0.1:5557')

    for i in range(5):
        for j in range(5):
            tasks.append(
                spawn(printer, "[math] Adding: %s + %s" % (i, j), math.add, i, j)
            )

    for task in tasks:
        task.wait()

