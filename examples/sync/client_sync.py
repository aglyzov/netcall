#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

""" A simple RPC client that shows how to:

    * use specific serializer
    * make synchronous RPC calls
    * handle remote exceptions
    * do load balancing
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from netcall import SyncRPCClient, RemoteRPCError, RPCTimeoutError, JSONSerializer


if __name__ == '__main__':
    #from netcall import setup_logger
    #setup_logger()

    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = SyncRPCClient(serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')
    print "Echoing: ", echo.echo("Hi there")
    try:
        echo.error()
    except RemoteRPCError, e:
        print "Got an expected remote exception:"
        print e.ename
        print e.evalue
        print e.traceback

    try:
        echo.call('sleep', args=[2.3], timeout=1.1)
    except RPCTimeoutError, e:
        print "Got an expected timeout:"
        print repr(e)

    echo.call('error', ignore=True)

    math = SyncRPCClient()
    # By connecting to two instances, requests are load balanced.
    math.connect('tcp://127.0.0.1:5556')
    math.connect('tcp://127.0.0.1:5557')
    for i in range(5):
        for j in range(5):
            print "Adding: ", i, j, '=', math.add(i,j)

