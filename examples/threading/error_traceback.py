#!/usr/bin/env python
# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

""" A simple asynchronous RPC client that shows how to:

    * use specific serializer
    * handle remote exceptions
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from netcall.concurrency import get_tools
from netcall.threading   import ThreadingRPCClient
from netcall             import RemoteRPCError, RPCTimeoutError, JSONSerializer

def printer(msg, func, *args):
    "run a function, print results"
    print msg, '<request>'
    res = func(*args)
    print msg, '<response>', res

if __name__ == '__main__':
    #from netcall import setup_logger
    #setup_logger()

    tools    = get_tools(env=None)
    executor = tools.Executor(16)
    spawn    = executor.submit

    # Custom serializer/deserializer functions can be passed in. The server
    # side ones must match.
    echo = ThreadingRPCClient(executor=executor, serializer=JSONSerializer())
    echo.connect('tcp://127.0.0.1:5555')

    print "Testing a remote exception...",
    echo.error()

