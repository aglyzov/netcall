# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
NetCall - A simple Python RPC system using ZeroMQ as a transport

Authors:

* Brian Granger
* Alexander Glyzov

Example
-------

To create a simple service:

    from netcall import ThreadingRPCService

    echo = ThreadingRPCService()

    @echo.task
    def echo(self, s):
        return s

    echo.bind('tcp://127.0.0.1:5555')
    echo.start()
    echo.serve()

To talk to this service:

    from netcall import ThreadingRPCClient

    p = ThreadingRPCClient()
    p.connect('tcp://127.0.0.1:5555')
    p.echo('Hi there')
    'Hi there'
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

# Notice:
# for tornado versions of the classes import netcall.tornado
# for gevent versions of the classes import netcall.green

from .utils        import logger, RemoteMethod, get_zmq_classes, detect_green_env
from .errors       import RPCError, RemoteRPCError, RPCTimeoutError
from .serializer   import *

from .base_client  import RPCClientBase
from .base_service import RPCServiceBase

from .sync         import SyncRPCClient
from .threading    import ThreadingRPCService, ThreadingRPCClient, ThreadingRPCLoadBalancer

