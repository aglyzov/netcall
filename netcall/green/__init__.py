"""
Gevent versions of RPC client, service and load-balancer

Authors:

* Brian Granger
* Alexander Glyzov

Example
-------

To create a simple service::

    from netcall.green import GreenRPCService

    echo = GreenRPCService()

    @echo.task
    def echo(self, s):
        return s

    echo = Echo()
    echo.bind('tcp://127.0.0.1:5555')
    echo.start()
    echo.serve()

To talk to this service::

    from netcall.green import GreenRPCClient

    p = GreenRPCClient()
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

from ..base_service import RPCServiceBase
from ..utils        import RemoteMethod
from ..errors       import RPCError, RemoteRPCError, RPCTimeoutError
from ..serializer   import *

from .client   import GreenRPCClient
from .service  import GreenRPCService
from .balancer import GreenRPCLoadBalancer

