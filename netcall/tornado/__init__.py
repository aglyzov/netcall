# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

"""
Tornado versions of RPC service and client

Authors:

* Brian Granger
* Alexander Glyzov

Example
-------

To create a simple service::

    from netcall.tornado import TornadoRPCService

    echo = TornadoRPCService()

    @echo.task
    def echo(self, s):
        return s

    echo.bind('tcp://127.0.0.1:5555')
    echo.start()
    echo.serve()

To talk to this service::

    from netcall.tornado import TornadoRPCClient

    p = TornadoRPCClient()
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

from .service import TornadoRPCService
from .client  import TornadoRPCClient
