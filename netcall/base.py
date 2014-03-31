# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}
"""
A base class for RPC services and proxies.

Authors:

* Brian Granger
* Alexander Glyzov
* Axel Voitier

"""
#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov,
#  Axel Voitier
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from abc    import ABCMeta, abstractmethod
from random import randint

import zmq

from .serializer import PickleSerializer
from .utils      import logger


#-----------------------------------------------------------------------------
# RPC base
#-----------------------------------------------------------------------------

class RPCBase(object):  #{
    __metaclass__ = ABCMeta

    logger = logger

    def __init__(self, serializer=None, identity=None):  #{
        """Base class for RPC service and proxy.

        Parameters
        ==========
        serializer : [optional] <Serializer>
            An instance of a Serializer subclass that will be used to serialize
            and deserialize args, kwargs and the result.
        identity   : [optional] <bytes>
        """
        self.identity    = identity or b'%08x' % randint(0, 0xFFFFFFFF)
        self.socket      = None
        self._ready      = False
        self._serializer = serializer if serializer is not None else PickleSerializer()
        self.bound       = set()
        self.connected   = set()
        self.reset()
    #}
    @abstractmethod
    def _create_socket(self):  #{
        "A subclass has to create a socket here"
        self._ready = False
    #}

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------

    def reset(self):  #{
        """Reset the socket/stream."""
        if self.socket is not None:
            self.socket.close(linger=0)
        self._create_socket()
        self._ready    = False
        self.bound     = set()
        self.connected = set()
    #}

    def shutdown(self):  #{
        """ Deallocate resources (cleanup)
        """
        self.logger.debug('closing the socket')
        self.socket.close(0)
    #}

    def bind(self, urls, only=False):  #{
        """Bind the service to a number of urls of the form proto://address"""
        if isinstance(urls, basestring):
            urls = [urls]

        urls  = set(urls)
        bound = self.bound

        fresh = urls - bound
        for url in fresh:
            self.socket.bind(url)
            bound.add(url)

        if only:
            stale = bound - urls
            for url in stale:
                try:    self.socket.unbind(url)
                except: pass
                bound.remove(url)

        self._ready = bool(bound)
    #}
    def connect(self, urls, only=False):  #{
        """Connect the service to a number of urls of the form proto://address"""
        if isinstance(urls, basestring):
            urls = [urls]

        urls      = set(urls)
        connected = self.connected

        fresh = urls - connected
        for url in fresh:
            self.socket.connect(url)
            connected.add(url)

        if only:
            stale = connected - urls
            for url in stale:
                try:    self.socket.disconnect(url)
                except: pass
                connected.remove(url)

        self._ready = bool(connected)
    #}

    def bind_ports(self, ip, ports):  #{
        """Try to bind a socket to the first available tcp port.

        The ports argument can either be an integer valued port
        or a list of ports to try. This attempts the following logic:

        * If ports==0, we bind to a random port.
        * If ports > 0, we bind to port.
        * If ports is a list, we bind to the first free port in that list.

        In all cases we save the eventual url that we bind to.

        This raises zmq.ZMQBindError if no free port can be found.
        """
        if isinstance(ports, int):
            ports = [ports]
        for p in ports:
            try:
                if p==0:
                    port = self.socket.bind_to_random_port("tcp://%s" % ip)
                else:
                    self.socket.bind("tcp://%s:%i" % (ip, p))
                    port = p
            except zmq.ZMQError:
                # bind raises this if the port is not free
                continue
            except zmq.ZMQBindError:
                # bind_to_random_port raises this if no port could be found
                continue
            else:
                break
        else:
            raise zmq.ZMQBindError('Could not find an available port')

        url = 'tcp://%s:%i' % (ip, port)
        self.bound.add(url)
        self._ready = True

        return port
    #}
#}
