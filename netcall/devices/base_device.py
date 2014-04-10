"""
Base classes for devices.

Authors:

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

import re
from collections import namedtuple
from functools   import partial
from logging     import getLogger

from ..base        import RPCBase
from ..concurrency import get_tools
from ..utils       import detect_green_env, get_zmq_classes

#-----------------------------------------------------------------------------
# Base device classes
#-----------------------------------------------------------------------------


class DeviceSocket(RPCBase):
    """
    A DeviseSocket is basically a subclass of RPCBase, thus benefiting from 
    all its bind/connect methods. The socket type is given in the constructor.
    
    Also, all the methods of ZMQ's Socket are made available directly on this
    object.
    """
    logger = getLogger('netcall.devices')
    
    def __init__(self, socket_type, env=None, context=None, **kwargs):
        env = env or detect_green_env() or 'gevent'
        Context, self.Poller = get_zmq_classes(env)

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context
        
        self.socket_type = socket_type # A zmq socket type (eg. ROUTER, SUB)
            
        super(DeviceSocket, self).__init__(**kwargs)
    
    def _create_socket(self):  #{
        super(DeviceSocket, self)._create_socket()
        self.socket = self.context.socket(self.socket_type)
        self.socket.identity = self.identity
        
    def __getattr__(self, attr):
        return getattr(self.socket, attr)
        
class BaseDevice(object):
    """
    A BaseDevice does all the heavy lifting for implementing devices, ie. an 
    object connecting one or more socket inputs to one or more socket outputs.
    The difference with the original ZMQ device/proxy is that the subclasses of
    BaseDevice get to control how data are handled.
    
    To subclass BaseDevice you need some minimal boiler plate. First, in
    addition to extend BaseDevice itself, it also has to extend a namedtuple
    listing the name of the sockets. Their order of declaration is important
    for later as well.
    You will then be able to access the sockets directly by their names as 
    members of the object. Or by iterating over. Or by accessing the object has
    an array.
    
    You define the socket types by setting a class member named socket_types.
    It should be an array of ZMQ's socket types in their order of declaration
    in the namedtuple.
    
    You can pass keyword arguments to BaseDevice. All arguments recognized by
    DeviceSocket/RPCBase are available and will be passed to each declared
    sockets. In addition, if you want to specify arguments to be only for a
    particular socket (eg. identity), prefix the argument name with
    'socketname_' where socketname would be a name given in the namedtuple.
    
    To bind/connect the sockets, call the DeviceSocket/RPCBase methods for it
    by suffixing them with '_socketname'.
    Alternatively, you can also directly call ZMQ methods on the socket
    themselves.
    
    The BaseDevice will handle the creation, start and management of worker
    threads/greenlets associated to each data-flow you will declare.
    For each socket you wish to read from and process data, create a function
    doing the inputs/outputs, and register it with self._handlers.append().
    You should append a tuple (socket, function). When called, the function
    will receives all the ZMQ sockets in their order of declaration in the
    namedtuple. Alternatively, you can also directly use self.socketname.
    BaseDevice will take care of polling on the sockets if necessary (when
    using threads), and looping over.
    
    Example (this is effectively a load balancer device):
    > from collections import namedtuple
    > import zmq
    > from netcall.devices import BaseDevice
    > 
    > class MyDevice(BaseDevice, namedtuple('MyDevice', 'client service')):
    >     __slots__ = () # In order to save memory from namedtuple
    >     socket_types = [zmq.ROUTER, zmq.DEALER]
    >
    >     def __init__(self, **kwargs):
    >         super(MyDevice, self).__init__(**kwargs)
    >         self._handlers.append(
    >             (self.client.socket, self._handle_client_to_service))
    >         self._handlers.append(
    >             (self.service.socket, self._handle_service_to_client))
    >
    >     def _handle_client_to_service(self, s_client, s_service):
    >         s_service.send_multipart(s_client.recv_multipart())
    >    
    >     def _handle_service_to_client(self, s_client, s_service):
    >         s_client.send_multipart(s_service.recv_multipart())
    >
    > my_device = MyDevice(client_identity='myclient', env='gevent')
    > my_device.bind_client('tcp://127.0.0.1:5000') # From RPCBase
    > my_device.service.connect('tcp://192.168.1.12:6000) # From ZMQ
    >
    > my_device.start() # Non-blocking
    > my_device.serve() # Blocking
    > ...
    > my_device.shutdown()
    """
    def __new__(cls, **kwargs):
        devices = []
        devices_args = []
        to_remove = []
        for i, field in enumerate(cls._fields):
            devices_args.append(dict(kwargs))
            for arg in kwargs:
                match = re.match('%s_(.+)' % field, arg)
                if match:
                    devices_args[i][match.group(1)] = devices_args[i][arg]
                    to_remove.append(arg)
                    
        for i, _ in enumerate(cls._fields):
            for arg in to_remove:
                del devices_args[i][arg]
            devices.append(DeviceSocket(cls.socket_types[i], **devices_args[i]))
        
        return super(BaseDevice, cls).__new__(cls, *devices)
    
    def __init__(self, env=None, **kwargs):
        kwargs['env'] = env
        super(BaseDevice, self).__init__(**kwargs)
        
        self._tools = get_tools(env=env) # While waiting for get_concurrent_tools() we use greenlets by default
        
        for field, socket in zip(self._fields, self):
            bind_func = partial(self.__bind, socket)
            bind_func.__doc__ = self.__bind.__doc__ % field
            setattr(self, 'bind_' + field, bind_func)
            
            connect_func = partial(self.__connect, socket)
            connect_func.__doc__ = self.__connect.__doc__ % field
            setattr(self, 'connect_' + field, connect_func)
            
            bind_ports_func = partial(self.__bind_ports, socket)
            bind_ports_func.__doc__ = self.__bind_ports.__doc__ % field
            setattr(self, 'bind_ports_' + field, bind_ports_func)
            
        self._handlers = [] # List of (socket, handler_function)
        self.greenlets = []
        
    def _run_greenlet(self):
        spawn = self._tools.Executor().submit
        self.running = True
        
        def _loop(handler):
            while self.running:
                handler(*self)
              
        for _, handler in self._handlers:
            self.greenlets.append(spawn(_loop, handler))
            
        return self.greenlets
        
    def _run_threading(self): # Not used yet
        poller = get_zmq_classes()[1]()
        
        for socket, _ in self._handlers:
            poller.register(socket, POLLIN)
            poller.register(socket, POLLIN)
        
        self.running = True
        while self.running:
            #self.logger.debug('polling')
            events = dict(poller.poll())
            for socket, handler in self._handlers:
                if socket in events:
                    handler(*self)

    #-------------------------------------------------------------------------
    # Public API
    #-------------------------------------------------------------------------
        
    def start(self):
        return self._run_greenlet()
        
    def serve(self):
        if not self.greenlets:
            self.start()

        for greenlet in self.greenlets:
            greenlet.join()
        
    def stop(self):
        self.running = False
        for greenlet in self.greenlets:
            greenlet.join()
        # TODO: reset + restore connects/binds
        
    def reset(self):
        "Reset the sockets."
        for socket in self:
            socket.reset()

    def shutdown(self):
        "Deallocate resources (cleanup)"
        self.stop()
        for socket in self:
            socket.shutdown()

    def __bind(self, socket, urls, only=False):
        "Bind the %s socket to a number of urls of the form proto://address"
        socket.bind(urls, only)
    
    def __connect(self, socket, urls, only=False):
        "Connect the %s socket to a number of urls of the form proto://address"
        socket.connect(urls, only)

    def __bind_ports(self, socket, ip, ports):
        """Try to bind the %s socket to the first available tcp port.

        The ports argument can either be an integer valued port
        or a list of ports to try. This attempts the following logic:

        * If ports==0, we bind to a random port.
        * If ports > 0, we bind to port.
        * If ports is a list, we bind to the first free port in that list.

        In all cases we save the eventual url that we bind to.

        This raises zmq.ZMQBindError if no free port can be found.
        """
        socket.bind_ports(ip, ports)
        
