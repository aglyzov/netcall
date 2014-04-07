import re
from collections import namedtuple
from functools import partial
from logging import getLogger

from ..base  import RPCBase
from ..utils import detect_green_env, get_zmq_classes, get_green_tools

class DeviceSocket(RPCBase):
    logger = getLogger('netcall.devices')
    
    def __init__(self, socket_type, env=None, context=None, **kwargs):
        env = env or detect_green_env() or 'gevent'
        Context, self.Poller = get_zmq_classes(env)
        self._tools = get_green_tools(env) # While waiting for get_concurrent_tools() we use greenlets by default

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
        
        self._tools = get_green_tools(env) # While waiting for get_concurrent_tools() we use greenlets by default
        
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
        spawn = self._tools[0]
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
        
