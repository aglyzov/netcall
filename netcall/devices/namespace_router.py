from collections import namedtuple
from logging   import getLogger

from zmq          import ROUTER, DEALER

from .base_device import BaseDevice

class NamespaceRouter(BaseDevice, namedtuple('NamespaceRouter', 'client service')):
    logger = getLogger('netcall.namespace_router')

    __slots__ = ()
    
    socket_types = [ROUTER, ROUTER]
        
    def __init__(self, **kwargs):
        super(NamespaceRouter, self).__init__(**kwargs)
        
        self._handlers.append((self.client.socket, self._handle_client_to_service))
        self._handlers.append((self.service.socket, self._handle_service_to_client))
        
    def _handle_client_to_service(self, s_client, s_service):
        data = s_client.recv_multipart()
        self.logger.debug('received from client: %r', data)
        
        boundary = data.index(b'|')
        calling = data[boundary+2]
        
        new_route = calling.split('.')[0]
        new_call = '.'.join(calling.split('.')[1:])
        data[boundary+2] = new_call
        data = [new_route] + data
        
        self.logger.debug('sending to service: %r', data)
        s_service.send_multipart(data)
        
    def _handle_service_to_client(self, s_client, s_service):
        data = s_service.recv_multipart()
        self.logger.debug('received from service: %r', data)
        
        data = data[1:]
        
        self.logger.debug('sending to client: %r', data)
        s_client.send_multipart(data)
        
