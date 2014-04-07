from collections import namedtuple
from logging   import getLogger

from zmq          import ROUTER, DEALER

from .base_device import BaseDevice

class LoadBalancer(BaseDevice, namedtuple('LoadBalancer', 'client service')):
    logger = getLogger('netcall.load_balancer')

    __slots__ = ()
    
    socket_types = [ROUTER, DEALER]
        
    def __init__(self, **kwargs):
        super(LoadBalancer, self).__init__(**kwargs)
        
        self._handlers.append((self.client.socket, self._handle_client_to_service))
        self._handlers.append((self.service.socket, self._handle_service_to_client))
        
    def _handle_client_to_service(self, s_client, s_service):
        s_service.send_multipart(s_client.recv_multipart(copy=False), copy=False)
        
    def _handle_service_to_client(self, s_client, s_service):
        s_client.send_multipart(s_service.recv_multipart(copy=False), copy=False)
