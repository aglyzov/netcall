from collections import namedtuple
from logging   import getLogger

from zmq          import ROUTER, DEALER, SUB, SUBSCRIBE, UNSUBSCRIBE, ROUTER_MANDATORY

from .base_device import BaseDevice

class SubClient(BaseDevice, namedtuple('SubClient', 'service sub client')):
    logger = getLogger('netcall.sub_client')

    __slots__ = ()
    
    socket_types = [DEALER, SUB, ROUTER]
        
    def __init__(self, **kwargs):
        super(SubClient, self).__init__(**kwargs)
        
        self.client.setsockopt(ROUTER_MANDATORY, 1)
        
        self._handlers.append((self.client.socket, self._handle_client_to_service))
        self._handlers.append((self.service.socket, self._handle_service_to_client))
        self._handlers.append((self.sub.socket, self._handle_sub_to_client))
    
        self.known_yields = {}
        self.known_topics = {}
        
        self._executor = self._tools.Executor()
        
    def _handle_client_to_service(self, s_service, s_sub, s_client):
        data = s_client.recv_multipart()
        self.logger.debug('received from client: %r', data)
        
        boundary = data.index(b'|')
        req_id = data[boundary+1]
        proc = data[boundary+2]
        
        if proc.startswith('_') and req_id in self.known_yields:
            self.known_yields[req_id][1].put(None)
            self.logger.debug('skipping %r/%r' % (req_id, proc))
            return
        
        self.logger.debug('sending to service %r', data)
        s_service.send_multipart(data)
        
    def _handle_service_to_client(self, s_service, s_sub, s_client):
        Queue = self._tools.Queue
        
        data = s_service.recv_multipart()
        self.logger.debug('receiving from service: %r', data)
        
        boundary = data.index(b'|')
        req_id = data[boundary+1]
        proc = data[boundary+2]
        
        if proc == 'PUB':
            if req_id in self.known_yields:
                self.logger.error('Receving a yield for a known req_id :S')
                return
            
            self.known_yields[req_id] = (data[0:boundary], Queue(1))
            
            topic, result = data[boundary+3:]
            data = data[0:boundary+2] + ['YIELD', result]
            
            if topic not in self.known_topics:
                self.known_topics[topic] = []
            self.known_topics[topic].append(req_id)
            
            self.logger.debug('subscribing to %r', topic)
            s_sub.setsockopt(SUBSCRIBE, topic)

        self.logger.debug('sending to client %r', data)
        s_client.send_multipart(data)
                

        
    def _handle_sub_to_client(self, s_service, s_sub, s_client):
        spawn = self._executor.submit
        
        data = s_sub.recv_multipart()
        self.logger.debug('receiving from sub %r', data)
        
        topic, proc = data[0:2]
        
        for req_id in self.known_topics[topic]:            
            queue = self.known_yields[req_id][1]
            reply = self.known_yields[req_id][0] + [b'|'] + [req_id] + data[1:]
            spawn(self._send_to_client, s_client, s_sub, queue, topic, req_id, proc, reply)
                

    def _send_to_client(self, s_client, s_sub, queue, topic, req_id, proc, data):
        queue.get()
        self.logger.debug('sending to client %r', data)
        s_client.send_multipart(data)
        
        if proc == 'FAIL':
            self.logger.debug('unsubscribing from %r', topic)
            s_sub.setsockopt(UNSUBSCRIBE, topic)
            del self.known_yields[req_id]
            
            self.known_topics[topic].remove(req_id)
            if len(self.known_topics[topic]) == 0:
                del self.known_topics[topic]
