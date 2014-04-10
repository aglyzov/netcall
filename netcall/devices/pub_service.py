from collections import namedtuple
from logging   import getLogger

from zmq          import ROUTER, DEALER, PUB, ROUTER_MANDATORY

from .base_device import BaseDevice

class PubService(BaseDevice, namedtuple('PubService', 'service pub client')):
    logger = getLogger('netcall.pub_service')

    __slots__ = ()
    
    socket_types = [DEALER, PUB, ROUTER]
        
    def __init__(self, **kwargs):
        super(PubService, self).__init__(**kwargs)
        
        self.client.setsockopt(ROUTER_MANDATORY, 1)
        
        self._handlers.append((self.client.socket, self._handle_client_to_service))
        self._handlers.append((self.service.socket, self._handle_service_to_client))
        
        # Get serializer instance from any socket (only used to make null constants)
        self._serializer = self.client._serializer
    
        self.known_requests = {}
        self.known_procs = {}
        self.known_procs_yielding = {}
        self.known_yields = {}
        self.none_args_kwargs = list(self._serializer.serialize_args_kwargs(None, None))
        self.none_result = self._serializer.serialize_result(None)
        self.ignore = bytes(int(False))
        
        self._executor = self._tools.Executor()
        
    def _handle_client_to_service(self, s_service, s_pub, s_client):
        data = s_client.recv_multipart()
        self.logger.debug('received from client: %r' % data)
        
        boundary = data.index(b'|')
        req_id = data[boundary+1]
        proc = data[boundary+2]
        
        self.known_procs[req_id] = proc
        if proc in self.known_procs_yielding:
            self.known_procs_yielding[proc].append(req_id)
            reply = data[0:boundary+2] + ['PUB', proc] + self.none_result #TODO: lastval goes here
            self.logger.debug('sending to client %r' % reply)
            s_client.send_multipart(reply)
            return
        
        self.logger.debug('sending to service %r' % data)
        s_service.send_multipart(data)
        
    def _handle_service_to_client(self, s_service, s_pub, s_client):
        spawn = self._executor.submit
        #spawn_later = self._tools[1]
        Queue = self._tools.Queue
    
        data = s_service.recv_multipart()
        self.logger.debug('receiving from service: %r' % data)
        
        boundary = data.index(b'|')
        req_id = data[boundary+1]
        proc = data[boundary+2]
        original_proc = self.known_procs[req_id]
        
        if proc == 'YIELD':
            if req_id not in self.known_yields:
                self.known_yields[req_id] = Queue(1)
                self.known_yields[req_id].put(True)
                
                self.known_procs_yielding[original_proc] = [req_id]

                data = data[0:boundary+2] + ['PUB', original_proc, data[boundary+3]]
                
                self.logger.debug('sending to client %r' % data)
                s_client.send_multipart(data) # TODO: Send pub addr here
                
                #spawn_later(0.5, yield_consummer, req_id) # Very short wait time, only good for LAN
                spawn(self._yield_consummer, s_service, req_id) # Assume immediate subscribe, but to test over a slow interconnect
                
            else:
                data = [original_proc] + data[boundary+2:]
                self.logger.debug('sending to pub %r' % data)
                s_pub.send_multipart(data)
                self.known_yields[req_id].put(True)
                
        elif (proc == 'FAIL') and (req_id in self.known_yields):
            data = [original_proc] + data[boundary+2:]
            self.logger.debug('sending to pub %r' % data)
            s_pub.send_multipart(data)
            self.known_yields[req_id].put(False)
            del self.known_yields[req_id]
            
            original_proc = self.known_procs[req_id]
            if original_proc in self.known_procs_yielding:
                for r in self.known_procs_yielding[original_proc]:
                    del self.known_procs[r]
                del self.known_procs_yielding[original_proc]
            
        elif (proc == 'ACK') and (req_id in self.known_yields):
            self.logger.debug('skipping ACK for published yield %r' % req_id)
            return
        else:
            self.logger.debug('sending to client %r' % data)
            s_client.send_multipart(data)
            if proc != 'ACK': # OK or FAIL
                original_proc = self.known_procs[req_id]
                if original_proc in self.known_procs_yielding:
                    for r in self.known_procs_yielding[original_proc]:
                        del self.known_procs[r]
                    del self.known_procs_yielding[original_proc]
                
    def _yield_consummer(self, s_service, req_id):
        queue = self.known_yields[req_id]
        data = [b'|', req_id, '_SEND'] + self.none_args_kwargs + [self.ignore]
        while self.running:
            if queue.get():
                self.logger.debug('sending to service %r' % data)
                s_service.send_multipart(data)
            else:
                return
