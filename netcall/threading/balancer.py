# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

"""
An RPC load balancer class using ZeroMQ as a transport and
the standard Python threading API for concurrency.

Authors
-------
* Alexander Glyzov
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2014. Alexander Glyzov
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

from random    import randint
from itertools import chain

import zmq

from ..base  import RPCLoadBalancerBase
from ..utils import get_zmq_classes


#-----------------------------------------------------------------------------
# RPC Load Balancer
#-----------------------------------------------------------------------------

class ThreadingRPCLoadBalancer(RPCLoadBalancerBase):

    def __init__(self, *ar, **kw):
        super(ThreadingRPCLoadBalancer, self).__init__(*ar, **kw)

        self._client_side_url = 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
        )
        self._service_side_url = 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
        )

        self._threads_sync = self.tools.Event()
        self._control_sync = self.tools.Event()

        self._control = self.context.socket(zmq.PUSH)
        self._control.connect(self._service_side_url)

        # start threads
        submit = self.executor.submit

        self.client_io_thread  = submit(self._client_io_thread)
        self.service_io_thread = submit(self._service_io_thread)
        self.service_refresher = submit(self._peer_refresher)

        # sync the _control socket with service_io_thread
        while not self._control_sync.is_set():
            self._control.send_multipart([b'CONTROL', b'SYNC'])
            self._control_sync.wait(0.05)

    def _update_connections(self, fresh_addrs, stale_addrs):
        """ Updates connections to the services.

            This thread-safe version delegates the task to the service_io_thread
        """
        packet = list(chain(
            [b'CONTROL', b'UPDATE'],
            fresh_addrs,
            [b'|'],
            stale_addrs,
        ))
        self._control.send_multipart(packet)

    def _service_io_thread(self):
        """ Forwards client requests to connected services balancing their
            load by tracking number of running tasks and receives answers
            passing them to the _client_io_thread.

            Notice: this thread manages I/O of self.out_sock exclusively
                    -- this is a requirement of ZMQ (a socket must be used
                    by a single thread)
        """
        exit_ev = self._exit_ev
        logger  = self.logger

        send_request = self.send_request

        from_service = self.out_sock

        from_client = self.context.socket(zmq.PULL)
        from_client.bind(self._service_side_url)

        to_client = self.context.socket(zmq.PUSH)
        to_client.connect(self._client_side_url)

        # sync from_client with the _control socket and client_io_thread
        for _ in (1, 2):
            packet = from_client.recv_multipart()
            if packet[0] == b'CONTROL':
                self._control_sync.set()
                logger.debug('service_io_thread synchronized with the _control socket')
            else:
                self._threads_sync.set()
                logger.debug('service_io_thread synchronized with client_io_thread')

        _, Poller = get_zmq_classes(env=self.green_env)
        poller = Poller()
        poller.register(from_client,  zmq.POLLIN)
        poller.register(from_service, zmq.POLLIN)
        poll = poller.poll

        # -- I/O loop --
        running = True

        while running and not exit_ev.is_set():
            request = None
            try:
                for socket, _ in poll():
                    packet = socket.recv_multipart()
                    #logger.debug('received %s', packet)

                    if socket is from_service:
                        if packet[0] == b'QUIT':
                            logger.debug('service_io_thread received a QUIT signal')
                            running = False
                            break
                        # pass the answer to the client_io_thread
                        to_client.send_multipart(packet)

                    elif packet[0] == b'CONTROL':
                        if packet[1] == b'UPDATE':
                            idx = packet.index(b'|')
                            fresh_addrs = packet[2:idx]
                            stale_addrs = packet[idx+1:]
                            super(ThreadingRPCLoadBalancer, self)._update_connections(
                                fresh_addrs, stale_addrs
                            )
                    else:
                        request = packet

            except Exception, err:
                logger.warning(err)
                break

            if not running:
                break

            if request is None:
                continue

            if len(request) < 6 or b'|' not in request:
                logger.warning('skipping bad request: %r', request)
                continue

            send_request(request)

        # -- cleanup --
        from_client .close(0)
        to_client   .close(0)

        logger.debug('service_io_thread exited')

    def _client_io_thread(self):
        """ Forwards service answers back to the client and receives client
            requests passing them to the self._service_io_thread.

            Notice: this thread manages I/O of self.inp_sock exclusively
                    -- this is a requirement of ZMQ (a socket must be used
                    by a single thread)
        """
        exit_ev = self._exit_ev
        logger  = self.logger

        send_answer = self.send_answer

        from_client = self.inp_sock

        from_service = self.context.socket(zmq.PULL)
        from_service.bind(self._client_side_url)

        to_service = self.context.socket(zmq.PUSH)
        to_service.connect(self._service_side_url)

        # sync the to_service socket with service_io_thread
        while not self._threads_sync.is_set():
            to_service.send_multipart([b'CLIENT_IO_THREAD', b'SYNC'])
            self._threads_sync.wait(0.05)

        _, Poller = get_zmq_classes(env=self.green_env)
        poller = Poller()
        poller.register(from_client,  zmq.POLLIN)
        poller.register(from_service, zmq.POLLIN)
        poll = poller.poll

        # -- I/O loop --
        running = True

        while running and not exit_ev.is_set():
            answer = None
            try:
                for socket, _ in poll():
                    packet = socket.recv_multipart()
                    #logger.debug('received %s', packet)
                    if socket is from_client:
                        if packet[0] == b'QUIT':
                            logger.debug('client_io_thread received a QUIT signal')
                            running = False
                            break
                        # pass the request to the service_io_thread
                        to_service.send_multipart(packet)
                    else:
                        answer = packet
            except Exception, err:
                logger.warning(err)
                break

            if not running:
                break

            if answer is None:
                continue

            if len(answer) < 5 or b'|' not in answer:
                logger.warning('skipping bad answer: %r', answer)
                continue

            try:
                send_answer(answer)
            except Exception, err:
                logger.warning(err)
                break

        # -- cleanup --
        from_service .close(0)
        to_service   .close(0)

        logger.debug('client_io_thread exited')

    def _close_sockets(self, linger=0):
        super(ThreadingRPCLoadBalancer, self)._close_sockets(linger)
        self._control.close(linger)


