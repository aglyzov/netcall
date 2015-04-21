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
from pickle    import dumps as pickle_dumps, loads as pickle_loads
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

        self._control_addr = 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
        )
        #self._control_sync = self.tools.Event()

        self._control = self.context.socket(zmq.PUSH)
        self._control.connect(self._control_addr)

        # start threads
        submit = self.executor.submit

        self.io_thread      = submit(self._io_thread)
        self.peer_refresher = submit(self._peer_refresher)

    def _init_peer_refresher(self):
        # sync the _control socket with service_io_thread
        self._control.send(b'SYNC')

    def _update_connections(self, fresh_addrs, stale_addrs):
        """ Updates connections to the services.

            This thread-safe version delegates the task to the io_thread.

            Note: it should be called from the _peer_refresher thread only.
        """
        packet = list(chain(
            [b'UPDATE'],
            map(bytes, fresh_addrs),
            [b'|'],
            map(bytes, stale_addrs),
        ))
        self._control.send_multipart(packet)

    def send_requests(self, *requests):
        """ Triggers sending of pending requests

            This thread-safe version delegates the task to the io_thread.

            Note: it should be called from the _peer_refresher thread only.
        """
        packet = [b'SEND']
        if requests:
            packet.append(pickle_dumps(requests, protocol=-1))
        self._control.send_multipart(packet)

    def _handle_control(self, packet):
        """ This should be called from _service_io_thread
        """
        if packet[0] == b'UPDATE':
            # update service connections
            idx = packet.index(b'|')
            fresh_addrs = packet[1:idx]
            stale_addrs = packet[idx+1:]
            super(ThreadingRPCLoadBalancer, self)._update_connections(
                fresh_addrs, stale_addrs
            )

        elif packet[0] == b'SEND':
            # trigger sending of pending requests
            if len(packet) > 1:
                requests = pickle_loads(packet[1])
            else:
                requests = []
            super(ThreadingRPCLoadBalancer, self).send_requests(*requests)

    def _io_thread(self):
        """ Forwards client requests to connected services balancing their
            load by tracking number of running tasks and receives answers
            passing them back to the client.

            Notice: this thread manages I/O of self.out_sock and self.inp_sock
                    exclusively -- this is a requirement of ZMQ (a socket must
                    be used by a single thread)
        """
        exit_ev = self._exit_ev
        logger  = self.logger

        send_requests = super(ThreadingRPCLoadBalancer, self).send_requests
        send_answer   = self.send_answer

        client  = self.inp_sock
        service = self.out_sock
        control = self.context.socket(zmq.PULL)
        control.bind(self._control_addr)

        # sync the control socket with _peer_refresher
        control.recv()
        logger.debug('io_thread synchronized with peer_refresher')

        _, Poller = get_zmq_classes(env=self.green_env)
        poller = Poller()
        poller.register(client,  zmq.POLLIN)
        poller.register(service, zmq.POLLIN)
        poller.register(control, zmq.POLLIN)
        poll = poller.poll

        # -- I/O loop --
        running = True

        while running and not exit_ev.is_set():
            try:
                for socket, _ in poll():
                    packet = socket.recv_multipart()
                    #logger.debug('recv: %r', packet)

                    if socket is control:
                        self._handle_control(packet)

                    elif socket is service:
                            send_answer(packet)

                    elif socket is client:
                        if len(packet) < 6 or b'|' not in packet:
                            logger.warning('skipping bad request: %r', packet)
                            continue
                        elif packet[0] == b'QUIT':
                            logger.debug('io_thread received a QUIT signal')
                            running = False
                            break
                        else:
                            send_requests(packet)  # send or postpone a request

            except Exception as err:
                logger.warning(err)
                break

        # -- cleanup --
        control.close(0)

        logger.debug('io_thread exited')

    def _close_sockets(self, linger=0):
        super(ThreadingRPCLoadBalancer, self)._close_sockets(linger)
        self._control.close(linger)

