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

from ..base import RPCLoadBalancerBase


#-----------------------------------------------------------------------------
# RPC Load Balancer
#-----------------------------------------------------------------------------

class GreenRPCLoadBalancer(RPCLoadBalancerBase):

    def __init__(self, *ar, **kw):
        super(GreenRPCLoadBalancer, self).__init__(*ar, **kw)

        # start greenlets
        submit = self.executor.submit

        self.req_balancer   = submit(self._req_balancer)
        self.ans_forwarder  = submit(self._ans_forwarder)
        self.peer_refresher = submit(self._peer_refresher)

    def _req_balancer(self):
        """ Request balancer -- forwards client requests to peer workers
            balancing their load by tracking number of running tasks
        """
        recv_request  = self.inp_sock.recv_multipart
        send_requests = self.send_requests
        exit_ev       = self._exit_ev
        logger        = self.logger

        # receive loop
        while not exit_ev.is_set():
            try:
                request = recv_request()
                #logger.debug('recv: %r', request)
            except Exception as err:
                logger.warning(err)
                break

            if request[0] == b'QUIT':
                logger.debug('req_balancer received a QUIT signal')
                break

            if len(request) < 6 or b'|' not in request:
                logger.warning('skipping bad request: %r', request)
                continue

            send_requests(request)

        logger.debug('greenlet exited')

    def _ans_forwarder(self):
        """ Forwards worker answers back to the client
        """
        recv_answer = self.out_sock.recv_multipart
        send_answer = self.send_answer
        exit_ev     = self._exit_ev
        logger      = self.logger

        while not exit_ev.is_set():
            try:
                answer = recv_answer()
                #logger.debug('received: %r', answer)
            except Exception as err:
                self.logger.warning(err)
                break

            if answer[0] == b'QUIT':
                logger.debug('ans_forwarder received a QUIT signal')
                break

            if len(answer) < 5 or b'|' not in answer:
                logger.warning('skipping bad answer: %r', answer)
                continue

            try:
                send_answer(answer)
            except Exception as err:
                logger.warning(err)
                break

        logger.debug('greenlet exited')

