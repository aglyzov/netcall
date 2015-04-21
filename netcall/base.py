# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
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

from abc     import ABCMeta, abstractmethod
from random  import randint, shuffle
from logging import getLogger

import zmq

from .utils       import logger, detect_green_env, get_zmq_classes, is_str
from .serializer  import PickleSerializer
from .datastruct  import priority_dict
from .concurrency import get_tools


#-----------------------------------------------------------------------------
# RPC base
#-----------------------------------------------------------------------------

class RPCBase(object):
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
        self.identity    = identity or ('%08x' % randint(0, 0xFFFFFFFF)).encode()
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
        if is_str(urls):
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
        if is_str(urls):
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


class RPCLoadBalancerBase(object):
    """ RPC Load Balancer base class.

        It is a smart ZMQ device with the ROUTER sockets on both sides.
        It can be plugged in between an RPC client and RPC services to
        achieve a _fair_ load balancing based on the total number of
        running tasks in each connected service.

        This device replaces the simplistic round-robin routing that is
        built into the ZMQ DEALER socket. The round-robin approach is not
        suited RPC because it makes for an uneven work spread among the
        connected services.

        Moreover the peer service discovery is done by a supplied function
        `discovery_func` every `interval` seconds.

        As a bonus such an intermediary quickly recognizes dead or
        disconnected peers.
    """
    __metaclass__ = ABCMeta

    logger = getLogger('netcall.balancer')

    def __init__(self, discover_func, interval=30, context=None, executor=None, bind_url=None):
        """ Parameters
            ----------
            discover_func - <callable> that returns a <dict> {<url>:<identity>} for active services
            interval      - (opt) <float> number of seconds to wait between service discoveries
            context       - (opt) ZMQ <Context> for sockets
            executor      - (opt) <Executor> for threads/greenlets
            bind_addr     - (opt) <str> URL address for the client side ZMQ ROUTER socket
        """
        self.discover_func = discover_func
        self.interval      = interval

        Context, _ = get_zmq_classes()  # auto detect green env

        if context is None:
            self.context = Context.instance()
        else:
            assert isinstance(context, Context)
            self.context = context

        self.green_env = detect_green_env()
        self.tools     = get_tools(env=self.green_env)  # <netcall.concurrency.ConcurrencyTools>
        self.executor  = executor or self.tools.Executor(3)

        self._ext_executor = executor is not None

        # shared objects for tracking running tasks (protected with _lock)
        self.addr_set      = set()            # set of connected service addresses
        self.wid2addr_map  = {}               # {<worker_id> : <worker_addr>}
        self.wid2nrun_map  = priority_dict()  # {<worker_id> : <n_running>}
        self._lock         = self.tools.Lock()

        self._exit_ev      = self.tools.Event()

        self._pending = []  # pending requests

        # client side socket
        self.inp_sock = self.context.socket(zmq.ROUTER)
        self.inp_addr = bind_url or 'inproc://%s-%s' % (
            self.__class__.__name__,
            b'%08x' % randint(0, 0xFFFFFFFF)
        )
        self.inp_sock.bind(self.inp_addr)

        # worker side socket
        self.out_sock = self.context.socket(zmq.ROUTER)
        self.out_sock.ROUTER_MANDATORY = 1  # fail explicitly if route_id is unknown

    def _update_connections(self, fresh_addrs, stale_addrs):
        """ Updates connections to the services
        """
        #self.logger.debug('_update_connections(%r, %r)', fresh_addrs, stale_addrs)

        for addr in stale_addrs:
            self.out_sock.disconnect(addr)

        for addr in fresh_addrs:
            self.out_sock.connect(addr)

    def _init_peer_refresher(self):
        "This might be used in a subclass to init the _peer_refresher thread"
        pass

    def _peer_refresher(self):
        """ Refresher thread discovers active RPC services every `self.interval` secs
            and makes sure `self.out_sock` is connected accordingly.
        """
        exit_ev = self._exit_ev
        lock    = self._lock
        logger  = self.logger

        addr_set     = self.addr_set
        wid2addr_map = self.wid2addr_map
        wid2nrun_map = self.wid2nrun_map

        self._init_peer_refresher()

        while not exit_ev.is_set():
            logger.debug('discovering active services')
            url2wid_map = self.discover_func()
            logger.debug('found %s services', len(url2wid_map))

            addrs = set(url2wid_map)

            fresh_addrs = addrs - addr_set
            stale_addrs = addr_set - addrs

            if fresh_addrs or stale_addrs:
                fresh_wids = []

                with lock:
                    for addr in fresh_addrs:
                        logger.debug(' + %-20s (new)', addr)
                        addr_set.add(addr)
                        wid = url2wid_map[addr]
                        wid2addr_map[wid] = addr
                        fresh_wids.append(wid)

                    for addr in stale_addrs:
                        logger.debug(' - %-20s (old)', addr)
                        addr_set.discard(addr)
                        wid = url2wid_map[addr]
                        wid2addr_map.pop(wid, None)
                        wid2nrun_map.pop(wid, None)

                self._update_connections(fresh_addrs, stale_addrs)

                if fresh_wids:
                    # wait a bit to make sure sockets are connected
                    exit_ev.wait(0.33)
                    # to make wid2nrun_map.smallest() non-deterministic on different machines
                    shuffle(fresh_wids)
                    # now expose fresh ids to the balancer
                    with lock:
                        wid2nrun_map.update((wid,0) for wid in fresh_wids)

                    # trigger sending of pending requests
                    if self._pending:
                        self.send_requests()

            if addrs:
                exit_ev.wait(self.interval)
            else:
                logger.warning('no workers, waiting')
                exit_ev.wait(3)

        logger.debug('peer_refresher exited')

    def pick_worker(self):
        """ Returns <worker_id> for the least used worker
            or None if there are no workers
        """
        with self._lock:
            if self.wid2nrun_map:
                return self.wid2nrun_map.smallest()
            else:
                return None

    def send_answer(self, answer):
        """ Sends an answer to the client
        """
        wid2nrun_map = self.wid2nrun_map

        wid = answer[0]
        idx = answer.index(b'|')
        typ = answer[idx+2]

        if typ in (b'OK', b'FAIL'):
            with self._lock:
                # decrement the number of running tasks for this worker
                wid2nrun_map[wid] = max(0, wid2nrun_map.get(wid, 1) - 1)

        # we skip the first identity -- it's a <service_id> added by our out_sock just now.
        # the next identity should be <client_id> for it was set by our inp_sock
        # on the request -- thus a necessary routing is already in place.
        self.inp_sock.send_multipart(answer[1:])

    def send_requests(self, *requests):
        """ Sends all requests (including pending) to the
            least loaded connected services.

            Returns a number of sent requests.
        """
        pending = self._pending
        pending.extend(requests)

        logger      = self.logger
        pick_worker = self.pick_worker

        wid = pick_worker()
        if wid is None:
            logger.debug('no workers, postponing sending')
            return 0

        wid2addr_map = self.wid2addr_map
        wid2nrun_map = self.wid2nrun_map
        lock         = self._lock
        exit_ev      = self._exit_ev
        to_service   = self.out_sock
        n_sent       = 0

        while wid and pending and not exit_ev.is_set():
            request = pending.pop(0)

            try:
                # prepending worker_id we picked to explicitly route the request
                to_service.send_multipart([wid] + request)
            except Exception as err:
                pending.insert(0, request)
                logger.warning('disabling worker %s (id:%s): %s', wid2addr_map.get(wid), wid, err)
                with lock:
                    wid2nrun_map.pop(wid, None)
                    wid2addr_map.pop(wid, None)
            else:
                n_sent += 1

                idx = request.index(b'|')
                try:    ignore = bool(int(request[idx+5]))
                except: ignore = True

                if not ignore:
                    with lock:
                        # increment the number of running tasks for this worker
                        wid2nrun_map[wid] = wid2nrun_map.get(wid, 0) + 1

            if pending:
                wid = pick_worker()

        return n_sent

    def _close_sockets(self, linger=0):
        self.inp_sock.close(linger)
        self.out_sock.close(linger)

    def shutdown(self):
        logger = self.logger

        # set the exit event for the threads/greenlets
        logger.debug('setting exit_ev for the threads')
        self._exit_ev.set()

        # send QUIT signal to the threads/greenlets
        logger.debug('sending a QUIT signal to the threads')

        # client side
        exiter1 = self.context.socket(zmq.DEALER)
        exiter1.IDENTITY = b'QUIT'
        exiter1.connect(self.inp_addr)
        exiter1.send(b'')

        # service side
        exiter2 = self.context.socket(zmq.DEALER)
        exiter2.IDENTITY = b'QUIT'
        addr   = 'inproc://exiter-%08x' % randint(0, 0xFFFFFFFF)
        exiter2.bind(addr)
        self.out_sock.connect(addr)
        exiter2.send(b'')

        # shutdown the executor
        if not self._ext_executor:
            logger.debug('shutting down the executor')
            self.executor.shutdown()

        # close ZMQ sockets
        self.logger.debug('closing ZMQ sockets')
        exiter1.close(0)
        exiter2.close(0)
        self._close_sockets(0)

        # we never destroy the ZMQ context here because we did not create it
