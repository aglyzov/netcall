from __future__ import absolute_import

from gevent      import Greenlet, Timeout, GreenletExit
from gevent.pool import Group, Pool

from .base    import FutureBase, ExecutorBase
from .futures import TimeoutError


class GreenletFuture(Greenlet, FutureBase):
    """ A Gevent Greenlet providing the Future interface
    """
    def result(self, timeout=None):
        try:
            return self.get(timeout=timeout)
        except Timeout as e:
            raise TimeoutError(e)

    def exception(self, timeout=None):
        try:
            self.join(timeout=timeout)
        except Timeout as e:
            raise TimeoutError(e)
        return super(GreenletFuture, self).exception

    def cancel(self):
        if not self.ready():
            self.kill()
        return True

    def cancelled(self):
        exc = super(GreenletFuture, self).exception
        return self.ready() and isinstance(exc, GreenletExit)

    def running(self):
        return not self.done()

    def done(self):
        return self.ready()

    def add_done_callback(self, func):
        return self.link(func)

class GeventExecutor(ExecutorBase):
    """ An Executor using Gevent Group/Pool
    """
    def __init__(self, limit=None):
        self._limit = limit

        if limit is None:
            self._pool = Group()
            self._pool.greenlet_class = GreenletFuture
        else:
            self._pool = Pool(size=limit, greenlet_class=GreenletFuture)

    def submit(self, func, *args, **kw):
        pool = self._pool
        if pool is None:
            return
        return pool.spawn(func, *args, **kw)

    def wait(self, timeout=None):
        pool = self._pool
        if pool is None:
            return
        pool.join(timeout=timeout)
        num = len(pool.greenlets)
        if timeout is not None and num > 0:
            raise TimeoutError('%s tasks are still running' % num)

    def shutdown(self, wait=True, cancel=False):
        pool = self._pool
        self._pool = None

        if pool is None:
            return
        if cancel:
            pool.kill(block=True)
        if wait:
            pool.join()


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
