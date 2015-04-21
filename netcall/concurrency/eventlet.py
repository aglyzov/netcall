from __future__ import absolute_import

from eventlet.greenthread        import GreenThread
from eventlet.greenpool          import GreenPool
from eventlet.timeout            import Timeout, with_timeout
from eventlet.support.greenlets  import GreenletExit

from .base    import FutureBase, ExecutorBase
from .futures import TimeoutError


class GreenThreadFutureAdapter(FutureBase):
    """ An Eventlet GreenThread adapter providing the Future interface
    """
    def __init__(self, greenthread):
        assert isinstance(greenthread, GreenThread)
        self._greenthread = greenthread

    def result(self, timeout=None):
        get = self._greenthread.wait
        if timeout is not None:
            get = with_timeout(timeout, get)
        try:
            return get()
        except Timeout as e:
            raise TimeoutError(e)

    def exception(self, timeout=None):
        get = self._greenthread.wait
        if timeout is not None:
            get = with_timeout(timeout, get)
        try:
            get()
        except Timeout as e:
            raise TimeoutError(e)
        except Exception as e:
            return e

    def cancel(self):
        self._greenthread.kill()
        return True

    def cancelled(self):
        return self._greenthread.dead and isinstance(self.exception(), GreenletExit)

    def running(self):
        return not self.done()

    def done(self):
        return self._greenthread.dead

    def add_done_callback(self, func):
        return self._greenthread.link(lambda gr: func(self))

class EventletExecutor(ExecutorBase):
    """ An Executor using Eventlet GreenPool
    """
    def __init__(self, limit=None):
        self._limit = limit or 100000
        self._pool  = GreenPool(size=self._limit)

    def submit(self, func, *args, **kw):
        pool = self._pool
        if pool is None:
            return
        gt = pool.spawn(func, *args, **kw)
        return GreenThreadFutureAdapter(gt)

    def wait(self, timeout=None):
        pool = self._pool
        if pool is None:
            return
        wait = pool.waitall
        if timeout is not None:
            wait = with_timeout(timeout, wait)
        try:
            wait()
        except Timeout as e:
            raise TimeoutError(e)

    def shutdown(self, wait=True, cancel=False):
        pool = self._pool
        self._pool = None

        if pool is None:
            return
        if cancel:
            for gt in pool.coroutines_running.copy():
                gt.kill()
        if wait:
            pool.waitall()


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
