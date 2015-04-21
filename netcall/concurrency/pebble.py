from __future__  import absolute_import

from pebble import Task, TimeoutError as PebbleTimeout
try:
    from pebble.thread import Pool as ThreadPool
except ImportError:
    from pebble import ThreadPool

from .base    import FutureBase, ExecutorBase
from .futures import TimeoutError


class TaskFutureAdapter(FutureBase):
    """ A Pebble Task adapter providing the Future interface
    """
    def __init__(self, task):
        assert isinstance(task, Task)
        self._task = task

    def result(self, timeout=None):
        try:
            return self._task.get(timeout=timeout)
        except PebbleTimeout as e:
            raise TimeoutError(e)
        except Exception as e:
            tb = getattr(e, 'traceback')
            if tb is None:
                raise
            else:
                raise e.__class__(tb)

    def exception(self, timeout=None):
        try:
            self._task.get(timeout=timeout)
        except PebbleTimeout as e:
            raise TimeoutError(e)
        except Exception as e:
            return e

    def cancel(self):
        return self._task.cancel()

    def cancelled(self):
        return self._task.cancelled

    def running(self):
        return not self.done()

    def done(self):
        return self._task.ready

    def add_done_callback(self, func):
        if self.done():
            func(self)
        else:
            raise NotImplemented("pebble.Task does not support callback notification")

class ThreadPoolExecutor(ExecutorBase):
    """ An Executor using Pebble ThreadPool
    """
    def __init__(self, limit=None):
        self._limit = limit or 128
        self._pool  = ThreadPool(workers=self._limit)

    def submit(self, func, *args, **kw):
        pool = self._pool
        if pool is None:
            return
        task = pool.schedule(func, args=args, kwargs=kw)
        return TaskFutureAdapter(task)

    def wait(self, timeout=None):
        pool = self._pool
        if pool is None:
            return
        if hasattr(pool, '_context'):  # Pebble >= 2.6
            queue = pool._context.queue
        else:
            queue = pool._queue
        queue.all_tasks_done.acquire()
        try:
            if queue.unfinished_tasks > 0:
                queue.all_tasks_done.wait(timeout=timeout)
                if timeout is not None and queue.unfinished_tasks > 0:
                    raise TimeoutError('Timeout while waiting for queue.all_tasks_done')
        finally:
            queue.all_tasks_done.release()

    def shutdown(self, wait=True, cancel=False):
        pool = self._pool
        self._pool = None

        if pool is None:
            return
        if cancel:
            pool.close()
            pool.stop()
        else:
            pool.close()
        if wait:
            pool.join()


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
