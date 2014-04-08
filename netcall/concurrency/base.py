from __future__  import absolute_import
from abc         import ABCMeta, abstractmethod


class Executor(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def submit(self, func, *args, **kw):
        """ Returns a Future (an object representing a running task).

            A Future has the following methods:

            .result(timeout=None)    -- returns a result or re-raises exception
                                        (blocking)
            .exception(timeout=None) -- returns an exception or None
                                        (blocking)
            .cancel()                -- attempts to cancel the running task,
                                        returns True on success
            .cancelled()             -- True if the task is cancelled
            .running()               -- True if the task is running
            .done()                  -- True if the task is finished or cancelled
            .add_done_callback(func) -- runs func(future) when the task is done
        """
        pass

    @abstractmethod
    def wait(timeout=None):
        """ Blocks until all running tasks are done.
            May raise a TimeoutError exception.
        """
        pass

    @abstractmethod
    def shutdown(wait=True, cancel=False):
        """ Signals the executor to free up resources when all running
            tasks are done. Optionally cancels current tasks.
        """
        pass


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
