from __future__  import absolute_import
from collections import namedtuple
from functools   import wraps

from ..utils  import detect_green_env, gevent_patched_threading

from .futures import Future as _Future, TimeoutError


ConcurrencyTools = namedtuple(
    'ConcurrencyTools',
    'sleep Thread Timer Event Condition Lock RLock Queue Empty Future Executor'
)

def get_tools(env='auto'):
    """ Returns a <ConcurrencyTools> named tuple:

        (sleep, Thread, Timer, Event, Condition, Lock, RLock,
         Queue, Empty, Future, Executor)

        compatible with current environment.

        If env is 'auto' (default), tries to detect a monkey-patched green
        thread environment with detect_green_env(). Gevent, Eventlet and
        Greenhouse are supported as well as the regular Python Threading API.
    """
    if env == 'auto':
        env = detect_green_env()

    if env == 'gevent':
        import gevent       as time
        import gevent.queue as Queue
        from .gevent import GeventExecutor as Executor
        threading = gevent_patched_threading()

        Future = wraps(_Future)(lambda: _Future(condition=threading.Condition()))

    elif env == 'eventlet':
        import eventlet       as time
        import eventlet.queue as Queue
        from eventlet.green import threading
        from .eventlet      import EventletExecutor as Executor

        Future = wraps(_Future)(lambda: _Future(condition=threading.Condition()))

    elif env == 'greenhouse':
        import greenhouse
        time      = greenhouse
        threading = greenhouse
        Queue     = greenhouse
        #from .greenhouse import GreenhouseExecutor as Executor # TODO

        Future   = wraps(_Future)(lambda: _Future(condition=threading.Condition()))
        Executor = NotImplementedError  # TODO

    elif env in [None, 'threading']:
        import time, threading
        try:
            import Queue
        except ImportError: # Python 3 support
            import queue as Queue
        from .pebble import ThreadPoolExecutor as Executor

        Future = _Future

    else:
        raise ValueError('unsupported environment %r' % env)

    return ConcurrencyTools(
        time.sleep,
        threading.Thread,
        threading.Timer,
        threading.Event,
        threading.Condition,
        threading.Lock,
        threading.RLock,
        Queue.Queue,
        Queue.Empty,
        Future,
        Executor
    )


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
