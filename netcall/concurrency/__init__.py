from __future__  import absolute_import
from collections import namedtuple
from functools   import wraps

from ..utils  import detect_green_env, gevent_patched_threading

from .futures import Future


ConcurrencyTools = namedtuple(
    'ConcurrencyTools', 'sleep Event Condition RLock Queue Empty Future Executor'
)

def get_tools(env=None):
    """ Returns a ConcurrencyTools named tuple:
          (sleep, Event, Condition, RLock, Queue, Empty, Future, Executor)
        compatible with current environment.

        If env is undefined, tries to detect a monkey-patched green
        environment with detect_green_env().
    """
    env = env or detect_green_env()

    if env == 'gevent':
        from gevent       import sleep
        from gevent.queue import Queue, Empty
        from .gevent      import GeventExecutor

        threading = gevent_patched_threading()

        tools = ConcurrencyTools(
            sleep,
            threading.Event,
            threading.Condition,
            threading.RLock,
            Queue,
            Empty,
            wraps(Future)(lambda: Future(condition=threading.Condition())),
            GeventExecutor
        )

    elif env == 'eventlet':
        from eventlet.green.threading import Event, Condition, RLock
        from eventlet.queue           import Queue, Empty
        from eventlet                 import sleep
        from .eventlet                import EventletExecutor

        tools = ConcurrencyTools(
            sleep,
            Event,
            Condition,
            RLock,
            Queue,
            Empty,
            wraps(Future)(lambda: Future(condition=Condition())),
            EventletExecutor
        )

    elif env == 'greenhouse':
        from greenhouse  import sleep, Event, Condition, RLock, Queue, Empty
        from .greenhouse import GreenhouseExecutor

        tools = ConcurrencyTools(
            sleep,
            Event,
            Condition,
            RLock,
            Queue,
            Empty,
            wraps(Future)(lambda: Future(condition=Condition())),
            GreenhouseExecutor
        )

    elif env is None:
        from time      import sleep
        from threading import Event, Condition, RLock
        from Queue     import Queue, Empty
        from .pebble   import ThreadPoolExecutor

        tools = ConcurrencyTools(
            sleep,
            Event,
            Condition,
            RLock,
            Queue,
            Empty,
            Future,
            ThreadPoolExecutor
        )

    else:
        raise ValueError('unsupported environment %r' % env)

    return tools


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0
