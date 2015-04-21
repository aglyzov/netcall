# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

from __future__  import absolute_import
#from collections import namedtuple

from sys     import stderr, modules
from imp     import new_module
from runpy   import _get_module_details
from logging import getLogger


logger = getLogger('netcall')  # generic netcall logger
_gevent_cache = {}

#ZMQClasses = namedtuple('ZMQClasses', 'Context Poller')


#-----------------------------------------------------------------------------
# Utilies
#-----------------------------------------------------------------------------

# Python 2 and 3 transparent support for string type comparison
try:
    isinstance("", basestring)
    def is_str(s):
        return isinstance(s, basestring)
except NameError:
    def is_str(s):
        return isinstance(s, str)

def setup_logger(logger=None, level='DEBUG', format=None, stream=stderr):
    """ A utility function to setup a basic logging handler for a given logger
    """
    from logging import StreamHandler, Formatter, getLogger, getLevelName

    assert logger is not None, 'logger is required (either <str> or <Logger>)'
    
    if is_str(logger):
        logger = getLogger(logger)

    # https://bugs.launchpad.net/oslo-incubator/+bug/1322701
    # Python 3.4 changed the behaviour of getLevelName().
    # Apparently, since Python 2.7 setLevel() accept strings and numerics.
    # (anything before Python 2.7 is not supported by the Python Foundation anyway)
    #if is_str(level):
    #    level  = getLevelName(level)
    
    logger.setLevel(level)

    if format is None:
        format = "%(asctime)s.%(msecs)d:%(process)s/%(threadName)s:%(levelname)s:%(name)s:%(funcName)s:%(message)s"

    if not logger.handlers:
        handler   = StreamHandler(stream)
        formatter = Formatter(format, datefmt="%H:%M:%S")
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def import_module(name, cache=modules):
    """Execute a module's code without importing it

       Returns the resulting top level namespace dictionary
    """
    if name in cache:
        return cache[name]

    mod_name, loader, code, fname = _get_module_details(name)
    pkg_name = mod_name.rpartition('.')[0]
    module = new_module(mod_name)
    module.__dict__.update(
        __name__    = mod_name,
        __file__    = fname,
        __loader__  = loader,
        __package__ = pkg_name
    )
    exec(code, module.__dict__)

    return module

def gevent_patched_module(name, items=None, cache=_gevent_cache):
    """ Returns a gevent monkey-patched module (bypassing sys.modules)
    """
    if name in cache:
        return cache[name]

    gevent_module = getattr(__import__('gevent.' + name), name)
    module_name = getattr(gevent_module, '__target__', name)
    module = import_module(module_name, cache=cache)
    if items is None:
        items = getattr(gevent_module, '__implements__', None)
        if items is None:
            raise AttributeError('%r does not have __implements__' % gevent_module)
    for attr in items:
        setattr(module, attr, getattr(gevent_module, attr))

    return module

def gevent_patched_threading():
    """ Returns a gevent monkey-patched `threading` module
    """
    threading = gevent_patched_module('threading')
    from gevent.event import Event
    threading.Event = Event

    return threading


def detect_green_env():
    """ Detects a monkey-patched green thread environment and
        returns either one of these:

        'gevent' | 'eventlet' | 'greenhouse' | None

        Notice, it relies on a monkey-patched threading module.
    """
    import threading
    thr_module = threading._start_new_thread.__module__

    if 'gevent' in thr_module:
        return 'gevent'
    elif 'greenhouse' in thr_module:
        return 'greenhouse'
    elif 'eventlet' in thr_module:
        return 'eventlet'
    else:
        return None

def green_device(inp, out, env=None):
    """ Runs a greenlet-compatible ZMQ device (message forwarder).
        Starts two green threads, one for each direction.
    """
    from .concurrency import get_tools

    env   = env or detect_green_env() or 'gevent'
    spawn = get_tools(env=env).Executor().submit

    def _inp_to_out():
        while True:
            out.send_multipart(inp.recv_multipart())

    def _out_to_inp():
        while True:
            inp.send_multipart(out.recv_multipart())

    i2o = spawn(_inp_to_out)
    o2i = spawn(_out_to_inp)

    i2o.result()
    o2i.result()


def get_zmq_classes(env='auto'):
    """ Returns ZMQ Context and Poller classes that are
        compatible with the current environment.

        If env is 'auto' (default), tries to detect a monkey-patched
        green thread environment. Gevent, Eventlet and Greenhouse are
        supported as well as the regular PyZMQ Context class.
    """
    if env == 'auto':
        env = detect_green_env()

    if env == 'gevent':
        from zmq.green import Context, Poller

    elif env == 'greenhouse':
        import greenhouse
        green = greenhouse.patched('zmq')
        Context, Poller = green.Context, green.Poller

    elif env == 'eventlet':
        from eventlet.green.zmq import Context
        class Poller(object):
            def __init__(self, *args, **kwargs):
                raise NotImplementedError('eventlet does not support ZeroMQ Poller')

    elif env in [None, 'threading']:
        from zmq import Context, Poller

    else:
        raise ValueError('unsupported environment %r' % env)

    return Context, Poller


class RemoteMethodBase(object):
    """A remote method class to enable a nicer call syntax."""

    def __init__(self, client, method):
        self.client = client
        self.method = method

class RemoteMethod(RemoteMethodBase):

    def __call__(self, *args, **kwargs):
        return self.client.call(self.method, args, kwargs)


    def __getattr__(self, name):
        return RemoteMethod(self.client, '.'.join([self.method, name]))



