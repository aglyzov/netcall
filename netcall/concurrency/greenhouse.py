from __future__ import absolute_import

from .base    import Executor
from .futures import TimeoutError


raise NotImplementedError("TODO")


#class Greenlet(object):

#    def __init__(self, func, *args, **kwargs):
#        self.exit_ev = Event()
#        def func_ev():
#            try:     func(*args, **kwargs)
#            except:  raise
#            finally: self.exit_ev.set()
#        self.greenlet = greenlet(func_ev)

#    def wait(self, *args, **kwargs):
#        return self.exit_ev.wait(*args, **kwargs)

#    def spawn(self, after=None):
#        if after is None:
#            schedule(self.greenlet)
#        else:
#            schedule_in(after, self.greenlet)
#        return self
#    # aliases
#    join = wait
#    run  = spawn

#def spawn(func, *args, **kwargs):
#    g = Greenlet(func, *args, **kwargs)
#    return g.spawn()

#def spawn_later(sec, func, *args, **kwargs):
#    g = Greenlet(func, *args, **kwargs)
#    return g.spawn(after=sec)


# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

