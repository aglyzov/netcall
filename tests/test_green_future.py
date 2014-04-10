# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall.concurrency.futures import Future, TimeoutError

from .base import BaseCase


try:
    from gevent        import spawn, sleep
    from netcall.utils import gevent_patched_threading
    threading = gevent_patched_threading()

    class GreenFutureTest(BaseCase):

        def test_default_future_blocks_greenlets(self):
            """ Default Future using regular Condition/RLock blocks greenlets
            """
            f = Future()
            def _set_result():
                sleep(0.1)
                f.set_result(123)
            spawn(_set_result)
            try:
                f.result(timeout=0.2)
            except TimeoutError:
                # a timeout means the _set_result greenlet has been blocked
                # by the future's lock, which we expect
                pass
            else:
                self.assertTrue(False)

        def test_patched_future_respects_greenlets(self):
            """ A Future using gevent Condition/RLock respects greenlets
            """
            f = Future(condition=threading.Condition())
            def _set_result():
                sleep(0.1)
                f.set_result(123)
            spawn(_set_result)
            try:
                res = f.result(timeout=0.2)
                self.assertEquals(res, 123)
            except TimeoutError:
                # a timeout means the _set_result greenlet has been blocked
                # by the future's lock, which should not happen
                self.assertTrue(False)

except ImportError:
    pass
