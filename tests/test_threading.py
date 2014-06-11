# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall.utils       import get_zmq_classes
from netcall.threading   import ThreadingRPCClient, ThreadingRPCService
from netcall.concurrency import get_tools

from .base          import BaseCase
from .client_mixins import ClientBindConnectMixIn
from .rpc_mixins    import RPCCallsMixIn


class ThreadingBase(BaseCase):

    def setUp(self):
        env = None
        Context, _ = get_zmq_classes(env)

        self.tools    = get_tools(env)
        self.context  = Context()
        self.executor = self.tools.Executor(12)
        #from concurrent.futures import ThreadPoolExecutor
        #self.executor = ThreadPoolExecutor(12)
        self.client   = ThreadingRPCClient(context=self.context, executor=self.executor)
        self.service  = ThreadingRPCService(context=self.context, executor=self.executor)

        super(ThreadingBase, self).setUp()

    def tearDown(self):
        self.client.shutdown()
        self.service.shutdown()
        self.context.term()
        self.executor.shutdown(cancel=True)
        #self.executor.shutdown()

        super(ThreadingBase, self).tearDown()


class ThreadingClientBindConnectTest(ClientBindConnectMixIn, ThreadingBase):
    pass

class ThreadingRPCCallsTest(RPCCallsMixIn, ThreadingBase):
    pass

