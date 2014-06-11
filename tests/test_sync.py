# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall.utils       import get_zmq_classes
from netcall.sync        import SyncRPCClient
from netcall.threading   import ThreadingRPCService
from netcall.concurrency import get_tools

from .base          import BaseCase
from .client_mixins import ClientBindConnectMixIn
from .rpc_mixins    import RPCCallsMixIn


class SyncBase(BaseCase):

    def setUp(self):
        env = None
        Context, _ = get_zmq_classes(env)

        self.tools    = get_tools(env)
        self.context  = Context()
        self.executor = self.tools.Executor(12)
        self.client   = SyncRPCClient(context=self.context)
        self.service  = ThreadingRPCService(context=self.context, executor=self.executor)

        super(SyncBase, self).setUp()

    def tearDown(self):
        self.client.shutdown()
        self.service.shutdown()
        self.context.term()
        self.executor.shutdown(cancel=True)

        super(SyncBase, self).tearDown()


class SyncClientBindConnectTest(ClientBindConnectMixIn, SyncBase):
    pass

class SyncRPCCallsTest(RPCCallsMixIn, SyncBase):
    pass

