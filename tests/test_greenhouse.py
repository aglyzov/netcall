# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

from netcall.utils       import get_zmq_classes
from netcall.green       import GreenRPCClient, GreenRPCService
from netcall.concurrency import get_tools

from .base          import BaseCase
from .client_mixins import ClientBindConnectMixIn
from .rpc_mixins    import RPCCallsMixIn


class GreenhouseBase(BaseCase):

    def setUp(self):
        env = 'greenhouse'
        Context, _ = get_zmq_classes(env)

        self.tools    = get_tools(env)
        self.context  = Context()
        self.executor = self.tools.Executor(24)
        self.client   = GreenRPCClient(context=self.context, green_env=env, executor=self.executor)
        self.service  = GreenRPCService(context=self.context, green_env=env, executor=self.executor)

        super(GreenhouseBase, self).setUp()

    def tearDown(self):
        self.client.shutdown()
        self.service.shutdown()
        self.context.term()
        self.executor.shutdown(cancel=True)

        super(GreenhouseBase, self).tearDown()

try:
    raise ImportError  # disable immature greenhouse support

    import greenhouse

    class GeventClientBindConnectTest(ClientBindConnectMixIn, GreenhouseBase):
        pass

    class GeventRPCCallsTest(RPCCallsMixIn, GreenhouseBase):
        pass

except ImportError:
    pass
