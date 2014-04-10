# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

from netcall.utils       import get_zmq_classes
from netcall.green       import GreenRPCClient, GreenRPCService
from netcall.concurrency import get_tools

from .base          import BaseCase
from .client_mixins import ClientBindConnectMixIn
from .rpc_mixins    import RPCCallsMixIn


class EventletBase(BaseCase):

    def setUp(self):
        env = 'eventlet'
        Context, _ = get_zmq_classes(env)

        self.tools    = get_tools(env)
        self.context  = Context()
        self.executor = self.tools.Executor(24)
        self.client   = GreenRPCClient(context=self.context, green_env=env, executor=self.executor)
        self.service  = GreenRPCService(context=self.context, green_env=env, executor=self.executor)

        super(EventletBase, self).setUp()

    def tearDown(self):
        self.client.shutdown()
        self.service.shutdown()
        self.context.term()
        self.executor.shutdown(cancel=True)

        super(EventletBase, self).tearDown()


try:
    import eventlet

    class EventletClientBindConnectTest(ClientBindConnectMixIn, EventletBase):
        pass

    class EventletRPCCallsTest(RPCCallsMixIn, EventletBase):
        pass

except ImportError:
    pass
