# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0 fdm=marker fmr=#{,#}

from netcall.green import GreenRPCClient, GreenRPCService
from netcall.utils import get_zmq_classes

from .base          import BaseCase
from .client_mixins import ClientBindConnectMixIn
from .rpc_mixins    import RPCCallsMixIn


class GreenhouseBase(BaseCase):

    def setUp(self):
        green_env  = 'greenhouse'
        Context, _ = get_zmq_classes(env=green_env)

        self.context = Context()
        self.client  = GreenRPCClient(context=self.context, green_env=green_env)
        self.service = GreenRPCService(context=self.context, green_env=green_env)

        super(GreenhouseBase, self).setUp()

    def tearDown(self):
        self.client.shutdown()
        self.service.shutdown()
        self.context.term()

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
