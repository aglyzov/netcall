from random import randint

from gevent import sleep as green_sleep, spawn

from zmq           import ROUTER, DEALER
from netcall.green import GreenRPCService, GreenRPCClient
from netcall.utils import get_zmq_classes, get_green_tools, logger, setup_logger
from netcall.devices import NamespaceRouter

setup_logger(logger, level='WARNING')

class Echo(object):
    def __init__(self, nb):
        self.nb = nb
        
    def echo(self, value):
        print '#', self.nb, 'received', value
        return value


# Service 1
echo_service1 = GreenRPCService(green_env='gevent', identity='echoer1')
echo1 = Echo(1)
#echo_service1.register_object(echo1, namespace='echoer1')
echo_service1.register_object(echo1)
    
echo_service1.connect('ipc:///tmp/echo.service')
#echo_service1.bind('tcp://127.0.0.1:5555')
echo_service1.start()


# Service 2
echo_service2 = GreenRPCService(green_env='gevent', identity='echoer2')
echo2 = Echo(2)
#echo_service2.register_object(echo2, namespace='echoer2')
echo_service2.register_object(echo2)
    
echo_service2.connect('ipc:///tmp/echo.service')
#echo_service.bind('tcp://127.0.0.1:5555')
echo_service2.start()


# Proxy/Router
router = NamespaceRouter(env='gevent')
router.bind_client('tcp://127.0.0.1:5555')
router.bind_service('ipc:///tmp/echo.service')
router.start()

green_sleep(0.25)

# Client
echo_client = GreenRPCClient(green_env='gevent')
echo_client.connect('tcp://127.0.0.1:5555')

print echo_client.echoer1.echo('Hello, 1')
print echo_client.echoer2.echo('Hello, 2')
