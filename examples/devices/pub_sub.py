from eventlet import sleep as green_sleep, spawn

from netcall.concurrency import get_tools
from netcall.devices     import PubService, SubClient
from netcall.green       import GreenRPCService, GreenRPCClient
from netcall.utils       import logger, setup_logger

setup_logger(logger, level='WARNING')
_tools = get_tools(env='eventlet')

#service_url = 'tcp://127.0.0.1:5555'
#service_url = 'ipc:///tmp/echo_generator.service'
service_url = 'inproc://echo_generator.service'
service_public_url = 'tcp://127.0.0.1:5555'
#client_url = 'tcp://127.0.0.1:5555'
#client_url = 'ipc:///tmp/echo_generator.client'
client_url = 'inproc://echo_generator.client'
pub_url = 'tcp://127.0.0.1:6666'

#Service
echo_generator = GreenRPCService(green_env='eventlet')

start_flag = False
@echo_generator.task
def yield_list():
    global start_flag
    
    while not start_flag:
        green_sleep(0.1)
        
    for item in range(10):
        print 'Yielding item', item
        yield item
        green_sleep(0.25)
    start_flag = False
        
@echo_generator.task
def start_producing():
    global start_flag
    start_flag = True

echo_generator.bind(service_url)
echo_generator.start()


# Proxy pub/sub

#TODO: PERF: use nocopy/buffer on zmq messages (to evaluate)
#TODO: helpers for flow control (start/stop/pause/continue flags)
#TODO: close (YIELD_CLOSE) unsubscribe the client

pub_service = PubService(env='eventlet')
pub_service.connect_service(service_url)
pub_service.bind_pub(pub_url)
pub_service.bind_client(service_public_url)

sub_client = SubClient(env='eventlet')
sub_client.connect_service(service_public_url)
sub_client.connect_sub(pub_url)
sub_client.bind_client(client_url)

pub_service.start()
sub_client.start()


# Client
echo_client1 = GreenRPCClient(green_env='eventlet')
echo_client1.connect(client_url)
echo_client2 = GreenRPCClient(green_env='eventlet')
echo_client2.connect(client_url)

print  'Partial subscription with late joiner'
echo_client2.start_producing()
a = spawn(list, echo_client1.yield_list())
print 'waiting 1.5s...'
_tools.sleep(1.5)
print 'done waiting'
b = spawn(list, echo_client2.yield_list())

print 'client 1: %r' % a.wait()
print 'client 2: %r' % b.wait()

print
print 'Synchronisation between two subscriptions thanks to flags (over RPC)'
a = spawn(list, echo_client1.yield_list())
print 'waiting 1.5s...'
_tools.sleep(1.5)
print 'done waiting'
b = spawn(list, echo_client2.yield_list())
echo_client2.start_producing()

print 'client 1: %r' % a.wait()
print 'client 2: %r' % b.wait()


