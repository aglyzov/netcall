# vim: fileencoding=utf-8 et ts=4 sts=4 sw=4 tw=0

from unittest import skip
from types    import GeneratorType

from netcall import RemoteRPCError, RPCTimeoutError, RPCServiceBase


class RPCCallsMixIn(object):

    def setUp(self):
        super(RPCCallsMixIn, self).setUp()

        assert self.tools
        assert self.client
        assert self.service

        self.url = self.urls[0]
        self.service.bind(self.url)
        self.client.connect(self.url)

        self.service.start()


    def assertNotImplementedRemotely(self, func_name):
        err_msg = "NotImplementedError: Unregistered procedure '%s'" % func_name
        with self.assertRaisesRegexp(RemoteRPCError, err_msg):
            self.client.call(func_name)


    def test_netcall_reserved(self):
        # Avoid asking for the special _* GEN names, they raise another exception
        reserved = [n for n in RPCServiceBase._RESERVED if not n.startswith('_')]
        for name in reserved:
            self.assertNotImplementedRemotely(name)

    def test_cannot_register_netcall_reserved(self):
        def dummy():
            pass

        for name in RPCServiceBase._RESERVED:
            with self.assertRaisesRegexp(ValueError, '%s is a reserved function name' % name):
                self.service.register(dummy, name=name)

        self.assertDictEqual(self.service.procedures, {})

        # Avoid asking for the special _* GEN names, they raise another exception
        reserved = [n for n in RPCServiceBase._RESERVED if not n.startswith('_')]
        for name in reserved:
            self.assertNotImplementedRemotely(name)

    def test_cannot_register_object_netcall_reserved(self):
        def dummy():
            pass

        class Dummy(object):
            pass

        toy = Dummy()
        for name in RPCServiceBase._RESERVED:
            setattr(toy, name, dummy)
        self.service.register_object(toy)

        self.assertDictEqual(self.service.procedures, {})

        # Avoid asking for the special _* GEN names, they raise another exception
        reserved = [n for n in RPCServiceBase._RESERVED if not n.startswith('_')]
        for name in reserved:
            self.assertNotImplementedRemotely(name)


    def test_function(self):
        @self.service.register
        def fixture():
            return 'This is a test'

        self.assertEqual(self.client.fixture(), 'This is a test')

    def test_function_named(self):
        @self.service.register(name='work')
        def fixture():
            return 'This is a test'

        self.assertEqual(self.client.work(), 'This is a test')
        self.assertNotImplementedRemotely("fixture")

    def test_function_args(self):
        @self.service.register
        def fn_double_one_arg(arg1):
            return arg1 * 2

        @self.service.register
        def fn_mul_two_args(arg1, arg2):
            return arg1 * arg2

        @self.service.register
        def fn_mul_vargs(*args):
            mul = 1
            for arg in args:
                mul = mul * arg
            return mul

        self.assertEqual(self.client.fn_double_one_arg(7),   14)
        self.assertEqual(self.client.fn_mul_two_args(7, 3),  21)
        self.assertEqual(self.client.fn_mul_vargs(7, 3, 10), 210)

    def test_function_kwargs(self):
        @self.service.register
        def fn_double_one_kwarg(arg1=None):
            return arg1 * 2

        @self.service.register
        def fn_mul_two_kwargs(arg1=None, arg2=None):
            return arg1 * arg2

        @self.service.register
        def fn_mul_vkwargs(**kwargs):
            mul = 1
            for arg in kwargs.values():
                mul = mul * arg
            return mul

        self.assertEqual(self.client.fn_double_one_kwarg(arg1=7), 14)
        self.assertEqual(self.client.fn_mul_two_kwargs(arg1=7, arg2=3), 21)
        self.assertEqual(self.client.fn_mul_vkwargs(arg1=7, arg2=3, arg3=10), 210)

        with self.assertRaisesRegexp(RemoteRPCError, "TypeError: .*() got an unexpected keyword argument 'argNot'"):
            self.client.fn_double_one_kwarg(argNot=7)
        with self.assertRaisesRegexp(RemoteRPCError, "TypeError: .*() got an unexpected keyword argument 'argNot'"):
            self.client.fn_mul_two_kwargs(arg1=7, arg2=3, argNot=17)

    def test_function_args_kwargs(self):
        @self.service.register
        def fn_one_arg_one_kwarg(arg1, arg2=None):
            return arg1 * arg2

        @self.service.register
        def fn_vargs_vkwargs(*args, **kwargs):
            return args[0] * sorted(kwargs.items())[0][1]

        self.assertEqual(self.client.fn_one_arg_one_kwarg(7, arg2=3), 21)
        self.assertEqual(self.client.fn_vargs_vkwargs(7, 5, argA=3, argB=18), 21)


    def test_timeout(self):
        @self.service.register
        def fixture():
            self.tools.sleep(0.2)

        with self.assertRaises(RPCTimeoutError):
            self.client.call('fixture', timeout=0.1)

    @skip('TODO')
    def test_ignore_missing_service(self):
        fail_url = self.extra[0]

        self.client.connect(fail_url)
        self.assertTrue(fail_url in self.client.connected)

        self.service.register(lambda: 123, name='fixture')

        call = lambda: self.client.call('fixture', timeout=0.5)

        self.assertEqual(call(), 123)  # ask existing service
        self.assertEqual(call(), 123)  # skip missing service


    def test_object(self):
        toy = ToyObject(12)
        self.service.register_object(toy)

        self.assertEqual(self.client.value(), toy.value())
        self.assertIsNone(self.client.restricted())

    def test_object_private(self):
        toy = ToyObject(12)
        self.service.register_object(toy)

        self.assertNotImplementedRemotely("_private")

    def test_object_restricted(self):
        toy = ToyObject(12)
        self.service.register_object(toy, restricted=['restricted'])

        self.assertNotImplementedRemotely("restricted")

    def test_object_namespace_one_level(self):
        toys = []
        for i, n in enumerate('abc'):
            toys.append(ToyObject(i))
            self.service.register_object(toys[i], namespace=n)

        self.assertNotImplementedRemotely('value')
        self.assertEqual(self.client.a.value(), toys[0].value())
        self.assertEqual(self.client.b.value(), toys[1].value())
        self.assertEqual(self.client.c.value(), toys[2].value())

    def test_object_namespace_n_levels(self):
        toy = ToyObject(12)
        self.service.register_object(toy, namespace='this.has.a.toy')

        self.assertEqual(self.client.this.has.a.toy.value(), toy.value())

    def test_object_module(self):
        import random
        self.service.register_object(random)

        self.assertIsInstance(self.client.randint(0, 10), int)
        self.assertIsInstance(self.client.random(), float)


    def test_generator(self):
        fixture = list(range(10))
        @self.service.register
        def yielder():
            for i in fixture:
                yield i

        gen = self.client.yielder()
        self.assertIsInstance(gen, GeneratorType)
        self.assertEqual(list(gen), fixture)
        self.assertDictEqual(self.service.generators, {})

    def test_generator_none(self):
        @self.service.register
        def yielder():
            for i in range(10):
                yield

        self.assertEqual(list(self.client.yielder()), [None] * 10)
        self.assertDictEqual(self.service.generators, {})

    def test_generator_next(self):
        @self.service.register
        def echo(value=None):
            while True:
                yield value

        gen = self.client.echo(1)
        #self.assertEqual(gen.next(), 1) # Not working on Python 3. Same thing anyway.
        self.assertEqual(next(gen), 1)
        gen = None
        self.assertDictEqual(self.service.generators, {})

    def test_generator_send(self):
        @self.service.register
        def echo(value=None):
            while True:
                value = (yield value)

        gen = self.client.echo(1)
        self.assertEqual(gen.send(None), 1)
        self.assertEqual(gen.send(2), 2)
        gen = None
        self.assertDictEqual(self.service.generators, {})

    def test_generator_throw(self):
        @self.service.register
        def echo(value=None):
            while True:
                try:
                    value = (yield value)
                    print('Received value', value)
                except Exception as e:
                    value = e

        gen = self.client.echo(1)
        next(gen)
        e = gen.throw(TypeError, 'spam')
        self.assertIsInstance(e, Exception)
        with self.assertRaisesRegexp(TypeError, 'spam'):
            raise e
        gen = None
        self.assertDictEqual(self.service.generators, {})

    def test_generator_close_explicit(self):
        closed = [False]
        @self.service.register
        def echo(value=None):
            try:
                while True: yield value
            finally:
                closed[0] = True

        gen = self.client.echo(1)
        next(gen)

        gen.close()

        self.tools.sleep(0.2)  # allows GC to collect the client generator
                               # and its Queue

        self.assertTrue(closed[0])
        with self.assertRaises(StopIteration):
            next(gen)

        self.assertFalse(self.service.generators)
        self.assertFalse(self.client._gen_queues)

    def test_generator_close_implicit(self):
        closed = [False]
        @self.service.register
        def repeat(value=None):
            try:
                while True: yield value
            finally:
                closed[0] = True

        gen = self.client.repeat(1)
        next(gen)

        del gen  # this implicitly sends _CLOSE to the service
                 # cleaning up there as well

        self.tools.sleep(0.2)  # allows GC to collect the client generator
                               # and its Queue

        self.assertTrue(closed[0])
        self.assertFalse(self.service.generators)
        self.assertFalse(self.client._gen_queues)


class ToyObject(object):

    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def _private(self):
        pass

    def restricted(self):
        pass

