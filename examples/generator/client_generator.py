#!/usr/bin/env python

""" A simple asynchronous RPC client that shows how to:

    * use functions yielding (generators) from a RPC service
    * use next(), send(), throw() and close() on the generators
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#  Axel Voitier
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from netcall.threading import ThreadingRPCClient

if __name__ == '__main__':
    echo_generator = ThreadingRPCClient()
    echo_generator.connect('tcp://127.0.0.1:5555')

    a_list = range(10)
    print 'Get a list:', a_list
    for item in echo_generator.yield_list(a_list):
        print 'Received item', item

    print
    print 'Getting an echo generator with argument 1'
    echo = echo_generator.echo(1)
    print 'next(): Receiving back the argument (initial value):', next(echo)
    print 'next(): Receiving back None (nothing was sent):', next(echo)
    print 'send(2): Receiving back:', echo.send(2)
    print 'send(3): Receiving back:', echo.send(3)
    print 'throw(ValueError("Hello")): Receiving back the exception', repr(echo.throw(ValueError('Hello')))
    print 'close(): Terminating the generator:', echo.close()

    print
    print 'Calling a generator and closing it implicitely (garbage collector did the job)'
    next(echo_generator.echo(1))
