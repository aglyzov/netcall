#!/usr/bin/env python

""" A simple RPC server that shows how to serve generators
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2012-2014. Brian Granger, Min Ragan-Kelley, Alexander Glyzov
#  Axel Voitier
#
#  Distributed under the terms of the BSD License.  The full license is in
#  the file LICENSE distributed as part of this software.
#-----------------------------------------------------------------------------

from netcall import ThreadingRPCService

echo_generator = ThreadingRPCService()
        
@echo_generator.task
def yield_list(a_list):
    for item in a_list:
        print 'Yielding item', item
        yield item

@echo_generator.task
def echo(value):
    print
    try:
        while True:
            try:
                print 'Send', value, 'back'
                value = yield value
                print 'Received', value
            except GeneratorExit:
                raise # Don't catch for echo, re-raise to exit
            except Exception, e:
                value = e
                print 'Received exception', repr(value)
    finally:
        print 'Exiting generator on request'


if __name__ == '__main__':
    echo_generator.bind('tcp://127.0.0.1:5555')
    echo_generator.start()
    echo_generator.serve()
