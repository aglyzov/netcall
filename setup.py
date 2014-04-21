#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name = "netcall",
    version = "0.3.6",
    packages = find_packages(),

    install_requires = ['pyzmq', 'pebble'],

    author = "Alexander Glyzov",
    author_email = "bonoba@gmail.com",
    description = "A simple Python RPC system (ZeroMQ + Threading/Tornado/Gevent/Eventlet/Greenhouse)",
    license = "Modified BSD",
    keywords = "ZeroMQ ZMQ PyZMQ Thread Threading Tornado Green Greenlet Gevent Eventlet Greenhouse RPC async",
    url = "http://github.com/aglyzov/netcall",
)

