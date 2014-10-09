# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import contextlib
import functools
import unittest
import unittest.mock as mock
from collections import deque

import aiocouchdb.client
from aiocouchdb.client import urljoin


def run_in_loop(f):
    @functools.wraps(f)
    def wrapper(testcase, *args, **kwargs):
        coro = asyncio.coroutine(f)
        future = asyncio.wait_for(coro(testcase, *args, **kwargs),
                                  timeout=testcase.timeout)
        return testcase.loop.run_until_complete(future)
    return wrapper


class MetaAioTestCase(type):

    def __new__(cls, name, bases, attrs):
        for key, obj in attrs.items():
            if key.startswith('test_'):
                attrs[key] = run_in_loop(obj)
        return super().__new__(cls, name, bases, attrs)


class TestCase(unittest.TestCase, metaclass=MetaAioTestCase):

    url = 'http://localhost:5984'
    timeout = 5

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.patch = mock.patch('aiohttp.request')
        self.request = self.patch.start()
        self.set_response(self.prepare_response())

    def tearDown(self):
        self.patch.stop()
        self.loop.close()

    def future(self, obj):
        fut = asyncio.Future(loop=self.loop)
        fut.set_result(obj)
        return fut

    def prepare_response(self, *, status=200, headers=None, data=b'', err=None):
        def side_effect(*args, **kwargs):
            fut = asyncio.Future(loop=self.loop)
            if queue:
                resp.content.at_eof.return_value = False
                fut.set_result(queue.popleft())
            elif err:
                fut.set_exception(err)
            else:
                resp.content.at_eof.return_value = True
                fut.set_result(b'')
            return fut
        headers = headers or {}
        headers.setdefault('CONTENT-TYPE', 'application/json')

        queue = deque(data if isinstance(data, list) else [data])

        resp = aiocouchdb.client.HttpResponse('', '')
        resp.status = status
        resp.headers = headers
        resp.content = unittest.mock.Mock()
        resp.content._buffer = bytearray()
        resp.content.at_eof.return_value = False
        resp.content.read.side_effect = side_effect
        resp.close = mock.Mock()

        return resp

    @contextlib.contextmanager
    def response(self, *, status=200, headers=None, data=b'', err=None):
        resp = self.prepare_response(status=status,
                                     headers=headers,
                                     data=data,
                                     err=err)
        self.set_response(resp)
        yield resp
        self.set_response(self.prepare_response())

    def set_response(self, resp):
        self.request.return_value = self.future(resp)

    def assert_request_called_with(self, method, *path, **kwargs):
        self.assertTrue(self.request.called and self.request.call_count >= 1)

        call_args, call_kwargs = self.request.call_args
        self.assertEqual((method, urljoin(self.url, *path)), call_args)

        kwargs.setdefault('data', None)
        kwargs.setdefault('headers', {})
        kwargs.setdefault('params', {})
        for key, value in kwargs.items():
            self.assertIn(key, call_kwargs)
            if value is not Ellipsis:
                self.assertEqual(value, call_kwargs[key])

