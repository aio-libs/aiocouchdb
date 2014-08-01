# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import unittest
import unittest.mock as mock
from collections import deque

import aiocouchdb.client
from aiocouchdb.client import urljoin


class TestCase(unittest.TestCase):

    url = 'http://localhost:5984'

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.patch = mock.patch('aiohttp.request')
        self.request = self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.loop.close()

    def future(self, obj):
        fut = asyncio.Future(loop=self.loop)
        fut.set_result(obj)
        return fut

    def run_loop(self, coro):
        return self.loop.run_until_complete(coro)

    def mock_response(self, status=200, headers=None, data=b'', err=None):
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
        queue = deque(data if isinstance(data, list) else [data])
        resp = aiocouchdb.client.HttpResponse('', '')
        resp.status = status
        resp.headers = headers or {}
        resp.content = unittest.mock.Mock()
        resp.content.at_eof.return_value = False
        resp.content.read.side_effect = side_effect
        resp.close = mock.Mock()
        return resp

    def mock_json_response(self, status=200, headers=None, data=b''):
        headers = headers or {}
        headers.update({'CONTENT-TYPE': 'application/json'})
        return self.mock_response(status, headers, data)

    def assert_request_called_with(self, method, *path, **kwargs):
        self.assertTrue(self.request.called_once)
        call_args, call_kwargs = self.request.call_args
        self.assertEqual((method, urljoin(self.url, *path)), call_args)
        for key, value in kwargs.items():
            self.assertIn(key, call_kwargs)
            self.assertEqual(value, call_kwargs[key])

