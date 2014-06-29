# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiohttp
import unittest
import unittest.mock as mock
from io import BytesIO

import aiocouchdb.client

URL = 'http://localhost:5984'


class ResourceTestCase(unittest.TestCase):

    def setUp(self):
        self.patch = mock.patch('aiohttp.request')
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.request = self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.loop.close()

    def test_head_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.head())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('HEAD', URL), args)

    def test_get_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.get())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('GET', URL), args)

    def test_post_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.post())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('POST', URL), args)

    def test_put_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.put())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('PUT', URL), args)

    def test_delete_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.delete())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('DELETE', URL), args)

    def test_copy_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.copy())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('COPY', URL), args)

    def test_options_request(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.options())
        self.assertTrue(self.request.called)
        args, _ = self.request.call_args
        self.assertEquals(('OPTIONS', URL), args)

    def test_to_str(self):
        self.assertEqual("<Resource @ 'http://localhost:5984'>",
                         str(aiocouchdb.client.Resource(URL)))

    def test_on_call(self):
        res = aiocouchdb.client.Resource('http://localhost:5984')
        new_res = res('foo', 'bar/baz')
        self.assertIsNot(res, new_res)
        self.assertEqual('http://localhost:5984/foo/bar%2Fbaz', new_res.url)

    def test_empty_call(self):
        res = aiocouchdb.client.Resource('http://localhost:5984')
        new_res = res()
        self.assertIsNot(res, new_res)
        self.assertEqual('http://localhost:5984', new_res.url)

    def test_request_with_path(self):
        res = aiocouchdb.client.Resource(URL)
        self.loop.run_until_complete(res.request('get', 'foo/bar'))
        args, _ = self.request.call_args
        self.assertTrue(self.request.called)
        self.assertEquals(('get', URL + '/foo%2Fbar'), args)


class HttpRequestTestCase(unittest.TestCase):

    def test_encode_json_body(self):
        req = aiocouchdb.client.HttpRequest('post', URL, data={'foo': 'bar'})
        self.assertEqual(b'{"foo": "bar"}', req.body)

    def test_correct_encode_boolean_params(self):
        req = aiocouchdb.client.HttpRequest('get', URL, params={'foo': True})
        self.assertEqual('/?foo=true', req.path)

        req = aiocouchdb.client.HttpRequest('get', URL, params={'bar': False})
        self.assertEqual('/?bar=false', req.path)


class HttpResponseTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_read_body(self):
        content = BytesIO(b'{"couchdb": "Welcome!"}')
        def read():
            data = content.read()
            if data:
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(data)
                return fut
            raise aiohttp.EofStream
        resp = aiocouchdb.client.HttpResponse('get', URL)
        resp.content = mock.Mock()
        resp.content.read = read
        result = self.loop.run_until_complete(resp.read())
        self.assertEqual(b'{"couchdb": "Welcome!"}', result)

    def test_decode_json_body(self):
        content = BytesIO(b'{"couchdb": "Welcome!"}')
        def read():
            data = content.read()
            if data:
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(data)
                return fut
            raise aiohttp.EofStream
        resp = aiocouchdb.client.HttpResponse('get', URL)
        resp.headers = {'CONTENT-TYPE': 'application/json'}
        resp.content = mock.Mock()
        resp.content.read = read
        result = self.loop.run_until_complete(resp.json())
        self.assertEqual({'couchdb': 'Welcome!'}, result)

    def test_decode_json_from_empty_body(self):
        def read():
            data = content.read()
            if data:
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(data)
                return fut
            raise aiohttp.EofStream
        content = BytesIO(b'\n')
        resp = aiocouchdb.client.HttpResponse('get', URL)
        resp.headers = {'CONTENT-TYPE': 'application/json'}
        resp.content = mock.Mock()
        resp.content.read = read
        result = self.loop.run_until_complete(resp.json())
        self.assertEqual(None, result)
