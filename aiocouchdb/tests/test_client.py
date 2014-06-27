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
