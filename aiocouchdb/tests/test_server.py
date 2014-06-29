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
import aiocouchdb.feeds
import aiocouchdb.server

URL = 'http://localhost:5984'


class ServerTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.server = aiocouchdb.server.Server(URL)

    def tearDown(self):
        self.loop.close()

    def make_future(self, obj):
        fut = asyncio.Future(loop=self.loop)
        fut.set_result(obj)
        return fut

    def make_mock_response(self, status, content, headers):
        def read():
            data = content.read()
            if data:
                return self.make_future(data)
            raise aiohttp.EofStream
        resp = aiocouchdb.client.HttpResponse('get', URL)
        resp.content = mock.Mock()
        resp.content.read = read
        resp.status = status
        resp.headers = headers
        return resp

    def test_db_updates(self):
        resp = self.make_mock_response(200, BytesIO(b'{}'),
                                       {'CONTENT-TYPE': 'application/json'})
        self.server.resource = mock.Mock()
        self.server.resource.get.return_value = self.make_future(resp)
        result = self.loop.run_until_complete(self.server.db_updates())
        self.assertIsInstance(result, dict)

    def test_db_updates_feed_continuous(self):
        resp = self.make_mock_response(200, BytesIO(b'{}'), {})
        self.server.resource = mock.Mock()
        self.server.resource.get.return_value = self.make_future(resp)
        result = self.loop.run_until_complete(
            self.server.db_updates(feed='continuous'))
        self.assertIsInstance(result, aiocouchdb.feeds.JsonFeed)

    def test_db_updates_feed_eventsource(self):
        resp = self.make_mock_response(200, BytesIO(b'{}'), {})
        self.server.resource = mock.Mock()
        self.server.resource.get.return_value = self.make_future(resp)
        result = self.loop.run_until_complete(
            self.server.db_updates(feed='eventsource'))
        self.assertIsInstance(result, aiocouchdb.feeds.Feed)

    def test_replicate(self):
        resp = self.make_mock_response(200, BytesIO(b'{}'), {})
        post = self.server.resource.post = mock.Mock()
        post.return_value = self.make_future(resp)

        coro = self.server.replicate('source', 'target')
        self.loop.run_until_complete(coro)

        _, kwargs = post.call_args
        data = kwargs['data']
        self.assertEqual({'source': 'source', 'target': 'target'}, data)

    def test_replicate_kwargs(self):
        resp = self.make_mock_response(200, BytesIO(b'{}'), {})
        post = self.server.resource.post = mock.Mock()
        post.return_value = self.make_future(resp)

        all_kwargs = {
            'auth': {'oauth': {}},
            'cancel': True,
            'continuous': True,
            'create_target': False,
            'doc_ids': ['foo', 'bar', 'baz'],
            'filter': '_design/filter',
            'headers': {'X-Foo': 'bar'},
            'proxy': 'http://localhost:8080',
            'query_params': {'test': 'passed'},
            'since_seq': 0,
            'checkpoint_interval': 5000,
            'connection_timeout': 60000,
            'http_connections': 10,
            'retries_per_request': 10,
            'socket_options': '[]',
            'use_checkpoints': True,
            'worker_batch_size': 200,
            'worker_processes': 4
        }

        for key, value in all_kwargs.items():
            kwarg = {key: value}
            coro = self.server.replicate('source', 'target', **kwarg)
            self.loop.run_until_complete(coro)

            _, kwargs = post.call_args
            data = kwargs['data']
            expected = {'source': 'source', 'target': 'target', key: value}
            self.assertEqual(expected, data)

    def test_restart(self):
        resp = self.make_mock_response(200, BytesIO(b'{"ok": true}'),
                                       {'CONTENT-TYPE': 'application/json'})
        post = self.server.resource.post = mock.Mock()
        post.return_value = self.make_future(resp)

        result = self.loop.run_until_complete(self.server.restart())
        self.assertEqual({'ok': True}, result)


class ServerFunctionalTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.server = aiocouchdb.server.Server(URL)

    def tearDown(self):
        self.loop.close()

    def test_init_with_url(self):
        self.assertIsInstance(self.server.resource, self.server.resource_class)

    def test_init_with_resource(self):
        res = self.server.resource_class(URL)
        server = aiocouchdb.server.Server(res)
        self.assertIsInstance(server.resource, server.resource_class)
        self.assertEqual(URL, self.server.resource.url)

    def test_info(self):
        result = self.loop.run_until_complete(self.server.info())
        self.assertIn('couchdb', result)
        self.assertIn('version', result)

    def test_active_tasks(self):
        result = self.loop.run_until_complete(self.server.active_tasks())
        self.assertIsInstance(result, list)

    def test_all_dbs(self):
        result = self.loop.run_until_complete(self.server.all_dbs())
        self.assertIsInstance(result, list)

    def test_log(self):
        result = self.loop.run_until_complete(self.server.log())
        self.assertIsInstance(result, str)


class ServerConfigFunctionalTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.server = aiocouchdb.server.Server(URL)

    def tearDown(self):
        self.loop.close()

    def test_config(self):
        result = self.loop.run_until_complete(self.server.config.get())
        self.assertIsInstance(result, dict)
        self.assertIn('couchdb', result)

    def test_config_get_section(self):
        result = self.loop.run_until_complete(
            self.server.config.get('couchdb'))
        self.assertIsInstance(result, dict)
        self.assertIn('uuid', result)

    def test_config_get_option(self):
        result = self.loop.run_until_complete(
            self.server.config.get('couchdb', 'uuid'))
        self.assertIsInstance(result, str)

    def test_config_set_option(self):
        result = self.loop.run_until_complete(
            self.server.config.update('test', 'aiocouchdb', 'passed'))
        self.assertEqual('', result)

    def test_config_del_option(self):
        self.loop.run_until_complete(
            self.server.config.update('test', 'aiocouchdb', 'passed'))
        result = self.loop.run_until_complete(
            self.server.config.remove('test', 'aiocouchdb'))
        self.assertEqual('passed', result)
