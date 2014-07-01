# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import http.cookies

import aiocouchdb.authn
import aiocouchdb.client
import aiocouchdb.feeds
import aiocouchdb.server
import aiocouchdb.tests.utils as utils


class ServerTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.server = aiocouchdb.server.Server(self.url)

    def test_init_with_url(self):
        self.assertIsInstance(self.server.resource, self.server.resource_class)

    def test_init_with_resource(self):
        res = self.server.resource_class(self.url)
        server = aiocouchdb.server.Server(res)
        self.assertIsInstance(server.resource, server.resource_class)
        self.assertEqual(self.url, self.server.resource.url)

    def test_info(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.info())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET')

    def test_active_tasks(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.active_tasks())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_active_tasks')

    def test_all_dbs(self):
        resp = self.mock_json_response(data=b'[]')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.all_dbs())
        self.assertIsInstance(result, list)
        self.assert_request_called_with('GET', '_all_dbs')

    def test_contig(self):
        self.assertIsInstance(self.server.config, aiocouchdb.server.Config)

    def test_db_updates(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.db_updates())
        self.assert_request_called_with('GET', '_db_updates')
        self.assertIsInstance(result, dict)

    def test_db_updates_feed_continuous(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.db_updates(feed='continuous'))
        self.assert_request_called_with('GET', '_db_updates',
                                        params={'feed': 'continuous'})
        self.assertIsInstance(result, aiocouchdb.feeds.JsonFeed)

    def test_db_updates_feed_eventsource(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.db_updates(feed='eventsource'))
        self.assert_request_called_with('GET', '_db_updates',
                                        params={'feed': 'eventsource'})
        self.assertIsInstance(result, aiocouchdb.feeds.Feed)

    def test_log(self):
        resp = self.mock_response(data=b'hello')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.log())
        self.assertIsInstance(result, str)
        self.assert_request_called_with('GET', '_log')

    def test_replicate(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.replicate('source', 'target'))
        self.assertIsInstance(result, dict)
        self.assert_request_called_with(
            'POST', '_replicate', data={'source': 'source', 'target': 'target'})

    def test_replicate_kwargs(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        all_kwargs = {
            'authobj': {'oauth': {}},
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
            result = self.run_loop(self.server.replicate('source', 'target',
                                                         **{key: value}))
            self.assertIsInstance(result, dict)
            if key == 'authobj':
                key = 'auth'
            data = {'source': 'source', 'target': 'target', key: value}
            self.assert_request_called_with('POST', '_replicate', data=data)

    def test_restart(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.restart())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('POST', '_restart')

    def test_session(self):
        self.assertIsInstance(self.server.session, aiocouchdb.server.Session)


class ServerConfigFunctionalTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.server = aiocouchdb.server.Server(self.url)

    def test_config(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.config.get())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_config')

    def test_config_get_section(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.config.get('couchdb'))
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_config', 'couchdb')

    def test_config_get_option(self):
        resp = self.mock_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.config.get('couchdb', 'uuid'))
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_config', 'couchdb', 'uuid')

    def test_config_set_option(self):
        resp = self.mock_response(data=b'"relax!"')
        self.request.return_value = self.future(resp)

        result = self.run_loop(
            self.server.config.update('test', 'aiocouchdb', 'passed'))
        self.assertIsInstance(result, str)
        self.assert_request_called_with('PUT', '_config', 'test', 'aiocouchdb',
                                        data='passed')

    def test_config_del_option(self):
        resp = self.mock_response(data=b'"passed"')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.config.remove('test', 'aiocouchdb'))
        self.assertIsInstance(result, str)
        self.assert_request_called_with('DELETE',
                                        '_config', 'test', 'aiocouchdb')


class SessionTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.server = aiocouchdb.server.Server(self.url)
        self.resp = self.mock_json_response(data=b'{"ok": true}')
        self.resp.cookies = http.cookies.SimpleCookie()
        self.request.return_value = self.future(self.resp)

    def test_open_session(self):
        self.resp.cookies = http.cookies.SimpleCookie({'AuthSession': 'secret'})
        auth = self.run_loop(self.server.session.open('foo', 'bar'))

        self.assertIsInstance(auth, aiocouchdb.authn.CookieAuthProvider)
        self.assertIs(auth._cookies, self.resp.cookies)

        self.assert_request_called_with(
            'POST', '_session', data={'name': 'foo', 'password': 'bar'})

    def test_session_info(self):
        result = self.run_loop(self.server.session.info())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_session')

    def test_close_session(self):
        result = self.run_loop(self.server.session.close())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('DELETE', '_session')
