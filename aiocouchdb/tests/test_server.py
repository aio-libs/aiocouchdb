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
        self.assertIsInstance(self.server.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url)
        server = aiocouchdb.server.Server(res)
        self.assertIsInstance(server.resource, aiocouchdb.client.Resource)
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

    def test_authdb(self):
        db = self.server.authdb
        self.assertFalse(self.request.called)
        self.assertIsInstance(db, self.server.authdb_class)

    def test_authdb_custom_class(self):
        class CustomDatabase(object):
            def __init__(self, thing, **kwargs):
                self.resource = thing
        server = aiocouchdb.server.Server(authdb_class=CustomDatabase)
        db = server.authdb
        self.assertFalse(self.request.called)
        self.assertIsInstance(db, server.authdb_class)

    def test_authdb_name(self):
        self.assertEqual(self.server.authdb.name, '_users')

        server = aiocouchdb.server.Server(authdb_name='_authdb')
        self.assertEqual(server.authdb.name, '_authdb')

    def test_contig(self):
        self.assertIsInstance(self.server.config, aiocouchdb.server.Config)

    def test_database(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.db('db'))
        self.assert_request_called_with('HEAD', 'db')
        self.assertIsInstance(result, self.server.database_class)

    def test_database_custom_class(self):
        class CustomDatabase(object):
            def __init__(self, thing, **kwargs):
                self.resource = thing
        server = aiocouchdb.server.Server(self.url,
                                          database_class=CustomDatabase)

        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(server.db('db'))
        self.assert_request_called_with('HEAD', 'db')
        self.assertIsInstance(result, CustomDatabase)
        self.assertIsInstance(result.resource, aiocouchdb.client.Resource)

    def test_database_get_item(self):
        db = self.server['db']
        with self.assertRaises(AssertionError):
            self.assert_request_called_with('HEAD', 'db')
        self.assertIsInstance(db, self.server.database_class)

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
        self.assertIsInstance(result, aiocouchdb.feeds.EventSourceFeed)

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

    def test_stats(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.stats())
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_stats')

    def test_stats_flush(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.stats(flush=True))
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_stats', params={'flush': True})

    def test_stats_range(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.stats(range=60))
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_stats', params={'range': 60})

    def test_stats_single_metric(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.stats('httpd/requests'))
        self.assertIsInstance(result, dict)
        self.assert_request_called_with('GET', '_stats', 'httpd', 'requests')

    def test_stats_invalid_metric(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        self.assertRaises(ValueError, self.run_loop,
                          self.server.stats('httpd'))

    def test_uuids(self):
        resp = self.mock_json_response(data=b'{"uuids": ["foo"]}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.uuids())
        self.assertIsInstance(result, list)
        self.assertEqual(['foo'], result)
        self.assert_request_called_with('GET', '_uuids')

    def test_uuids_count(self):
        resp = self.mock_json_response(data=b'{"uuids": ["foo", "bar"]}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.server.uuids(count=2))
        self.assertIsInstance(result, list)
        self.assertEqual(['foo', 'bar'], result)
        self.assert_request_called_with('GET', '_uuids', params={'count': 2})


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
