# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json
import types

import aiocouchdb.client
import aiocouchdb.feeds
import aiocouchdb.database
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class DatabaseTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_db = urljoin(self.url, 'db')
        self.db = aiocouchdb.database.Database(self.url_db)

    def test_init_with_url(self):
        self.assertIsInstance(self.db.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_db)
        server = aiocouchdb.server.Server(res)
        self.assertIsInstance(server.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_db, self.db.resource.url)

    def test_exists(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.exists())
        self.assert_request_called_with('HEAD', 'db')
        self.assertTrue(result)

    def test_exists_forbidden(self):
        resp = self.mock_json_response(data=b'{}')
        resp.status = 403
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.exists())
        self.assert_request_called_with('HEAD', 'db')
        self.assertFalse(result)

    def test_exists_not_found(self):
        resp = self.mock_json_response(data=b'{}')
        resp.status = 404
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.exists())
        self.assert_request_called_with('HEAD', 'db')
        self.assertFalse(result)

    def test_info(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.info())
        self.assert_request_called_with('GET', 'db')
        self.assertIsInstance(result, dict)

    def test_create(self):
        resp = self.mock_json_response(data=b'{"ok": true}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.create())
        self.assert_request_called_with('PUT', 'db')
        self.assertTrue(result)

    def test_delete(self):
        resp = self.mock_json_response(data=b'{"ok": true}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.delete())
        self.assert_request_called_with('DELETE', 'db')
        self.assertTrue(result)

    def test_all_docs(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.all_docs())
        self.assert_request_called_with('GET', 'db', '_all_docs')
        self.assertIsInstance(result, aiocouchdb.feeds.ViewFeed)

    def test_all_docs_params(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        all_params = {
            'attachments': False,       
            'conflicts': True,
            'descending': True,
            'endkey': 'foo',
            'endkey_docid': 'foo_id',
            'include_docs': True,
            'inclusive_end': False,
            'limit': 10,
            'skip': 20,
            'stale': 'ok',
            'startkey': 'bar',
            'startkey_docid': 'bar_id',
            'update_seq': True
        }

        for key, value in all_params.items():
            self.run_loop(self.db.all_docs(**{key: value}))
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            self.assert_request_called_with('GET', 'db', '_all_docs',
                                            params={key: value})

    def test_all_docs_key(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.all_docs('foo'))
        self.assert_request_called_with('GET', 'db', '_all_docs',
                                        params={'key': '"foo"'})

    def test_all_docs_keys(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.all_docs('foo', 'bar', 'baz'))
        self.assert_request_called_with('POST', 'db', '_all_docs',
                                        data={'keys': ('foo', 'bar', 'baz')})

    def test_bulk_docs(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.bulk_docs([{'_id': 'foo'}, {'_id': 'bar'}]))
        self.assert_request_called_with('POST', 'db', '_bulk_docs')
        data = self.request.call_args[1]['data']
        self.assertIsInstance(data, types.GeneratorType)
        self.assertEqual(b'{"docs": [{"_id": "foo"},{"_id": "bar"}]}',
                         b''.join(data))

    def test_bulk_docs_all_or_nothing(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.bulk_docs([{'_id': 'foo'}, {'_id': 'bar'}],
                                        all_or_nothing=True))
        self.assert_request_called_with('POST', 'db', '_bulk_docs')
        data = self.request.call_args[1]['data']
        self.assertIsInstance(data, types.GeneratorType)
        self.assertEqual(b'{"all_or_nothing": true, "docs": '
                         b'[{"_id": "foo"},{"_id": "bar"}]}',
                         b''.join(data))

    def test_bulk_docs_new_edits(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.bulk_docs([{'_id': 'foo'}], new_edits=False))
        self.assert_request_called_with('POST', 'db', '_bulk_docs',
                                        params={'new_edits': False})

    def test_changes(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.changes())
        self.assertIsInstance(result, aiocouchdb.feeds.ChangesFeed)
        self.assert_request_called_with('GET', 'db', '_changes')

    def test_changes_longpoll(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.changes(feed='longpoll'))
        self.assertIsInstance(result, aiocouchdb.feeds.LongPollChangesFeed)
        self.assert_request_called_with('GET', 'db', '_changes',
                                        params={'feed': 'longpoll'})

    def test_changes_continuous(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.changes(feed='continuous'))
        self.assertIsInstance(result, aiocouchdb.feeds.ContinuousChangesFeed)
        self.assert_request_called_with('GET', 'db', '_changes',
                                        params={'feed': 'continuous'})

    def test_changes_eventsource(self):
        resp = self.mock_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.db.changes(feed='eventsource'))
        self.assertIsInstance(result, aiocouchdb.feeds.EventSourceChangesFeed)
        self.assert_request_called_with('GET', 'db', '_changes',
                                        params={'feed': 'eventsource'})

    def test_changes_doc_ids(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.changes('foo', 'bar'))
        self.assert_request_called_with('POST', 'db', '_changes',
                                        data={'doc_ids': ('foo', 'bar')},
                                        params={'filter': '_doc_ids'})

    def test_changes_assert_filter_doc_ids(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.assertRaises(
            AssertionError,
            self.run_loop,
            self.db.changes('foo', 'bar', filter='somefilter')
        )

    def test_changes_params(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        all_params = {
            'att_encoding_info': False,
            'attachments': True,
            'conflicts': True,
            'descending': True,
            'feed': 'continuous',
            'filter': 'some/filter',
            'heartbeat': 1000,
            'include_docs': True,
            'limit': 20,
            'since': 'now',
            'style': 'all_docs',
            'timeout': 3000,
            'view': 'some/view'
        }

        for key, value in all_params.items():
            self.run_loop(self.db.changes(**{key: value}))
            params = {key: value}
            if key == 'view':
                params['filter'] = '_view'
            self.assert_request_called_with('GET', 'db', '_changes',
                                            params=params)

    def test_compact(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.compact())
        self.assert_request_called_with('POST', 'db', '_compact')

    def test_compact_ddoc(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        self.run_loop(self.db.compact('ddoc'))
        self.assert_request_called_with('POST', 'db', '_compact', 'ddoc')
