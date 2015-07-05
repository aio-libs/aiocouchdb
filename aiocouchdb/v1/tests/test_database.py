# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json
import random
import types

import aiocouchdb.client
import aiocouchdb.errors
import aiocouchdb.feeds
import aiocouchdb.v1.database
import aiocouchdb.v1.server
import aiocouchdb.v1.security

from . import utils


class DatabaseTestCase(utils.DatabaseTestCase):

    def test_init_with_url(self):
        self.assertIsInstance(self.db.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_db)
        db = aiocouchdb.v1.database.Database(res)
        self.assertIsInstance(db.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_db, db.resource.url)

    def test_init_with_name(self):
        res = aiocouchdb.client.Resource(self.url_db)
        db = aiocouchdb.v1.database.Database(res, dbname='foo')
        self.assertEqual(db.name, 'foo')

    def test_init_with_name_from_server(self):
        server = aiocouchdb.v1.server.Server()
        db = yield from server.db('foo')
        self.assertEqual(db.name, 'foo')

    def test_exists(self):
        result = yield from self.db.exists()
        self.assert_request_called_with('HEAD', self.db.name)
        self.assertTrue(result)

    @utils.with_fixed_admin_party('root', 'relax')
    def test_exists_forbidden(self, root):
        yield from self.db.security.update_members(auth=root, names=['foo'])
        with self.response(status=403):
            result = yield from self.db.exists()
            self.assert_request_called_with('HEAD', self.db.name)
        self.assertFalse(result)

    def test_exists_not_found(self):
        with self.response(status=404):
            dbname = self.new_dbname()
            result = yield from self.server[dbname].exists()
            self.assert_request_called_with('HEAD', dbname)
        self.assertFalse(result)

    def test_info(self):
        with self.response(data=b'{}'):
            result = yield from self.db.info()
            self.assert_request_called_with('GET', self.db.name)
        self.assertIsInstance(result, dict)

    def test_create(self):
        with self.response(data=b'{"ok": true}'):
            try:
                result = yield from self.db.create()
            except aiocouchdb.errors.PreconditionFailed:
                # Because we'd created it already during setUp routines
                result = {'ok': True}
            self.assert_request_called_with('PUT', self.db.name)
        self.assertEqual({'ok': True}, result)

    def test_delete(self):
        with self.response(data=b'{"ok": true}'):
            result = yield from self.db.delete()
            self.assert_request_called_with('DELETE', self.db.name)
        self.assertEqual({'ok': True}, result)

    def test_all_docs(self):
        with (yield from self.db.all_docs()) as view:
            self.assert_request_called_with('GET', self.db.name, '_all_docs')
            self.assertIsInstance(view, aiocouchdb.feeds.ViewFeed)

    @utils.run_for('mock')
    def test_all_docs_params(self):
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
            yield from self.db.all_docs(**{key: value})
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            self.assert_request_called_with('GET', self.db.name, '_all_docs',
                                            params={key: value})

    def test_all_docs_key(self):
        with (yield from self.db.all_docs('foo')):
            self.assert_request_called_with('GET', self.db.name, '_all_docs',
                                            params={'key': '"foo"'})

    def test_all_docs_keys(self):
        with (yield from self.db.all_docs('foo', 'bar', 'baz')):
            self.assert_request_called_with(
                'POST', self.db.name, '_all_docs',
                data={'keys': ('foo', 'bar', 'baz')})

    def test_bulk_docs(self):
        yield from self.db.bulk_docs([{'_id': 'foo'}, {'_id': 'bar'}])
        self.assert_request_called_with('POST', self.db.name, '_bulk_docs',
                                        data=Ellipsis)
        data = self.request.call_args[1]['data']
        self.assertIsInstance(data, types.GeneratorType)
        if self._test_target == 'mock':
            # while aiohttp.request is mocked, the payload generator
            # doesn't get used so we can check the real payload data.
            self.assertEqual(b'{"docs": [{"_id": "foo"},{"_id": "bar"}]}',
                             b''.join(data))

    def test_bulk_docs_all_or_nothing(self):
        yield from self.db.bulk_docs([{'_id': 'foo'}, {'_id': 'bar'}],
                                     all_or_nothing=True)
        self.assert_request_called_with('POST', self.db.name, '_bulk_docs',
                                        data=Ellipsis)
        data = self.request.call_args[1]['data']
        self.assertIsInstance(data, types.GeneratorType)
        if self._test_target == 'mock':
            # while aiohttp.request is mocked, the payload generator
            # doesn't get used so we can check the real payload data.
            self.assertEqual(b'{"all_or_nothing": true, "docs": '
                             b'[{"_id": "foo"},{"_id": "bar"}]}',
                             b''.join(data))

    def test_bulk_docs_new_edits(self):
        yield from self.db.bulk_docs([{'_rev': '1-foo'}], new_edits=False)
        self.assert_request_called_with('POST', self.db.name, '_bulk_docs',
                                        data=Ellipsis,
                                        params={})

    def test_changes(self):
        with (yield from self.db.changes()) as feed:
            self.assertIsInstance(feed, aiocouchdb.feeds.ChangesFeed)
            self.assert_request_called_with('GET', self.db.name, '_changes')

    @utils.skip_for('mock')
    def test_changes_reading(self):
        ids = [utils.uuid() for _ in range(3)]

        for idx in ids:
            yield from self.db[idx].update({})

        with (yield from self.db.changes()) as feed:

            while True:
                self.assertTrue(feed.is_active())
                event = yield from feed.next()
                if event is None:
                    break
                self.assertIsInstance(event, dict)
                self.assertIn(event['id'], ids)

            self.assertFalse(feed.is_active())

    def test_changes_longpoll(self):
        with (yield from self.db.changes(feed='longpoll')) as feed:
            self.assertIsInstance(feed, aiocouchdb.feeds.LongPollChangesFeed)
            self.assert_request_called_with('GET', self.db.name, '_changes',
                                            params={'feed': 'longpoll'})

    def test_changes_continuous(self):
        with (yield from self.db.changes(feed='continuous')) as feed:
            self.assertIsInstance(feed, aiocouchdb.feeds.ContinuousChangesFeed)
            self.assert_request_called_with('GET', self.db.name, '_changes',
                                            params={'feed': 'continuous'})

    @utils.skip_for('mock')
    def test_changes_continuous_reading(self):
        ids = [utils.uuid() for _ in range(3)]

        @asyncio.coroutine
        def task():
            for idx in ids:
                yield from self.db[idx].update({})
        asyncio.Task(task())

        with (yield from self.db.changes(feed='continuous',
                                         timeout=1000)) as feed:

            while True:
                self.assertTrue(feed.is_active())
                event = yield from feed.next()
                if event is None:
                    break
                self.assertIsInstance(event, dict)
                self.assertIn(event['id'], ids)

            self.assertFalse(feed.is_active())

    def test_changes_eventsource(self):
        with (yield from self.db.changes(feed='eventsource')) as feed:
            self.assertIsInstance(feed, aiocouchdb.feeds.EventSourceChangesFeed)
            self.assert_request_called_with('GET', self.db.name, '_changes',
                                            params={'feed': 'eventsource'})

    @utils.skip_for('mock')
    def test_changes_eventsource(self):
        ids = [utils.uuid() for _ in range(3)]

        @asyncio.coroutine
        def task():
            for idx in ids:
                yield from self.db[idx].update({})
        asyncio.Task(task())

        with (yield from self.db.changes(feed='eventsource',
                                         timeout=1000)) as feed:

            while True:
                self.assertTrue(feed.is_active())
                event = yield from feed.next()
                if event is None:
                    break
                self.assertIsInstance(event, dict)
                self.assertIn(event['id'], ids)

    def test_changes_doc_ids(self):
        with (yield from self.db.changes('foo', 'bar')):
            self.assert_request_called_with('POST', self.db.name, '_changes',
                                            data={'doc_ids': ('foo', 'bar')},
                                            params={'filter': '_doc_ids'})

    def test_changes_assert_filter_doc_ids(self):
        with self.assertRaises(AssertionError):
            yield from self.db.changes('foo', 'bar', filter='somefilter')

    @utils.skip_for('mock')
    def test_changes_filter_docid(self):
        ids = [utils.uuid() for _ in range(100)]
        filtered_ids = [random.choice(ids) for _ in range(10)]

        yield from self.db.bulk_docs({'_id': idx} for idx in ids)

        with (yield from self.db.changes(*filtered_ids)) as feed:

            while True:
                event = yield from feed.next()
                if event is None:
                    break
                self.assertIn(event['id'], filtered_ids)

            event = yield from feed.next()
            self.assertIsNone(event)
            self.assertFalse(feed.is_active())

    @utils.skip_for('mock')
    def test_changes_filter_view(self):
        docs = yield from utils.populate_database(self.db, 10)
        expected = [doc['_id'] for doc in docs.values() if doc['num'] > 5]
        ddoc = self.db['_design/' + utils.uuid()]

        yield from ddoc.doc.update({
            'views': {
                'test': {
                    'map': 'function(doc){ if(doc.num > 5) emit(doc._id) }'
                }
            }
        })

        view_name = '/'.join([ddoc.name, 'test'])
        with (yield from self.db.changes(view=view_name)) as feed:

            while True:
                event = yield from feed.next()
                if event is None:
                    break
                self.assertIn(event['id'], expected)

            event = yield from feed.next()
            self.assertIsNone(event)
            self.assertFalse(feed.is_active())

    @utils.run_for('mock')
    def test_changes_params(self):
        all_params = {
            'att_encoding_info': False,
            'attachments': True,
            'conflicts': True,
            'descending': True,
            'feed': 'continuous',
            'filter': 'some/filter',
            'headers': {'X-Foo': 'bar'},
            'heartbeat': 1000,
            'include_docs': True,
            'limit': 20,
            'params': {'test': 'passed'},
            'since': 'now',
            'style': 'all_docs',
            'timeout': 3000,
            'view': 'some/view'
        }

        for key, value in all_params.items():
            yield from self.db.changes(**{key: value})
            headers = {}
            if key == 'params':
                params = value
            elif key == 'headers':
                headers = value
                params = {}
            else:
                params = {key: value}
                if key == 'view':
                    params['filter'] = '_view'
            self.assert_request_called_with('GET', self.db.name, '_changes',
                                            headers=headers, params=params)

    def test_compact(self):
        yield from self.db.compact()
        self.assert_request_called_with('POST', self.db.name, '_compact')

    def test_compact_ddoc(self):
        resp = yield from self.db.resource.put('_design/ddoc', data={})
        resp.close()

        yield from self.db.compact('ddoc')
        self.assert_request_called_with(
            'POST', self.db.name, '_compact', 'ddoc')

    def test_document(self):
        result = yield from self.db.doc('docid')
        self.assert_request_called_with('HEAD', self.db.name, 'docid')
        self.assertIsInstance(result, self.db.document_class)

    def test_document_custom_class(self):
        class CustomDocument(object):
            def __init__(self, thing, **kwargs):
                self.resource = thing
        db = aiocouchdb.v1.database.Database(
            self.url_db,
            document_class=CustomDocument)

        result = yield from db.doc('docid')
        self.assert_request_called_with('HEAD', self.db.name, 'docid')
        self.assertIsInstance(result, CustomDocument)
        self.assertIsInstance(result.resource, aiocouchdb.client.Resource)

    def test_document_docid_gen_fun(self):
        def custom_id():
            return 'foo'

        result = yield from self.db.doc(idfun=custom_id)
        self.assert_request_called_with('HEAD', self.db.name, 'foo')
        self.assertIsInstance(result, self.db.document_class)

    def test_document_docid_gen_fun_default_uuid(self):
        result = yield from self.db.doc()
        call_args, _ = self.request.call_args
        docid = call_args[-1].rsplit('/', 1)[-1]
        self.assertRegex(docid, '[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}')
        self.assert_request_called_with('HEAD', self.db.name, docid)
        self.assertIsInstance(result, self.db.document_class)

    def test_design_document(self):
        result = yield from self.db.ddoc('ddoc')
        self.assert_request_called_with('HEAD', self.db.name, '_design', 'ddoc')
        self.assertIsInstance(result, self.db.design_document_class)

    def test_design_document_custom_class(self):
        class CustomDocument(object):
            def __init__(self, thing, **kwargs):
                self.resource = thing
        db = aiocouchdb.v1.database.Database(
            self.url_db,
            design_document_class=CustomDocument)

        result = yield from db.ddoc('_design/ddoc')
        self.assert_request_called_with('HEAD', self.db.name, '_design', 'ddoc')
        self.assertIsInstance(result, CustomDocument)
        self.assertIsInstance(result.resource, aiocouchdb.client.Resource)

    def test_document_get_item(self):
        doc = self.db['docid']
        with self.assertRaises(AssertionError):
            self.assert_request_called_with('HEAD', self.db.name, 'docid')
        self.assertIsInstance(doc, self.db.document_class)

    def test_design_document_get_item(self):
        doc = self.db['_design/ddoc']
        with self.assertRaises(AssertionError):
            self.assert_request_called_with(
                'HEAD', self.db.name, '_design', 'ddoc')
        self.assertIsInstance(doc, self.db.design_document_class)

    def test_ensure_full_commit(self):
        yield from self.db.ensure_full_commit()
        self.assert_request_called_with(
            'POST', self.db.name, '_ensure_full_commit')

    def test_missing_revs(self):
        yield from self.db.missing_revs({'docid': ['1-rev', '2-rev']})
        self.assert_request_called_with('POST', self.db.name, '_missing_revs',
                                        data={'docid': ['1-rev', '2-rev']})

    def test_purge(self):
        yield from self.db.purge({'docid': ['1-rev', '2-rev']})
        self.assert_request_called_with('POST', self.db.name, '_purge',
                                        data={'docid': ['1-rev', '2-rev']})

    def test_revs_diff(self):
        yield from self.db.revs_diff({'docid': ['1-rev', '2-rev']})
        self.assert_request_called_with('POST', self.db.name, '_revs_diff',
                                        data={'docid': ['1-rev', '2-rev']})

    def test_revs_limit(self):
        yield from self.db.revs_limit()
        self.assert_request_called_with('GET', self.db.name, '_revs_limit')

    def test_revs_limit_update(self):
        yield from self.db.revs_limit(42)
        self.assert_request_called_with('PUT', self.db.name, '_revs_limit',
                                        data=42)

    def test_security(self):
        self.assertIsInstance(self.db.security,
                              aiocouchdb.v1.security.DatabaseSecurity)

    def test_security_custom_class(self):
        class CustomSecurity(object):
            def __init__(self, thing):
                self.resource = thing
        db = aiocouchdb.v1.database.Database(self.url_db,
                                             security_class=CustomSecurity)
        self.assertIsInstance(db.security, CustomSecurity)

    def test_temp_view(self):
        mapfun = 'function(doc){ emit(doc._id); }'
        with (yield from self.db.temp_view(mapfun)) as view:
            self.assert_request_called_with('POST', self.db.name, '_temp_view',
                                            data={'map': mapfun})
            self.assertIsInstance(view, aiocouchdb.feeds.ViewFeed)

    def test_temp_view_reduce(self):
        mapfun = 'function(doc){ emit(doc._id); }'
        redfun = '_count'
        with (yield from self.db.temp_view(mapfun, redfun)) as view:
            self.assert_request_called_with('POST', self.db.name, '_temp_view',
                                            data={'map': mapfun,
                                                  'reduce': redfun})
            self.assertIsInstance(view, aiocouchdb.feeds.ViewFeed)

    def test_temp_view_language(self):
        mapfun = 'function(doc){ emit(doc._id); }'
        with (yield from self.db.temp_view(mapfun, language='javascript')):
            self.assert_request_called_with('POST', self.db.name, '_temp_view',
                                            data={'map': mapfun,
                                                  'language': 'javascript'})

    def test_temp_view_startkey_none(self):
        mapfun = 'function(doc){ emit(doc._id); }'

        with (yield from self.db.temp_view(mapfun, startkey=None)):
            self.assert_request_called_with('POST', self.db.name, '_temp_view',
                                            data={'map': mapfun},
                                            params={'startkey': 'null'})

    def test_temp_view_endkey_none(self):
        mapfun = 'function(doc){ emit(doc._id); }'

        with (yield from self.db.temp_view(mapfun, endkey=None)):
            self.assert_request_called_with('POST', self.db.name, '_temp_view',
                                            data={'map': mapfun},
                                            params={'endkey': 'null'})

    @utils.run_for('mock')
    def test_temp_view_params(self):
        all_params = {
            'att_encoding_info': False,
            'attachments': False,
            'conflicts': True,
            'descending': True,
            'endkey': 'foo',
            'endkey_docid': 'foo_id',
            'group': False,
            'group_level': 10,
            'include_docs': True,
            'inclusive_end': False,
            'keys': ['foo', 'bar'],
            'limit': 10,
            'reduce': True,
            'skip': 20,
            'stale': 'ok',
            'startkey': 'bar',
            'startkey_docid': 'bar_id',
            'update_seq': True
        }

        for key, value in all_params.items():
            yield from self.db.temp_view('fun(_)-> ok end', **{key: value})
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            if key == 'keys':
                self.assert_request_called_with(
                    'POST', self.db.name, '_temp_view',
                    data={'map': 'fun(_)-> ok end',
                          key: value})
            else:
                self.assert_request_called_with(
                    'POST', self.db.name, '_temp_view',
                    data={'map': 'fun(_)-> ok end'},
                    params={key: value})

    def test_view_cleanup(self):
        yield from self.db.view_cleanup()
        self.assert_request_called_with('POST', self.db.name, '_view_cleanup')
