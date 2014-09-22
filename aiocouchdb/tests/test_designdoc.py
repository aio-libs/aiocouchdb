# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json
import aiocouchdb.client
import aiocouchdb.document
import aiocouchdb.designdoc
import aiocouchdb.feeds
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class DesignDocTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_ddoc = urljoin(self.url, 'db', '_design', 'ddoc')
        self.ddoc = aiocouchdb.designdoc.DesignDocument(self.url_ddoc)

    def test_init_with_url(self):
        self.assertIsInstance(self.ddoc.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_ddoc)
        ddoc = aiocouchdb.designdoc.DesignDocument(res)
        self.assertIsInstance(ddoc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_ddoc, ddoc.resource.url)

    def test_access_to_document_api(self):
        self.assertIsInstance(self.ddoc.doc, aiocouchdb.document.Document)
        self.assertIsInstance(self.ddoc.document, aiocouchdb.document.Document)

    def test_access_to_custom_document_api(self):
        class CustomDoc(object):
            def __init__(self, resource):
                pass
        ddoc = aiocouchdb.designdoc.DesignDocument('', document_class=CustomDoc)
        self.assertIsInstance(ddoc.doc, CustomDoc)
        self.assertIsInstance(ddoc.document, CustomDoc)

    def test_info(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.ddoc.info())
        self.assert_request_called_with('GET', 'db', '_design', 'ddoc', '_info')
        self.assertIsInstance(result, dict)

    def test_view(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.view('viewname'))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_view', 'viewname')
        self.assertIsInstance(result, aiocouchdb.feeds.ViewFeed)

    def test_view_key(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.ddoc.view('viewname', 'foo'))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_view', 'viewname',
            params={'key': '"foo"'})
        self.assertIsInstance(result, aiocouchdb.feeds.ViewFeed)

    def test_view_keys(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.ddoc.view('viewname', 'foo', 'bar'))
        self.assert_request_called_with(
            'POST', 'db', '_design', 'ddoc', '_view', 'viewname',
            data={'keys': ('foo', 'bar')})
        self.assertIsInstance(result, aiocouchdb.feeds.ViewFeed)

    def test_view_params(self):
        self.request.return_value = self.future(self.mock_json_response())

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
            'limit': 10,
            'reduce': True,
            'skip': 20,
            'stale': 'ok',
            'startkey': 'bar',
            'startkey_docid': 'bar_id',
            'update_seq': True
        }

        for key, value in all_params.items():
            result = self.run_loop(self.ddoc.view('viewname', **{key: value}))
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            self.assert_request_called_with(
                'GET', 'db', '_design', 'ddoc', '_view', 'viewname',
                params={key: value})
            self.assertIsInstance(result, aiocouchdb.feeds.ViewFeed)

    def test_list(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.list('listname'))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_list', 'listname')
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_view(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.list('listname', 'viewname'))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_list', 'listname', 'viewname')
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_view_ddoc(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.list('listname', 'ddoc/view'))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_list', 'listname', 'ddoc', 'view')
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_params(self):
        self.request.return_value = self.future(self.mock_json_response())

        all_params = {
            'att_encoding_info': False,
            'attachments': False,
            'conflicts': True,
            'descending': True,
            'endkey': 'foo',
            'endkey_docid': 'foo_id',
            'format': 'json',
            'group': False,
            'group_level': 10,
            'include_docs': True,
            'inclusive_end': False,
            'limit': 10,
            'reduce': True,
            'skip': 20,
            'stale': 'ok',
            'startkey': 'bar',
            'startkey_docid': 'bar_id',
            'update_seq': True
        }

        for key, value in all_params.items():
            result = self.run_loop(self.ddoc.list('listname', 'viewname',
                                                  **{key: value}))
            if key in ('endkey', 'startkey'):
                value = json.dumps(value)
            self.assert_request_called_with(
                'GET', 'db', '_design', 'ddoc', '_list', 'listname', 'viewname',
                params={key: value})
            self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_custom_method(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.list('listname', method='POST'))
        self.assert_request_called_with(
            'POST', 'db', '_design', 'ddoc', '_list', 'listname')
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_custom_headers(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.list('listname', headers={'Foo': '1'}))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_list', 'listname',
            headers={'Foo': '1'})
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_custom_params(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.ddoc.list('listname', params={'foo': '1'}))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_list', 'listname',
            params={'foo': '1'})
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_key(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.ddoc.list('listname', 'viewname', 'foo'))
        self.assert_request_called_with(
            'GET', 'db', '_design', 'ddoc', '_list', 'listname', 'viewname',
            params={'key': '"foo"'})
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)

    def test_list_keys(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.ddoc.list('listname', 'viewname',
                                              'foo', 'bar'))
        self.assert_request_called_with(
            'POST', 'db', '_design', 'ddoc', '_list', 'listname', 'viewname',
            data={'keys': ('foo', 'bar')})
        self.assertIsInstance(result, aiocouchdb.client.HttpResponse)
