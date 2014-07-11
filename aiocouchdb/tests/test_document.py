# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.client
import aiocouchdb.feeds
import aiocouchdb.document
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class DatabaseTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_doc = urljoin(self.url, 'db', 'docid')
        self.doc = aiocouchdb.document.Document(self.url_doc)

    def test_init_with_url(self):
        self.assertIsInstance(self.doc.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_doc)
        doc = aiocouchdb.document.Document(res)
        self.assertIsInstance(doc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_doc, self.doc.resource.url)

    def test_exists(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid')
        self.assertTrue(result)

    def test_exists_rev(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid',
                                        params={'rev': '1-ABC'})
        self.assertTrue(result)

    def test_exists_forbidden(self):
        resp = self.mock_json_response()
        resp.status = 403
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid')
        self.assertFalse(result)

    def test_exists_not_found(self):
        resp = self.mock_json_response()
        resp.status = 404
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid')
        self.assertFalse(result)

    def test_modified(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.modified('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid',
                                        headers={'IF-NONE-MATCH': '"1-ABC"'})
        self.assertTrue(result)

    def test_not_modified(self):
        resp = self.mock_json_response()
        resp.status = 304
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.modified('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid',
                                        headers={'IF-NONE-MATCH': '"1-ABC"'})
        self.assertFalse(result)
