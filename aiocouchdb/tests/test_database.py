# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.client
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
