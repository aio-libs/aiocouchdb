# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import base64
import hashlib

import aiocouchdb.client
import aiocouchdb.attachment
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class AttachmentTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_doc = urljoin(self.url, 'db', 'docid', 'att')
        self.att = aiocouchdb.attachment.Attachment(self.url_doc)

    def test_init_with_url(self):
        self.assertIsInstance(self.att.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_doc)
        doc = aiocouchdb.attachment.Attachment(res)
        self.assertIsInstance(doc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_doc, self.att.resource.url)

    def test_exists(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.att.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att')
        self.assertTrue(result)

    def test_exists_rev(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.att.exists('1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att',
                                        params={'rev': '1-ABC'})
        self.assertTrue(result)

    def test_exists_forbidden(self):
        resp = self.mock_json_response()
        resp.status = 403
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.att.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att')
        self.assertFalse(result)

    def test_exists_not_found(self):
        resp = self.mock_json_response()
        resp.status = 404
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.att.exists())
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att')
        self.assertFalse(result)

    def test_modified(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        digest = hashlib.md5(b'foo').digest()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'
        result = self.run_loop(self.att.modified(digest))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att',
                                        headers={'IF-NONE-MATCH': reqdigest})
        self.assertTrue(result)

    def test_not_modified(self):
        resp = self.mock_json_response()
        resp.status = 304
        self.request.return_value = self.future(resp)

        digest = hashlib.md5(b'foo').digest()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'
        result = self.run_loop(self.att.modified(digest))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att',
                                        headers={'IF-NONE-MATCH': reqdigest})
        self.assertFalse(result)

    def test_modified_with_base64_digest(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        digest = base64.b64encode(hashlib.md5(b'foo').digest()).decode()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'
        result = self.run_loop(self.att.modified(digest))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att',
                                        headers={'IF-NONE-MATCH': reqdigest})
        self.assertTrue(result)

    def test_modified_invalid_digest(self):
        self.assertRaises(TypeError, self.run_loop, self.att.modified({}))
        self.assertRaises(ValueError, self.run_loop, self.att.modified(b'foo'))
        self.assertRaises(ValueError, self.run_loop, self.att.modified('bar'))
