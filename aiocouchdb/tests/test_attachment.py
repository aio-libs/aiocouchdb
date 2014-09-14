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
import io

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

    def test_accepts_range(self):
        resp = self.mock_json_response()
        resp.headers['ACCEPT_RANGE'] = 'bytes'
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.att.accepts_range())
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att')
        self.assertTrue(result)

    def test_accepts_range_not(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.att.accepts_range())
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att')
        self.assertFalse(result)

    def test_accepts_range_with_rev(self):
        self.request.return_value = self.future(self.mock_json_response())

        result = self.run_loop(self.att.accepts_range(rev='1-ABC'))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'att',
                                        params={'rev': '1-ABC'})
        self.assertFalse(result)

    def test_get(self):
        self.request.return_value = self.future(self.mock_response())

        result = self.run_loop(self.att.get())
        self.assert_request_called_with('GET', 'db', 'docid', 'att')
        self.assertIsInstance(result, aiocouchdb.attachment.AttachmentReader)

    def test_get_rev(self):
        self.request.return_value = self.future(self.mock_response())

        result = self.run_loop(self.att.get('1-ABC'))
        self.assert_request_called_with('GET', 'db', 'docid', 'att',
                                        params={'rev': '1-ABC'})
        self.assertIsInstance(result, aiocouchdb.attachment.AttachmentReader)

    def test_get_range(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.get(range=slice(24, 42)))
        self.assert_request_called_with('GET', 'db', 'docid', 'att',
                                        headers={'RANGE': 'bytes=24-42'})

    def test_get_range_from_start(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.get(range=slice(42)))
        self.assert_request_called_with('GET', 'db', 'docid', 'att',
                                        headers={'RANGE': 'bytes=0-42'})

    def test_get_range_iterable(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.get(range=[11, 22]))
        self.assert_request_called_with('GET', 'db', 'docid', 'att',
                                        headers={'RANGE': 'bytes=11-22'})

    def test_get_range_int(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.get(range=42))
        self.assert_request_called_with('GET', 'db', 'docid', 'att',
                                        headers={'RANGE': 'bytes=0-42'})

    def test_update(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.update(io.BytesIO(b'')))
        self.assert_request_called_with(
            'PUT', 'db', 'docid', 'att',
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'application/octet-stream'})

    def test_update_ctype(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.update(io.BytesIO(b''), content_type='foo/bar'))
        self.assert_request_called_with(
            'PUT', 'db', 'docid', 'att',
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'foo/bar'})

    def test_update_rev(self):
        self.request.return_value = self.future(self.mock_response())

        self.run_loop(self.att.update(io.BytesIO(b''), rev='1-ABC'))
        self.assert_request_called_with(
            'PUT', 'db', 'docid', 'att',
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'application/octet-stream'},
            params={'rev': '1-ABC'})


class AttachmentReaderTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.att = aiocouchdb.attachment.AttachmentReader(self.request)

    def test_close(self):
        self.request.content.at_eof.return_value = False
        self.att.close()
        self.assertTrue(self.request.close.called)

    def test_closed(self):
        _ = self.att.closed
        self.assertTrue(self.request.content.at_eof.called)

    def test_close_when_closed(self):
        self.request.content.at_eof.return_value = True
        self.att.close()
        self.assertFalse(self.request.close.called)

    def test_readable(self):
        self.assertTrue(self.att.readable())

    def test_writable(self):
        self.assertFalse(self.att.writable())

    def test_seekable(self):
        self.assertFalse(self.att.seekable())

    def test_read(self):
        self.run_loop(self.att.read())
        self.request.content.read.assert_called_once_with(None)

    def test_read_some(self):
        self.run_loop(self.att.read(10))
        self.request.content.read.assert_called_once_with(10)

    def test_readall(self):
        resp = self.mock_response(data=[b'...', b'---'])
        self.att._resp = resp

        res = self.run_loop(self.att.readall())

        resp.content.read.assert_called_with(8192)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertIsInstance(res, bytearray)

    def test_readline(self):
        self.run_loop(self.att.readline())
        self.request.content.readline.assert_called_once_with()

    def test_readlines(self):
        resp = self.mock_response(data=[b'...', b'---'])
        resp.content.readline = resp.content.read
        self.att._resp = resp

        res = self.run_loop(self.att.readlines())

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertEqual(res, [b'...', b'---'])

    def test_readlines_hint(self):
        resp = self.mock_response(data=[b'...', b'---'])
        resp.content.readline = resp.content.read
        self.att._resp = resp

        res = self.run_loop(self.att.readlines(2))

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 1)
        self.assertEqual(res, [b'...'])

    def test_readlines_hint_more(self):
        resp = self.mock_response(data=[b'...', b'---'])
        resp.content.readline = resp.content.read
        self.att._resp = resp

        res = self.run_loop(self.att.readlines(42))

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertEqual(res, [b'...', b'---'])
