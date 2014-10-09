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
import aiocouchdb.document

from aiocouchdb.client import urljoin
from . import utils


class AttachmentTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_att = urljoin(self.url, *self.request_path())
        self.att = aiocouchdb.attachment.Attachment(self.url_att)

    def request_path(self, *parts):
        return ['db', 'docid', 'att'] + list(parts)

    def test_init_with_url(self):
        self.assertIsInstance(self.att.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_att)
        att = aiocouchdb.attachment.Attachment(res)
        self.assertIsInstance(att.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_att, att.resource.url)

    def test_init_with_name(self):
        res = aiocouchdb.client.Resource(self.url_att)
        db = aiocouchdb.database.Database(res, dbname='foo')
        self.assertEqual(db.name, 'foo')

    def test_init_with_name_from_doc(self):
        doc = aiocouchdb.document.Document(self.url)
        att = yield from doc.att('bar.txt')
        self.assertEqual(att.name, 'bar.txt')

    def test_exists(self):
        result = yield from self.att.exists()
        self.assert_request_called_with('HEAD', *self.request_path())
        self.assertTrue(result)

    def test_exists_rev(self):
        result = yield from self.att.exists('1-ABC')
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        params={'rev': '1-ABC'})
        self.assertTrue(result)

    def test_exists_forbidden(self):
        with self.response(status=403):
            result = yield from self.att.exists()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertFalse(result)

    def test_exists_not_found(self):
        with self.response(status=404):
            result = yield from self.att.exists()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertFalse(result)

    def test_modified(self):
        digest = hashlib.md5(b'foo').digest()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'
        result = yield from self.att.modified(digest)
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        headers={'IF-NONE-MATCH': reqdigest})
        self.assertTrue(result)

    def test_not_modified(self):
        digest = hashlib.md5(b'foo').digest()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'

        with self.response(status=304):
            result = yield from self.att.modified(digest)
            self.assert_request_called_with(
                'HEAD', *self.request_path(),
                headers={'IF-NONE-MATCH': reqdigest})
        self.assertFalse(result)

    def test_modified_with_base64_digest(self):
        digest = base64.b64encode(hashlib.md5(b'foo').digest()).decode()
        reqdigest = '"rL0Y20zC+Fzt72VPzMSk2A=="'
        result = yield from self.att.modified(digest)
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        headers={'IF-NONE-MATCH': reqdigest})
        self.assertTrue(result)

    def test_modified_invalid_digest(self):
        with self.assertRaises(TypeError):
            yield from self.att.modified({})

        with self.assertRaises(ValueError):
            yield from self.att.modified(b'foo')

        with self.assertRaises(ValueError):
            yield from self.att.modified('bar')

    def test_accepts_range(self):
        with self.response(headers={'ACCEPT_RANGE': 'bytes'}):
            result = yield from self.att.accepts_range()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertTrue(result)

    def test_accepts_range_not(self):
        result = yield from self.att.accepts_range()
        self.assert_request_called_with('HEAD', *self.request_path())
        self.assertFalse(result)

    def test_accepts_range_with_rev(self):
        result = yield from self.att.accepts_range(rev='1-ABC')
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        params={'rev': '1-ABC'})
        self.assertFalse(result)

    def test_get(self):
        result = yield from self.att.get()
        self.assert_request_called_with('GET', *self.request_path())
        self.assertIsInstance(result, aiocouchdb.attachment.AttachmentReader)

    def test_get_rev(self):
        result = yield from self.att.get('1-ABC')
        self.assert_request_called_with('GET', *self.request_path(),
                                        params={'rev': '1-ABC'})
        self.assertIsInstance(result, aiocouchdb.attachment.AttachmentReader)

    def test_get_range(self):
        yield from self.att.get(range=slice(24, 42))
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'RANGE': 'bytes=24-42'})

    def test_get_range_from_start(self):
        yield from self.att.get(range=slice(42))
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'RANGE': 'bytes=0-42'})

    def test_get_range_iterable(self):
        yield from self.att.get(range=[11, 22])
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'RANGE': 'bytes=11-22'})

    def test_get_range_int(self):
        yield from self.att.get(range=42)
        self.assert_request_called_with('GET', *self.request_path(),
                                        headers={'RANGE': 'bytes=0-42'})

    def test_update(self):
        yield from self.att.update(io.BytesIO(b''))
        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'application/octet-stream'})

    def test_update_ctype(self):
        yield from self.att.update(io.BytesIO(b''), content_type='foo/bar')
        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'foo/bar'})

    def test_update_with_encoding(self):
        yield from self.att.update(io.BytesIO(b''), content_encoding='gzip')
        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'application/octet-stream',
                     'CONTENT-ENCODING': 'gzip'})

    def test_update_rev(self):
        yield from self.att.update(io.BytesIO(b''), rev='1-ABC')
        self.assert_request_called_with(
            'PUT', *self.request_path(),
            data=Ellipsis,
            headers={'CONTENT-TYPE': 'application/octet-stream'},
            params={'rev': '1-ABC'})

    def test_delete(self):
        yield from self.att.delete('1-ABC')
        self.assert_request_called_with('DELETE', *self.request_path(),
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
        yield from self.att.read()
        self.request.content.read.assert_called_once_with(-1)

    def test_read_some(self):
        yield from self.att.read(10)
        self.request.content.read.assert_called_once_with(10)

    def test_readall(self):
        with self.response(data=[b'...', b'---']) as resp:
            self.att._resp = resp
            res = yield from self.att.readall()

        resp.content.read.assert_called_with(8192)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertIsInstance(res, bytearray)

    def test_readline(self):
        yield from self.att.readline()
        self.request.content.readline.assert_called_once_with()

    def test_readlines(self):
        with self.response(data=[b'...', b'---']) as resp:
            resp.content.readline = resp.content.read
            self.att._resp = resp
            res = yield from self.att.readlines()

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertEqual(res, [b'...', b'---'])

    def test_readlines_hint(self):
        with self.response(data=[b'...', b'---']) as resp:
            resp.content.readline = resp.content.read
            self.att._resp = resp
            res = yield from self.att.readlines(2)

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 1)
        self.assertEqual(res, [b'...'])

    def test_readlines_hint_more(self):
        with self.response(data=[b'...', b'---']) as resp:
            resp.content.readline = resp.content.read
            self.att._resp = resp
            res = yield from self.att.readlines(42)

        self.assertTrue(resp.content.readline.called)
        self.assertEqual(resp.content.read.call_count, 3)
        self.assertEqual(res, [b'...', b'---'])
