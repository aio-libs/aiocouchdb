# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json
import io

import aiocouchdb.client
import aiocouchdb.database
import aiocouchdb.document
import aiocouchdb.multipart

from .test_multipart import Stream
from . import utils


class DocumentTestCase(utils.DocumentTestCase):

    def request_path(self, *parts):
        return [self.db.name, self.doc.id] + list(parts)

    def test_init_with_url(self):
        self.assertIsInstance(self.doc.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_doc)
        doc = aiocouchdb.document.Document(res)
        self.assertIsInstance(doc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_doc, doc.resource.url)

    def test_init_with_id(self):
        res = aiocouchdb.client.Resource(self.url_doc)
        doc = aiocouchdb.document.Document(res, docid='foo')
        self.assertEqual(doc.id, 'foo')

    def test_init_with_id_from_database(self):
        db = aiocouchdb.database.Database(self.url)
        doc = yield from db.doc('foo')
        self.assertEqual(doc.id, 'foo')

    def test_exists(self):
        result = yield from self.doc.exists()
        self.assert_request_called_with('HEAD', *self.request_path())
        self.assertTrue(result)

    def test_exists_rev(self):
        result = yield from self.doc.exists(self.rev)
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        params={'rev': self.rev})
        self.assertTrue(result)

    @utils.run_for('mock')
    def test_exists_forbidden(self):
        # CouchDB doesn't supports per-document access control unless for
        # authentication and replicator databases.
        # We'll test this in real in another suites
        with self.response(status=403):
            result = yield from self.doc.exists()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertFalse(result)

    def test_exists_not_found(self):
        docid = utils.uuid()
        with self.response(status=404):
            result = yield from self.db[docid].exists()
            self.assert_request_called_with('HEAD', self.db.name, docid)
        self.assertFalse(result)

    def test_modified(self):
        result = yield from self.doc.modified('1-ABC')
        self.assert_request_called_with('HEAD', *self.request_path(),
                                        headers={'IF-NONE-MATCH': '"1-ABC"'})
        self.assertTrue(result)

    def test_not_modified(self):
        with self.response(status=304):
            result = yield from self.doc.modified(self.rev)
            self.assert_request_called_with(
                'HEAD', *self.request_path(),
                headers={'IF-NONE-MATCH': '"%s"' % self.rev})
        self.assertFalse(result)

    def test_attachment(self):
        result = yield from self.doc.att('attname')
        self.assert_request_called_with('HEAD', *self.request_path('attname'))
        self.assertIsInstance(result, self.doc.attachment_class)

    def test_attachment_custom_class(self):
        class CustomAttachment(object):
            def __init__(self, thing, **kwargs):
                self.resource = thing

        doc = aiocouchdb.document.Document(self.url_doc,
                                           attachment_class=CustomAttachment)

        result = yield from doc.att('attname')
        self.assert_request_called_with('HEAD', *self.request_path('attname'))
        self.assertIsInstance(result, CustomAttachment)
        self.assertIsInstance(result.resource, aiocouchdb.client.Resource)

    def test_attachment_get_item(self):
        att = self.doc['attname']
        with self.assertRaises(AssertionError):
            self.assert_request_called_with('HEAD',
                                            *self.request_path('attname'))
        self.assertIsInstance(att, self.doc.attachment_class)

    def test_rev(self):
        with self.response(headers={'ETAG': '"%s"' % self.rev}):
            result = yield from self.doc.rev()
            self.assert_request_called_with('HEAD', *self.request_path())
        self.assertEqual(self.rev, result)

    def test_get(self):
        yield from self.doc.get()
        self.assert_request_called_with('GET', *self.request_path())

    def test_get_rev(self):
        yield from self.doc.get(self.rev)
        self.assert_request_called_with('GET', *self.request_path(),
                                        params={'rev': self.rev})

    def test_get_params(self):
        all_params = {
            'att_encoding_info': True,
            'attachments': True,
            'atts_since': [self.rev],
            'conflicts': False,
            'deleted_conflicts': True,
            'local_seq': True,
            'meta': False,
            'open_revs': [self.rev, '2-CDE'],
            'rev': self.rev,
            'revs': True,
            'revs_info': True
        }

        for key, value in all_params.items():
            yield from self.doc.get(**{key: value})
            if key in ('atts_since', 'open_revs'):
                value = json.dumps(value)
            self.assert_request_called_with('GET', *self.request_path(),
                                            params={key: value})

    def test_get_open_revs(self):
        with self.response(headers={
            'CONTENT-TYPE': 'multipart/mixed;boundary=:'
        }):
            result = yield from self.doc.get_open_revs()
            self.assert_request_called_with('GET', *self.request_path(),
                                            headers={'ACCEPT': 'multipart/*'},
                                            params={'open_revs': 'all'})
        self.assertIsInstance(
            result,
            aiocouchdb.document.OpenRevsMultipartReader.response_wrapper_cls)
        self.assertIsInstance(
            result.stream,
            aiocouchdb.document.OpenRevsMultipartReader)

    def test_get_open_revs_list(self):
        with self.response(headers={
            'CONTENT-TYPE': 'multipart/mixed;boundary=:'
        }):
            yield from self.doc.get_open_revs('1-ABC', '2-CDE')
            self.assert_request_called_with(
                'GET', *self.request_path(),
                headers={'ACCEPT': 'multipart/*'},
                params={'open_revs': '["1-ABC", "2-CDE"]'})

    def test_get_open_revs_params(self):
        all_params = {
            'att_encoding_info': True,
            'atts_since': ['1-ABC'],
            'local_seq': True,
            'revs': True
        }

        for key, value in all_params.items():
            with self.response(headers={
                'CONTENT-TYPE': 'multipart/mixed;boundary=:'
            }):
                yield from self.doc.get_open_revs(**{key: value})

                if key == 'atts_since':
                    value = json.dumps(value)

                self.assert_request_called_with(
                    'GET', *self.request_path(),
                    headers={'ACCEPT': 'multipart/*'},
                    params={key: value,
                            'open_revs': 'all'})

    def test_get_with_atts(self):
        with self.response(
            headers={'CONTENT-TYPE': 'multipart/related;boundary=:'}
        ):
            result = yield from self.doc.get_with_atts()
            self.assert_request_called_with(
                'GET', *self.request_path(),
                headers={'ACCEPT': 'multipart/*, application/json'},
                params={'attachments': True})
        self.assertIsInstance(
            result,
            aiocouchdb.document.DocAttachmentsMultipartReader.response_wrapper_cls)
        self.assertIsInstance(
            result.stream,
            aiocouchdb.document.DocAttachmentsMultipartReader)

    def test_get_wth_atts_json(self):
        with self.response(headers={
            'CONTENT-TYPE': 'application/json'
        }):
            result = yield from self.doc.get_with_atts()
            self.assert_request_called_with(
                'GET', *self.request_path(),
                headers={'ACCEPT': 'multipart/*, application/json'},
                params={'attachments': True})
        self.assertIsInstance(
            result,
            aiocouchdb.document.DocAttachmentsMultipartReader.response_wrapper_cls)
        self.assertIsInstance(
            result.stream,
            aiocouchdb.document.DocAttachmentsMultipartReader)

    def test_get_wth_atts_json_hacks(self):
        jsondoc = json.dumps({'_id': self.doc.id, '_rev': self.rev},
                             sort_keys=True).replace(' ', '').encode()

        with self.response(
            data=jsondoc,
            headers={'CONTENT-TYPE': 'application/json'}
        ):
            result = yield from self.doc.get_with_atts()
            self.assert_request_called_with(
                'GET', *self.request_path(),
                headers={'ACCEPT': 'multipart/*, application/json'},
                params={'attachments': True})

        resp = result.resp
        self.assertTrue(
            resp.headers['CONTENT-TYPE'].startswith('multipart/related'))

        head, *body, tail = resp.content._buffer.splitlines()
        self.assertTrue(tail.startswith(head))
        self.assertEqual(
            b'Content-Type: application/json\r\n\r\n' + jsondoc,
            b'\r\n'.join(body))

    def test_get_with_atts_params(self):
        all_params = {
            'att_encoding_info': True,
            'atts_since': [self.rev],
            'conflicts': False,
            'deleted_conflicts': True,
            'local_seq': True,
            'meta': False,
            'rev': self.rev,
            'revs': True,
            'revs_info': True
        }

        for key, value in all_params.items():
            with self.response(headers={
                'CONTENT-TYPE': 'multipart/related;boundary=:'
            }):
                yield from self.doc.get_with_atts(**{key: value})

                if key == 'atts_since':
                    value = json.dumps(value)

                self.assert_request_called_with(
                    'GET', *self.request_path(),
                    headers={'ACCEPT': 'multipart/*, application/json'},
                    params={key: value, 'attachments': True})

    def test_update(self):
        yield from self.doc.update({}, rev=self.rev)
        self.assert_request_called_with('PUT', *self.request_path(),
                                        data={},
                                        params={'rev': self.rev})

    @utils.run_for('mock')
    def test_update_params(self):
        all_params = {
            'batch': "ok",
            'new_edits': True,
            'rev': '1-ABC'
        }

        for key, value in all_params.items():
            yield from self.doc.update({}, **{key: value})
            self.assert_request_called_with('PUT', *self.request_path(),
                                            data={},
                                            params={key: value})

    def test_update_expect_mapping(self):
        with self.assertRaises(TypeError):
            yield from self.doc.update([])

        class Foo(dict):
            pass

        doc = Foo()
        yield from self.doc.update(doc, rev=self.rev)
        self.assert_request_called_with('PUT', *self.request_path(),
                                        data={},
                                        params={'rev': self.rev})

    def test_update_reject_docid_collision(self):
        with self.assertRaises(ValueError):
            yield from self.doc.update({'_id': 'foo'})

    def test_update_with_atts(self):
        foo = io.BytesIO(b'foo')
        bar = b'bar'
        baz = open(__file__, 'rb')

        with self.response():
            yield from self.doc.update(
                {}, atts={'foo': foo,
                          'bar': bar,
                          'baz': baz}, rev=self.rev)
            self.assert_request_called_with(
                'PUT', *self.request_path(),
                data=...,
                headers=...,
                params={'rev': self.rev})

        with self.response():
            self.assertTrue((yield from self.doc['foo'].exists()))
        with self.response():
            self.assertTrue((yield from self.doc['bar'].exists()))
        with self.response():
            self.assertTrue((yield from self.doc['baz'].exists()))


    def test_delete(self):
        yield from self.doc.delete(self.rev)
        self.assert_request_called_with('DELETE', *self.request_path(),
                                        params={'rev': self.rev})

    def test_delete_preserve_content(self):
        with self.response(data=b'{"rev": "2-CDE"}'):
            resp = yield from self.doc.update({'foo': 'bar'}, rev=self.rev)

        rev = resp['rev']
        data = json.dumps({'_id': self.doc.id,
                           '_rev': rev,
                           'foo': 'bar'}).encode()
        with self.response(data=data):
            yield from self.doc.delete(rev, preserve_content=True)
            self.assert_request_called_with('PUT', *self.request_path(),
                                            data={'_id': self.doc.id,
                                                  '_rev': rev,
                                                  '_deleted': True,
                                                  'foo': 'bar'},
                                            params={'rev': rev})

    def test_copy(self):
        newid = utils.uuid()
        yield from self.doc.copy(newid)
        self.assert_request_called_with('COPY', *self.request_path(),
                                        headers={'DESTINATION': newid})

    def test_copy_rev(self):
        yield from self.doc.copy('idx', '1-A')
        self.assert_request_called_with('COPY', *self.request_path(),
                                        headers={'DESTINATION': 'idx?rev=1-A'})


class OpenRevsMultipartReader(utils.TestCase):

    def test_next(self):
        reader = aiocouchdb.document.OpenRevsMultipartReader(
            {'CONTENT-TYPE': 'multipart/mixed;boundary=:'},
            Stream(b'--:\r\n'
                   b'Content-Type: multipart/related;boundary=--:--\r\n'
                   b'\r\n'
                   b'----:--\r\n'
                   b'Content-Type: application/json\r\n'
                   b'\r\n'
                   b'{"_id": "foo"}\r\n'
                   b'----:--\r\n'
                   b'Content-Disposition: attachment; filename="att.txt"\r\n'
                   b'Content-Type: text/plain\r\n'
                   b'Content-Length: 9\r\n'
                   b'\r\n'
                   b'some data\r\n'
                   b'----:----\r\n'
                   b'--:--'))
        result = yield from reader.next()

        self.assertIsInstance(result, tuple)
        self.assertEqual(2, len(result))

        doc, subreader = result

        self.assertEqual({'_id': 'foo'}, doc)
        self.assertIsInstance(subreader, reader.multipart_reader_cls)

        partreader = yield from subreader.next()
        self.assertIsInstance(partreader, subreader.part_reader_cls)

        data = yield from partreader.next()
        self.assertEqual(b'some data', data)

        next_data = yield from partreader.next()
        self.assertIsNone(next_data)
        self.assertTrue(partreader.at_eof())

        next_data = yield from subreader.next()
        self.assertIsNone(next_data)
        self.assertTrue(subreader.at_eof())

        next_data = yield from reader.next()
        self.assertEqual((None, None), next_data)
        self.assertTrue(reader.at_eof())

    def test_next_only_doc(self):
        reader = aiocouchdb.document.OpenRevsMultipartReader(
            {'CONTENT-TYPE': 'multipart/mixed;boundary=:'},
            Stream(b'--:\r\n'
                   b'Content-Type: application/json\r\n'
                   b'\r\n'
                   b'{"_id": "foo"}\r\n'
                   b'--:--'))
        result = yield from reader.next()

        self.assertIsInstance(result, tuple)
        self.assertEqual(2, len(result))

        doc, subreader = result

        self.assertEqual({'_id': 'foo'}, doc)
        self.assertIsInstance(subreader, reader.part_reader_cls)

        next_data = yield from subreader.next()
        self.assertIsNone(next_data)
        self.assertTrue(subreader.at_eof())

        next_data = yield from reader.next()
        self.assertEqual((None, None), next_data)
        self.assertTrue(reader.at_eof())
