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
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin
from .test_multipart import Stream


class DocumentTestCase(utils.TestCase):

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
        self.assertEqual(self.url_doc, doc.resource.url)

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

    def test_attachment(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.attachment('attname'))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'attname')
        self.assertIsInstance(result, self.doc.attachment_class)

    def test_attachment_custom_class(self):
        class CustomAttachment(object):
            def __init__(self, thing):
                self.resource = thing
        doc = aiocouchdb.document.Document(self.url_doc,
                                           attachment_class=CustomAttachment)

        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        result = self.run_loop(doc.att('attname'))
        self.assert_request_called_with('HEAD', 'db', 'docid', 'attname')
        self.assertIsInstance(result, CustomAttachment)
        self.assertIsInstance(result.resource, aiocouchdb.client.Resource)

    def test_get(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get())
        self.assert_request_called_with('GET', 'db', 'docid')
        self.assertEqual({}, result)

    def test_get_rev(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get('1-ABC'))
        self.assert_request_called_with('GET', 'db', 'docid',
                                        params={'rev': '1-ABC'})
        self.assertEqual({}, result)

    def test_get_params(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        all_params = {
            'att_encoding_info': True,
            'attachments': True,
            'atts_since': ['1-ABC'],
            'conflicts': False,
            'deleted_conflicts': True,
            'local_seq': True,
            'meta': False,
            'open_revs': ['1-ABC', '2-CDE'],
            'rev': '1-ABC',
            'revs': True,
            'revs_info': True
        }

        for key, value in all_params.items():
            self.run_loop(self.doc.get(**{key: value}))
            if key in ('atts_since', 'open_revs'):
                value = json.dumps(value)
            self.assert_request_called_with('GET', 'db', 'docid',
                                            params={key: value})

    def test_get_open_revs(self):
        resp = self.mock_response(
            headers={'CONTENT-TYPE': 'multipart/mixed;boundary=:'})
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get_open_revs())
        self.assert_request_called_with('GET', 'db', 'docid',
                                        headers={'ACCEPT': 'multipart/*'},
                                        params={'open_revs': 'all'})
        self.assertIsInstance(
            result,
            aiocouchdb.document.OpenRevsMultipartReader.response_wrapper_cls)
        self.assertIsInstance(
            result.stream,
            aiocouchdb.document.OpenRevsMultipartReader)

    def test_get_open_revs_list(self):
        resp = self.mock_response(
            headers={'CONTENT-TYPE': 'multipart/mixed;boundary=:'})
        self.request.return_value = self.future(resp)

        self.run_loop(self.doc.get_open_revs('1-ABC', '2-CDE'))
        self.assert_request_called_with(
            'GET', 'db', 'docid',
            headers={'ACCEPT': 'multipart/*'},
            params={'open_revs': '["1-ABC", "2-CDE"]'})

    def test_get_open_revs_params(self):
        resp = self.mock_response(
            headers={'CONTENT-TYPE': 'multipart/mixed;boundary=:'})
        self.request.return_value = self.future(resp)

        all_params = {
            'att_encoding_info': True,
            'atts_since': ['1-ABC'],
            'local_seq': True,
            'revs': True
        }

        for key, value in all_params.items():
            self.run_loop(self.doc.get_open_revs(**{key: value}))
            if key == 'atts_since':
                value = json.dumps(value)
            self.assert_request_called_with('GET', 'db', 'docid',
                                            headers={'ACCEPT': 'multipart/*'},
                                            params={key: value,
                                                    'open_revs': 'all'})

    def test_get_with_atts(self):
        resp = self.mock_response(
            headers={'CONTENT-TYPE': 'multipart/related;boundary=:'})
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get_with_atts())
        self.assert_request_called_with(
            'GET', 'db', 'docid',
            headers={'ACCEPT': 'multipart/*, application/json'},
            params={'attachments': True})
        self.assertIsInstance(
            result,
            aiocouchdb.document.DocAttachmentsMultipartReader.response_wrapper_cls)
        self.assertIsInstance(
            result.stream,
            aiocouchdb.document.DocAttachmentsMultipartReader)

    def test_get_wth_atts_json(self):
        resp = self.mock_response(headers={'CONTENT-TYPE': 'application/json'})
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get_with_atts())
        self.assert_request_called_with(
            'GET', 'db', 'docid',
            headers={'ACCEPT': 'multipart/*, application/json'},
            params={'attachments': True})
        self.assertIsInstance(
            result,
            aiocouchdb.document.DocAttachmentsMultipartReader.response_wrapper_cls)
        self.assertIsInstance(
            result.stream,
            aiocouchdb.document.DocAttachmentsMultipartReader)

    def test_get_wth_atts_json_hacks(self):
        resp = self.mock_response(
            headers={'CONTENT-TYPE': 'application/json'},
            data=b'{"_id": "foo"}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.get_with_atts())
        self.assert_request_called_with(
            'GET', 'db', 'docid',
            headers={'ACCEPT': 'multipart/*, application/json'},
            params={'attachments': True})

        resp = result.resp
        self.assertTrue(
            resp.headers['CONTENT-TYPE'].startswith('multipart/related'))

        head, *body, tail = resp.content._buffer.splitlines()
        self.assertTrue(tail.startswith(head))
        self.assertEqual(
            b'Content-Type: application/json\r\n\r\n{"_id": "foo"}',
            b'\r\n'.join(body))

    def test_get_with_atts_params(self):
        resp = self.mock_response(
            headers={'CONTENT-TYPE': 'multipart/related;boundary=:'})
        self.request.return_value = self.future(resp)

        all_params = {
            'att_encoding_info': True,
            'atts_since': ['1-ABC'],
            'conflicts': False,
            'deleted_conflicts': True,
            'local_seq': True,
            'meta': False,
            'rev': '1-ABC',
            'revs': True,
            'revs_info': True
        }

        for key, value in all_params.items():
            self.run_loop(self.doc.get_with_atts(**{key: value}))
            if key == 'atts_since':
                value = json.dumps(value)
            self.assert_request_called_with(
                'GET', 'db', 'docid',
                headers={'ACCEPT': 'multipart/*, application/json'},
                params={key: value, 'attachments': True})

    def test_update(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.update({}))
        self.assert_request_called_with('PUT', 'db', 'docid',
                                        data={})
        self.assertEqual({}, result)

    def test_update_params(self):
        resp = self.mock_json_response()
        self.request.return_value = self.future(resp)

        all_params = {
            'batch': "ok",
            'new_edits': True,
            'rev': '1-ABC'
        }

        for key, value in all_params.items():
            self.run_loop(self.doc.update({}, **{key: value}))
            self.assert_request_called_with('PUT', 'db', 'docid',
                                            data={},
                                            params={key: value})

    def test_update_expect_mapping(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        self.assertRaises(TypeError, self.run_loop, self.doc.update([]))

        class Foo(dict):
            pass

        doc = Foo()
        self.run_loop(self.doc.update(doc))
        self.assert_request_called_with('PUT', 'db', 'docid', data={})

    def test_update_reject_docid_collision(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        self.assertRaises(ValueError,
                          self.run_loop,
                          self.doc.update({'_id': 'foo'}))

    def test_remove(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.remove('1-ABC'))
        self.assert_request_called_with('DELETE', 'db', 'docid',
                                        params={'rev': '1-ABC'})
        self.assertEqual({}, result)

    def test_remove_preserve_content(self):
        resp = self.mock_json_response(data=b'{"_id": "foo", "bar": "baz"}')
        self.request.return_value = self.future(resp)

        self.run_loop(self.doc.remove('1-ABC', preserve_content=True))
        self.assert_request_called_with('PUT', 'db', 'docid',
                                        data={'_id': 'foo',
                                              '_deleted': True,
                                              'bar': 'baz'},
                                        params={'rev': '1-ABC'})

    def test_copy(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.copy('newid'))
        self.assert_request_called_with('COPY', 'db', 'docid',
                                        headers={'DESTINATION': 'newid'})
        self.assertEqual({}, result)

    def test_copy_rev(self):
        resp = self.mock_json_response(data=b'{}')
        self.request.return_value = self.future(resp)

        result = self.run_loop(self.doc.copy('idx', '1-A'))
        self.assert_request_called_with('COPY', 'db', 'docid',
                                        headers={'DESTINATION': 'idx?rev=1-A'})
        self.assertEqual({}, result)


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
        result = self.run_loop(reader.next())

        self.assertIsInstance(result, tuple)
        self.assertEqual(2, len(result))

        doc, subreader = result

        self.assertEqual({'_id': 'foo'}, doc)
        self.assertIsInstance(subreader, reader.multipart_reader_cls)

        partreader = self.run_loop(subreader.next())
        self.assertIsInstance(partreader, subreader.part_reader_cls)

        data = self.run_loop(partreader.next())
        self.assertEqual(b'some data', data)

        next_data = self.run_loop(partreader.next())
        self.assertIsNone(next_data)
        self.assertTrue(partreader.at_eof())

        next_data = self.run_loop(subreader.next())
        self.assertIsNone(next_data)
        self.assertTrue(subreader.at_eof())

        next_data = self.run_loop(reader.next())
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
        result = self.run_loop(reader.next())

        self.assertIsInstance(result, tuple)
        self.assertEqual(2, len(result))

        doc, subreader = result

        self.assertEqual({'_id': 'foo'}, doc)
        self.assertIsInstance(subreader, reader.part_reader_cls)

        next_data = self.run_loop(subreader.next())
        self.assertIsNone(next_data)
        self.assertTrue(subreader.at_eof())

        next_data = self.run_loop(reader.next())
        self.assertEqual((None, None), next_data)
        self.assertTrue(reader.at_eof())

