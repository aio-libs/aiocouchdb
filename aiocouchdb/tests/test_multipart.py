# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import io
import unittest.mock as mock
import aiocouchdb.client
import aiocouchdb.multipart

from . import utils


class Response(object):

    def __init__(self, headers, content):
        self.headers = headers
        self.content = content


class Stream(object):

    def __init__(self, content):
        self.content = io.BytesIO(content)

    @asyncio.coroutine
    def read(self, size=None):
        return self.content.read(size)

    @asyncio.coroutine
    def readline(self):
        return self.content.readline()


class MultipartResponseWrapperTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        wrapper = aiocouchdb.multipart.MultipartResponseWrapper(mock.Mock(),
                                                                mock.Mock())
        self.wrapper = wrapper

    def test_at_eof(self):
        self.wrapper.at_eof()
        self.assertTrue(self.wrapper.resp.content.at_eof.called)

    def test_next(self):
        self.wrapper.stream.next.return_value = self.future(b'')
        self.wrapper.stream.at_eof.return_value = False
        yield from self.wrapper.next()
        self.assertTrue(self.wrapper.stream.next.called)

    def test_release(self):
        self.wrapper.resp.release.return_value = self.future(None)
        yield from self.wrapper.release()
        self.assertTrue(self.wrapper.resp.release.called)

    def test_release_when_stream_at_eof(self):
        self.wrapper.resp.release.return_value = self.future(None)
        self.wrapper.stream.next.return_value = self.future(b'')
        self.wrapper.stream.at_eof.return_value = True
        yield from self.wrapper.next()
        self.assertTrue(self.wrapper.stream.next.called)
        self.assertTrue(self.wrapper.resp.release.called)


class MultipartBodyPartReaderTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.boundary = b'--:'

    def test_next(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        result = yield from obj.next()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertTrue(obj.at_eof())

    def test_next_next(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        result = yield from obj.next()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertTrue(obj.at_eof())
        result = yield from obj.next()
        self.assertIsNone(result)

    def test_read(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        result = yield from obj.read()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertTrue(obj.at_eof())

    def test_read_chunk_at_eof(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'--:'))
        obj._at_eof = True
        result = yield from obj.read_chunk()
        self.assertEqual(b'', result)

    def test_read_chunk_requires_content_length(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        with self.assertRaises(AssertionError):
            yield from obj.read_chunk()

    def test_read_does_reads_boundary(self):
        stream = Stream(b'Hello, world!\r\n--:')
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, stream)
        result = yield from obj.read()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertEqual(b'', (yield from stream.read()))
        self.assertEqual([b'--:'], obj._unread)

    def test_multiread(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello,\r\n--:\r\n\r\nworld!\r\n--:--'))
        result = yield from obj.read()
        self.assertEqual(b'Hello,\r\n', result)
        result = yield from obj.read()
        self.assertEqual(b'', result)
        self.assertTrue(obj.at_eof())

    def test_read_multiline(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello\n,\r\nworld!\r\n--:--'))
        result = yield from obj.read()
        self.assertEqual(b'Hello\n,\r\nworld!\r\n', result)
        result = yield from obj.read()
        self.assertEqual(b'', result)
        self.assertTrue(obj.at_eof())

    def test_read_respects_content_length(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-LENGTH': 100500},
            Stream(b'.' * 100500 + b'\r\n--:--'))
        result = yield from obj.read()
        self.assertEqual(b'.' * 100500, result)
        self.assertTrue(obj.at_eof())

    def test_read_with_content_encoding_gzip(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'gzip'},
            Stream(b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x0b\xc9\xccMU'
                   b'(\xc9W\x08J\xcdI\xacP\x04\x00$\xfb\x9eV\x0e\x00\x00\x00'
                   b'\r\n--:--'))
        result = yield from obj.read(decode=True)
        self.assertEqual(b'Time to Relax!', result)

    def test_read_with_content_encoding_deflate(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'deflate'},
            Stream(b'\x0b\xc9\xccMU(\xc9W\x08J\xcdI\xacP\x04\x00\r\n--:--'))
        result = yield from obj.read(decode=True)
        self.assertEqual(b'Time to Relax!', result)

    def test_read_with_content_encoding_unknown(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'snappy'},
            Stream(b'\x0e4Time to Relax!\r\n--:--'))
        with self.assertRaises(AttributeError):
            yield from obj.read(decode=True)

    def test_read_text(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:--'))
        result = yield from obj.text()
        self.assertEqual('Hello, world!\r\n', result)

    def test_read_text_encoding(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, Stream('Привет, Мир!\r\n--:--'.encode('cp1251')))
        result = yield from obj.text(encoding='cp1251')
        self.assertEqual('Привет, Мир!\r\n', result)

    def test_read_text_guess_encoding(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'text/plain;charset=cp1251'},
            Stream('Привет, Мир!\r\n--:--'.encode('cp1251')))
        result = yield from obj.text()
        self.assertEqual('Привет, Мир!\r\n', result)

    def test_read_text_compressed(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'deflate',
                            'CONTENT-TYPE': 'text/plain'},
            Stream(b'\x0b\xc9\xccMU(\xc9W\x08J\xcdI\xacP\x04\x00\r\n--:--'))
        result = yield from obj.text()
        self.assertEqual('Time to Relax!', result)

    def test_read_text_while_closed(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'text/plain'}, Stream(b''))
        obj._at_eof = True
        result = yield from obj.text()
        self.assertEqual('', result)

    def test_read_json(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json'},
            Stream(b'{"test": "passed"}\r\n--:--'))
        result = yield from obj.json()
        self.assertEqual({'test': 'passed'}, result)

    def test_read_json_encoding(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json'},
            Stream('{"тест": "пассед"}\r\n--:--'.encode('cp1251')))
        result = yield from obj.json(encoding='cp1251')
        self.assertEqual({'тест': 'пассед'}, result)

    def test_read_json_guess_encoding(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json; charset=cp1251'},
            Stream('{"тест": "пассед"}\r\n--:--'.encode('cp1251')))
        result = yield from obj.json()
        self.assertEqual({'тест': 'пассед'}, result)

    def test_read_json_compressed(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'deflate',
                            'CONTENT-TYPE': 'application/json'},
            Stream(b'\xabV*I-.Q\xb2RP*H,.NMQ\xaa\x05\x00\r\n--:--'))
        result = yield from obj.json()
        self.assertEqual({'test': 'passed'}, result)

    def test_read_json_while_closed(self):
        stream = Stream(b'')
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json'}, stream)
        obj._at_eof = True
        result = yield from obj.json()
        self.assertEqual(None, result)

    def test_release(self):
        stream = Stream(b'Hello,\r\n--:\r\n\r\nworld!\r\n--:--')
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, stream)
        yield from obj.release()
        self.assertTrue(obj.at_eof())
        self.assertEqual(b'\r\nworld!\r\n--:--', stream.content.read())
        self.assertEqual([b'--:\r\n'], obj._unread)

    def test_release_respects_content_length(self):
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {'CONTENT-LENGTH': 100500},
            Stream(b'.' * 100500 + b'\r\n--:--'))
        result = yield from obj.release()
        self.assertIsNone(result)
        self.assertTrue(obj.at_eof())

    def test_release_release(self):
        stream = Stream(b'Hello,\r\n--:\r\n\r\nworld!\r\n--:--')
        obj = aiocouchdb.multipart.MultipartBodyPartReader(
            self.boundary, {}, stream)
        yield from obj.release()
        yield from obj.release()
        self.assertEqual(b'\r\nworld!\r\n--:--', stream.content.read())
        self.assertEqual([b'--:\r\n'], obj._unread)


class MultipartBodyReaderTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'Content-Type: text/plain\r\necho\r\n--:--'))

    def test_from_response(self):
        resp = Response({'CONTENT-TYPE': 'multipart/related;boundary=:'},
                        Stream(b'--:\r\n\r\nhello\r\n--:--'))
        res = aiocouchdb.multipart.MultipartBodyReader.from_response(resp)
        self.assertIsInstance(res,
                              aiocouchdb.multipart.MultipartResponseWrapper)
        self.assertIsInstance(res.stream,
                              aiocouchdb.multipart.MultipartBodyReader)

    def test_dispatch(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        res = stream.dispatch({'CONTENT-TYPE': 'text/plain'})
        self.assertIsInstance(res, stream.part_reader_cls)

    def test_dispatch_bodypart(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        res = stream.dispatch_bodypart({'CONTENT-TYPE': 'text/plain'})
        self.assertIsInstance(res, stream.part_reader_cls)

    def test_dispatch_multipart(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'----:--\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'----:--\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'----:----\r\n'
                   b'--:--'))
        res = stream.dispatch_multipart(
            {'CONTENT-TYPE': 'multipart/related;boundary=--:--'})
        self.assertIsInstance(res, stream.__class__)

    def test_dispatch_custom_multipart_reader(self):
        class Dispatcher(aiocouchdb.multipart.MultipartBodyReader):
            pass
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'----:--\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'----:--\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'----:----\r\n'
                   b'--:--'))
        stream.multipart_reader_cls = Dispatcher
        res = stream.dispatch_multipart(
            {'CONTENT-TYPE': 'multipart/related;boundary=--:--'})
        self.assertIsInstance(res, Dispatcher)

    def test_emit_next(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        res = yield from stream.next()
        self.assertIsInstance(res, stream.part_reader_cls)

    def test_invalid_boundary(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'---:\r\n\r\necho\r\n---:--'))
        with self.assertRaises(ValueError):
            yield from stream.next()

    def test_release(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/mixed;boundary=:'},
            Stream(b'--:\r\n'
                   b'Content-Type: multipart/related;boundary=--:--\r\n'
                   b'\r\n'
                   b'----:--\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'----:--\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'----:----\r\n'
                   b'--:--'))
        yield from stream.release()
        self.assertTrue(stream.at_eof())

    def test_release_release(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        yield from stream.release()
        self.assertTrue(stream.at_eof())
        yield from stream.release()
        self.assertTrue(stream.at_eof())

    def test_release_next(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        yield from stream.release()
        self.assertTrue(stream.at_eof())
        res = yield from stream.next()
        self.assertIsNone(res)

    def test_second_next_releases_previous_object(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'--:\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'--:--'))
        first = yield from stream.next()
        self.assertIsInstance(first,
                              aiocouchdb.multipart.MultipartBodyPartReader)
        second = yield from stream.next()
        self.assertTrue(first.at_eof())
        self.assertFalse(second.at_eof())

    def test_release_without_read_the_last_object(self):
        stream = aiocouchdb.multipart.MultipartBodyReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'--:\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'--:--'))
        first = yield from stream.next()
        second = yield from stream.next()
        third = yield from stream.next()
        self.assertTrue(first.at_eof())
        self.assertTrue(second.at_eof())
        self.assertTrue(second.at_eof())
        self.assertIsNone(third)
