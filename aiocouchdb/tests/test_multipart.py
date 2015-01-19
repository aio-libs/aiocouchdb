# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import io
import os
import unittest
import unittest.mock as mock
import aiocouchdb.client
import aiocouchdb.multipart
import aiocouchdb.hdrs

from aiohttp.helpers import parse_mimetype
from aiocouchdb.hdrs import (
    CONTENT_DISPOSITION,
    CONTENT_TYPE
)
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


class PartReaderTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.boundary = b'--:'

    def test_next(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        result = yield from obj.next()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertTrue(obj.at_eof())

    def test_next_next(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        result = yield from obj.next()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertTrue(obj.at_eof())
        result = yield from obj.next()
        self.assertIsNone(result)

    def test_read(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        result = yield from obj.read()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertTrue(obj.at_eof())

    def test_read_chunk_at_eof(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'--:'))
        obj._at_eof = True
        result = yield from obj.read_chunk()
        self.assertEqual(b'', result)

    def test_read_chunk_requires_content_length(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:'))
        with self.assertRaises(AssertionError):
            yield from obj.read_chunk()

    def test_read_does_reads_boundary(self):
        stream = Stream(b'Hello, world!\r\n--:')
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, stream)
        result = yield from obj.read()
        self.assertEqual(b'Hello, world!\r\n', result)
        self.assertEqual(b'', (yield from stream.read()))
        self.assertEqual([b'--:'], obj._unread)

    def test_multiread(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello,\r\n--:\r\n\r\nworld!\r\n--:--'))
        result = yield from obj.read()
        self.assertEqual(b'Hello,\r\n', result)
        result = yield from obj.read()
        self.assertEqual(b'', result)
        self.assertTrue(obj.at_eof())

    def test_read_multiline(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello\n,\r\nworld!\r\n--:--'))
        result = yield from obj.read()
        self.assertEqual(b'Hello\n,\r\nworld!\r\n', result)
        result = yield from obj.read()
        self.assertEqual(b'', result)
        self.assertTrue(obj.at_eof())

    def test_read_respects_content_length(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-LENGTH': 100500},
            Stream(b'.' * 100500 + b'\r\n--:--'))
        result = yield from obj.read()
        self.assertEqual(b'.' * 100500, result)
        self.assertTrue(obj.at_eof())

    def test_read_with_content_encoding_gzip(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'gzip'},
            Stream(b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x0b\xc9\xccMU'
                   b'(\xc9W\x08J\xcdI\xacP\x04\x00$\xfb\x9eV\x0e\x00\x00\x00'
                   b'\r\n--:--'))
        result = yield from obj.read(decode=True)
        self.assertEqual(b'Time to Relax!', result)

    def test_read_with_content_encoding_deflate(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'deflate'},
            Stream(b'\x0b\xc9\xccMU(\xc9W\x08J\xcdI\xacP\x04\x00\r\n--:--'))
        result = yield from obj.read(decode=True)
        self.assertEqual(b'Time to Relax!', result)

    def test_read_with_content_encoding_unknown(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'snappy'},
            Stream(b'\x0e4Time to Relax!\r\n--:--'))
        with self.assertRaises(AttributeError):
            yield from obj.read(decode=True)

    def test_read_text(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream(b'Hello, world!\r\n--:--'))
        result = yield from obj.text()
        self.assertEqual('Hello, world!\r\n', result)

    def test_read_text_encoding(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, Stream('Привет, Мир!\r\n--:--'.encode('cp1251')))
        result = yield from obj.text(encoding='cp1251')
        self.assertEqual('Привет, Мир!\r\n', result)

    def test_read_text_guess_encoding(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'text/plain;charset=cp1251'},
            Stream('Привет, Мир!\r\n--:--'.encode('cp1251')))
        result = yield from obj.text()
        self.assertEqual('Привет, Мир!\r\n', result)

    def test_read_text_compressed(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'deflate',
                            'CONTENT-TYPE': 'text/plain'},
            Stream(b'\x0b\xc9\xccMU(\xc9W\x08J\xcdI\xacP\x04\x00\r\n--:--'))
        result = yield from obj.text()
        self.assertEqual('Time to Relax!', result)

    def test_read_text_while_closed(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'text/plain'}, Stream(b''))
        obj._at_eof = True
        result = yield from obj.text()
        self.assertEqual('', result)

    def test_read_json(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json'},
            Stream(b'{"test": "passed"}\r\n--:--'))
        result = yield from obj.json()
        self.assertEqual({'test': 'passed'}, result)

    def test_read_json_encoding(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json'},
            Stream('{"тест": "пассед"}\r\n--:--'.encode('cp1251')))
        result = yield from obj.json(encoding='cp1251')
        self.assertEqual({'тест': 'пассед'}, result)

    def test_read_json_guess_encoding(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json; charset=cp1251'},
            Stream('{"тест": "пассед"}\r\n--:--'.encode('cp1251')))
        result = yield from obj.json()
        self.assertEqual({'тест': 'пассед'}, result)

    def test_read_json_compressed(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-ENCODING': 'deflate',
                            'CONTENT-TYPE': 'application/json'},
            Stream(b'\xabV*I-.Q\xb2RP*H,.NMQ\xaa\x05\x00\r\n--:--'))
        result = yield from obj.json()
        self.assertEqual({'test': 'passed'}, result)

    def test_read_json_while_closed(self):
        stream = Stream(b'')
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-TYPE': 'application/json'}, stream)
        obj._at_eof = True
        result = yield from obj.json()
        self.assertEqual(None, result)

    def test_release(self):
        stream = Stream(b'Hello,\r\n--:\r\n\r\nworld!\r\n--:--')
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, stream)
        yield from obj.release()
        self.assertTrue(obj.at_eof())
        self.assertEqual(b'\r\nworld!\r\n--:--', stream.content.read())
        self.assertEqual([b'--:\r\n'], obj._unread)

    def test_release_respects_content_length(self):
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {'CONTENT-LENGTH': 100500},
            Stream(b'.' * 100500 + b'\r\n--:--'))
        result = yield from obj.release()
        self.assertIsNone(result)
        self.assertTrue(obj.at_eof())

    def test_release_release(self):
        stream = Stream(b'Hello,\r\n--:\r\n\r\nworld!\r\n--:--')
        obj = aiocouchdb.multipart.BodyPartReader(
            self.boundary, {}, stream)
        yield from obj.release()
        yield from obj.release()
        self.assertEqual(b'\r\nworld!\r\n--:--', stream.content.read())
        self.assertEqual([b'--:\r\n'], obj._unread)


class MultipartReaderTestCase(utils.TestCase):

    def test_from_response(self):
        resp = Response({'CONTENT-TYPE': 'multipart/related;boundary=:'},
                        Stream(b'--:\r\n\r\nhello\r\n--:--'))
        res = aiocouchdb.multipart.MultipartReader.from_response(resp)
        self.assertIsInstance(res,
                              aiocouchdb.multipart.MultipartResponseWrapper)
        self.assertIsInstance(res.stream,
                              aiocouchdb.multipart.MultipartReader)

    def test_dispatch(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        res = reader._get_part_reader({'CONTENT-TYPE': 'text/plain'})
        self.assertIsInstance(res, reader.part_reader_cls)

    def test_dispatch_bodypart(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        res = reader._get_part_reader({'CONTENT-TYPE': 'text/plain'})
        self.assertIsInstance(res, reader.part_reader_cls)

    def test_dispatch_multipart(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'----:--\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'----:--\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'----:----\r\n'
                   b'--:--'))
        res = reader._get_part_reader(
            {'CONTENT-TYPE': 'multipart/related;boundary=--:--'})
        self.assertIsInstance(res, reader.__class__)

    def test_dispatch_custom_multipart_reader(self):
        class CustomReader(aiocouchdb.multipart.MultipartReader):
            pass
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'----:--\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'----:--\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'----:----\r\n'
                   b'--:--'))
        reader.multipart_reader_cls = CustomReader
        res = reader._get_part_reader(
            {'CONTENT-TYPE': 'multipart/related;boundary=--:--'})
        self.assertIsInstance(res, CustomReader)

    def test_emit_next(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        res = yield from reader.next()
        self.assertIsInstance(res, reader.part_reader_cls)

    def test_invalid_boundary(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'---:\r\n\r\necho\r\n---:--'))
        with self.assertRaises(ValueError):
            yield from reader.next()

    def test_release(self):
        reader = aiocouchdb.multipart.MultipartReader(
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
        yield from reader.release()
        self.assertTrue(reader.at_eof())

    def test_release_release(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        yield from reader.release()
        self.assertTrue(reader.at_eof())
        yield from reader.release()
        self.assertTrue(reader.at_eof())

    def test_release_next(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n\r\necho\r\n--:--'))
        yield from reader.release()
        self.assertTrue(reader.at_eof())
        res = yield from reader.next()
        self.assertIsNone(res)

    def test_second_next_releases_previous_object(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'--:\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'--:--'))
        first = yield from reader.next()
        self.assertIsInstance(first, aiocouchdb.multipart.BodyPartReader)
        second = yield from reader.next()
        self.assertTrue(first.at_eof())
        self.assertFalse(second.at_eof())

    def test_release_without_read_the_last_object(self):
        reader = aiocouchdb.multipart.MultipartReader(
            {'CONTENT-TYPE': 'multipart/related;boundary=:'},
            Stream(b'--:\r\n'
                   b'\r\n'
                   b'test\r\n'
                   b'--:\r\n'
                   b'\r\n'
                   b'passed\r\n'
                   b'--:--'))
        first = yield from reader.next()
        second = yield from reader.next()
        third = yield from reader.next()
        self.assertTrue(first.at_eof())
        self.assertTrue(second.at_eof())
        self.assertTrue(second.at_eof())
        self.assertIsNone(third)


class BodyPartWriterTestCase(unittest.TestCase):

    def setUp(self):
        self.part = aiocouchdb.multipart.BodyPartWriter(b'')

    def test_guess_content_length(self):
        self.assertIsNone(self.part._guess_content_length({}))
        self.assertIsNone(self.part._guess_content_length(object()))
        self.assertEqual(3, self.part._guess_content_length(io.BytesIO(b'foo')))
        self.assertIsNone(self.part._guess_content_length(io.StringIO('foo')))
        self.assertEqual(3, self.part._guess_content_length(b'bar'))
        with open(__file__, 'rb') as f:
            self.assertEqual(os.fstat(f.fileno()).st_size,
                             self.part._guess_content_length(f))

    def test_guess_content_type(self):
        default = 'application/octet-stream'
        self.assertEqual(default, self.part._guess_content_type(b'foo'))
        self.assertEqual('text/plain; charset=utf-8',
                         self.part._guess_content_type('foo'))
        with open(__file__, 'rb') as f:
            self.assertEqual('text/x-python',
                             self.part._guess_content_type(f))

    def test_guess_filename(self):
        class Named:
            name = 'foo'
        self.assertIsNone(self.part._guess_filename({}))
        self.assertIsNone(self.part._guess_filename(object()))
        self.assertIsNone(self.part._guess_filename(io.BytesIO(b'foo')))
        self.assertIsNone(self.part._guess_filename(Named()))
        with open(__file__, 'rb') as f:
            self.assertEqual(os.path.basename(f.name),
                             self.part._guess_filename(f))

    def test_autoset_content_disposition(self):
        self.part.obj = open(__file__, 'rb')
        self.part._fill_headers_with_defaults()
        self.assertIn(CONTENT_DISPOSITION, self.part.headers)
        fname = os.path.basename(self.part.obj.name)
        self.assertEquals(
            'attachment; filename="{0}"; filename*=utf-8\'\'{0}'.format(fname),
            self.part.headers[CONTENT_DISPOSITION])

    def test_set_content_disposition(self):
        self.part.set_content_disposition('attachment', foo='bar')
        self.assertEquals(
            'attachment; foo=bar',
            self.part.headers[CONTENT_DISPOSITION])

    def test_set_content_disposition_bad_type(self):
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('foo bar')
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('тест')
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('foo\x00bar')
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('')

    def test_set_content_disposition_bad_param(self):
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('inline', **{'foo bar': 'baz'})
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('inline', **{'тест': 'baz'})
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('inline', **{'': 'baz'})
        with self.assertRaises(ValueError):
            self.part.set_content_disposition('inline', **{'foo\x00bar': 'baz'})

    def test_serialize_bytes(self):
        self.assertEqual(b'foo', next(self.part._serialize_bytes(b'foo')))

    def test_serialize_str(self):
        self.assertEqual(b'foo', next(self.part._serialize_str('foo')))

    def test_serialize_str_custom_encoding(self):
        self.part.headers[CONTENT_TYPE] = \
            'text/plain;charset=cp1251'
        self.assertEqual('привет'.encode('cp1251'),
                         next(self.part._serialize_str('привет')))

    def test_serialize_io(self):
        self.assertEqual(b'foo',
                         next(self.part._serialize_io(io.BytesIO(b'foo'))))
        self.assertEqual(b'foo',
                         next(self.part._serialize_io(io.StringIO('foo'))))

    def test_serialize_io_chunk(self):
        flo = io.BytesIO(b'foobarbaz')
        self.part._chunk_size = 3
        self.assertEqual([b'foo', b'bar', b'baz'],
                         list(self.part._serialize_io(flo)))

    def test_serialize_json(self):
        self.assertEqual(b'{"\\u043f\\u0440\\u0438\\u0432\\u0435\\u0442":'
                         b' "\\u043c\\u0438\\u0440"}',
                         next(self.part._serialize_json({'привет': 'мир'})))

    def test_serialize_multipart(self):
        multipart = aiocouchdb.multipart.MultipartWriter(boundary=':')
        multipart.append('foo-bar-baz')
        multipart.append_json({'test': 'passed'})
        self.assertEqual(
            [b'--:\r\n',
             b'CONTENT-TYPE: text/plain; charset=utf-8',
             b'\r\n\r\n',
             b'foo-bar-baz',
             b'\r\n',
             b'--:\r\n',
             b'CONTENT-TYPE: application/json',
             b'\r\n\r\n',
             b'{"test": "passed"}',
             b'\r\n',
             b'--:--\r\n',
             b''],
            list(self.part._serialize_multipart(multipart))
        )

    def test_serialize_default(self):
        with self.assertRaises(TypeError):
            next(self.part._serialize_default(object()))


class MultipartWriterTestCase(unittest.TestCase):

    def setUp(self):
        self.writer = aiocouchdb.multipart.MultipartWriter(boundary=':')

    def test_default_subtype(self):
        mtype, stype, *_ = parse_mimetype(self.writer.headers.get(CONTENT_TYPE))
        self.assertEqual('multipart', mtype)
        self.assertEqual('mixed', stype)

    def test_bad_boundary(self):
        with self.assertRaises(ValueError):
            aiocouchdb.multipart.MultipartWriter(boundary='тест')

    def test_default_headers(self):
        self.assertEqual({CONTENT_TYPE: 'multipart/mixed; boundary=:'},
                         self.writer.headers)

    def test_iter_parts(self):
        self.writer.append('foo')
        self.writer.append('bar')
        self.writer.append('baz')
        self.assertEqual(3, len(list(self.writer)))

    def test_append(self):
        self.assertEqual(0, len(self.writer))
        self.writer.append('hello, world!')
        self.assertEqual(1, len(self.writer))
        self.assertIsInstance(self.writer.parts[0], self.writer.part_writer_cls)

    def test_append_with_headers(self):
        self.writer.append('hello, world!', {'x-foo': 'bar'})
        self.assertEqual(1, len(self.writer))
        self.assertIn('x-foo', self.writer.parts[0].headers)
        self.assertEqual(self.writer.parts[0].headers['x-foo'], 'bar')

    def test_append_json(self):
        self.writer.append_json({'foo': 'bar'})
        self.assertEqual(1, len(self.writer))
        part = self.writer.parts[0]
        self.assertEqual(part.headers[CONTENT_TYPE], 'application/json')

    def test_append_part(self):
        part = aiocouchdb.multipart.BodyPartWriter('test',
                                                   {CONTENT_TYPE: 'text/plain'})
        self.writer.append(part, {CONTENT_TYPE: 'test/passed'})
        self.assertEqual(1, len(self.writer))
        part = self.writer.parts[0]
        self.assertEqual(part.headers[CONTENT_TYPE], 'test/passed')

    def test_append_json_overrides_content_type(self):
        self.writer.append_json({'foo': 'bar'}, {CONTENT_TYPE: 'test/passed'})
        self.assertEqual(1, len(self.writer))
        part = self.writer.parts[0]
        self.assertEqual(part.headers[CONTENT_TYPE], 'application/json')

    def test_serialize(self):
        self.assertEqual([b''], list(self.writer.serialize()))

    def test_with(self):
        with aiocouchdb.multipart.MultipartWriter(boundary=':') as writer:
            writer.append('foo')
            writer.append(b'bar')
            writer.append_json({'baz': True})
        self.assertEqual(3, len(writer))
