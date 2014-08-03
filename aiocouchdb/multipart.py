# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json
from aiohttp.protocol import HttpParser
from aiohttp.helpers import parse_mimetype


class MultipartResponseWrapper(object):
    """Wrapper around the :class:`MultipartBodyReader` to take care about
    underlying connection and close it when it needs in."""

    def __init__(self, resp, stream):
        self.resp = resp
        self.stream = stream

    def at_eof(self):
        """Returns ``True`` when all response data had been read.

        :rtype: bool
        """
        return self.resp.content.at_eof()

    @asyncio.coroutine
    def next(self):
        """Emits next multipart reader object."""
        item = yield from self.stream.next()
        if self.stream.at_eof():
            yield from self.release()
        return item

    @asyncio.coroutine
    def release(self):
        """Releases the connection gracefully, reading all the content
        to the void."""
        yield from self.resp.release()


class MultipartBodyPartReader(object):
    """Multipart reader for single body part."""

    _chunk_size = 8192

    def __init__(self, boundary, headers, content):
        self.boundary = boundary
        self.content = content
        self.headers = headers
        self._at_eof = False
        length = self.headers.get('CONTENT-LENGTH', None)
        self._length = int(length) if length is not None else None
        self._read_bytes = 0

    @asyncio.coroutine
    def next(self):
        item = yield from self.read()
        if not item:
            return None
        return item

    @asyncio.coroutine
    def read(self):
        """Reads body part data.

        :rtype: bytearray
        """
        if self._at_eof:
            return b''
        data = bytearray()
        if self._length is None:
            data.extend((yield from self.content.readline()))
            self._at_eof = True
        else:
            while not self._at_eof:
                data.extend((yield from self.read_chunk(self._chunk_size)))
            assert b'\r\n' == (yield from self.content.readline()), \
                'reader did not read all the data or it is malformed'
        return data

    @asyncio.coroutine
    def read_chunk(self, size=_chunk_size):
        """Reads body part content chunk of the specified size.
        The body part must has `Content-Length` header with proper value.

        :param int size: chunk size

        :rtype: bytearray
        """
        if self._at_eof:
            return b''
        assert self._length is not None, \
            'Content-Length required for chunked read'
        chunk_size = min(size, self._length - self._read_bytes)
        chunk = yield from self.content.read(chunk_size)
        self._read_bytes += chunk_size
        if self._read_bytes == self._length:
            self._at_eof = True
        return chunk

    @asyncio.coroutine
    def release(self):
        """Lke :meth:`read`, but reads all the data to the void.

        :rtype: None
        """
        if self._at_eof:
            return
        if self._length is None:
            yield from self.content.readline()
            self._at_eof = True
        else:
            while not self._at_eof:
                yield from self.read_chunk(self._chunk_size)
            assert b'\r\n' == (yield from self.content.readline())

    @asyncio.coroutine
    def text(self, *, encoding=None):
        """Lke :meth:`read`, but assumes that body part contains text data.

        :param str encoding: Custom text encoding. Overrides specified
                             in charset param of `Content-Type` header

        :rtype: str
        """
        data = yield from self.read()
        encoding = encoding or get_charset(self.headers, default='latin1')
        return data.decode(encoding)

    @asyncio.coroutine
    def json(self, *, encoding=None):
        """Lke :meth:`read`, but assumes that body parts contains JSON data.

        :param str encoding: Custom JSON encoding. Overrides specified
                             in charset param of `Content-Type` header
        """
        data = yield from self.read()
        if not data:
            return None
        encoding = encoding or get_charset(self.headers, default='utf-8')
        return json.loads(data.decode(encoding))

    def at_eof(self):
        """Returns ``True`` if the boundary was reached or
        ``False`` otherwise.

        :rtype: bool
        """
        return self._at_eof


class MultipartBodyReader(object):
    """Multipart body reader."""

    #: Response wrapper, used when multipart readers constructs from response.
    response_wrapper_cls = MultipartResponseWrapper
    #: Multipart reader class, used to handle multipart/* body parts.
    #: None points to type(self)
    multipart_reader_cls = None
    #: Body part reader class for non multipart/* content types.
    part_reader_cls = MultipartBodyPartReader
    #: Mapping of content-type in format ``(basetype, subtype)``
    #: to the related handler which provides the right reader for it.
    dispatch_map = {}

    def __init__(self, headers, content):
        self.boundary = get_boundary(headers)
        self.content = content
        self.headers = headers
        self._last_part = None
        self._at_eof = False

    @classmethod
    def from_response(cls, response):
        """Constructs reader instance from HTTP response.

        :param response: :class:`~aiocouchdb.client.HttpResponse` instance
        """
        obj = cls.response_wrapper_cls(response, cls(response.headers,
                                                     response.content))
        return obj

    def at_eof(self):
        """Returns ``True`` if the final boundary was reached or
        ``False`` otherwise.

        :rtype: bool
        """
        return self._at_eof

    @asyncio.coroutine
    def next(self):
        """Emits the next multipart body part."""
        if self._at_eof:
            return
        yield from self.maybe_release_last_part()
        yield from self.read_boundary()
        if self._at_eof:  # we just read the last boundary, nothing to do there
            return
        self._last_part = yield from self.fetch_next_part()
        return self._last_part

    @asyncio.coroutine
    def release(self):
        """Reads all the body parts to the void till the final boundary."""
        while not self._at_eof:
            item = yield from self.next()
            if item is None:
                break
            yield from item.release()

    @asyncio.coroutine
    def read_boundary(self):
        """Reads the next boundary."""
        chunk = (yield from self.content.readline()).rstrip()
        if chunk == self.boundary:
            pass
        elif chunk == self.boundary + b'--':
            self._at_eof = True
        else:
            raise ValueError('Invalid boundary %r, expected %r'
                             % (chunk, self.boundary))
    @asyncio.coroutine
    def maybe_release_last_part(self):
        """Ensures that the last read body part is read completely."""
        if self._last_part is not None:
            if not self._last_part.at_eof():
                yield from self._last_part.release()
            self._last_part = None

    @asyncio.coroutine
    def fetch_next_part(self):
        """Returns the next body part reader."""
        headers = yield from read_headers(self.content)
        return self.dispatch(headers)

    def dispatch(self, headers):
        """Dispatches the response by the `Content-Type` header, returning
        suitable reader instance.

        :param dict headers: Response headers
        """
        ctype = headers.get('CONTENT-TYPE', '')
        mtype, stype, *_ = parse_mimetype(ctype)
        for key in ((mtype, stype), (mtype, None), None):
            handler = self.dispatch_map.get(key)
            if handler is not None:
                return handler(self, headers)
        raise AttributeError('no handler available for content type %r', ctype)

    def dispatch_multipart(self, headers):
        """Returns multipart body reader instance.

        :param dict headers: Response headers
        :rtype: :class:`MultipartBodyReader`
        """
        if self.multipart_reader_cls is None:
            return type(self)(headers, self.content)
        return self.multipart_reader_cls(headers, self.content)
    dispatch_map[('multipart', None)] = dispatch_multipart

    def dispatch_bodypart(self, headers):
        """Returns body part reader instance.

        :param dict headers: Response headers
        :rtype: :class:`MultipartBodyPartReader`
        """
        return self.part_reader_cls(self.boundary, headers, self.content)
    dispatch_map[None] = dispatch_bodypart


@asyncio.coroutine
def read_headers(content):
    lines = ['']
    while True:
        chunk = yield from content.readline()
        chunk = chunk.decode().strip()
        lines.append(chunk)
        if not chunk:
            break
    parser = HttpParser()
    headers, *_ = parser.parse_headers(lines)
    return headers


def get_boundary(headers):
    mtype, *_, params = parse_mimetype(headers['CONTENT-TYPE'])
    assert mtype == 'multipart'
    return ('--%s' % params['boundary']).encode()


def get_charset(headers, default=None):
    ctype = headers.get('CONTENT-TYPE', '')
    *_, params = parse_mimetype(ctype)
    return params.get('charset', default)
