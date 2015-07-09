# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from aiohttp.multipart import (
    MultipartReader as _MultipartReader,
    MultipartWriter as _MultipartWriter,
    BodyPartReader as _BodyPartReader,
    BodyPartWriter as _BodyPartWriter,
)
from aiocouchdb.hdrs import (
    CONTENT_ENCODING,
    CONTENT_LENGTH,
    CONTENT_TRANSFER_ENCODING,
)


class BodyPartReader(_BodyPartReader):

    # https://github.com/KeepSafe/aiohttp/commit/d3fa32eb3
    # TODO: remove read_chunk fix with aiohttp==0.17.0

    @asyncio.coroutine
    def read(self, *, decode=False):
        """Reads body part data.

        :param bool decode: Decodes data following by encoding
                            method from `Content-Encoding` header. If it missed
                            data remains untouched

        :rtype: bytearray
        """
        if self._at_eof:
            return b''
        data = bytearray()
        if self._length is None:
            while not self._at_eof:
                data.extend((yield from self.readline()))
        else:
            while not self._at_eof:
                data.extend((yield from self.read_chunk(self.chunk_size)))
        if decode:
            return self.decode(data)
        return data

    @asyncio.coroutine
    def read_chunk(self, size=_BodyPartReader.chunk_size):
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
        chunk = yield from self._content.read(chunk_size)
        self._read_bytes += chunk_size
        if self._read_bytes == self._length:
            self._at_eof = True
            assert b'\r\n' == (yield from self._content.readline()), \
                'reader did not read all the data or it is malformed'
        return chunk

    @asyncio.coroutine
    def release(self):
        """Lke :meth:`read`, but reads all the data to the void.

        :rtype: None
        """
        if self._at_eof:
            return
        if self._length is None:
            while not self._at_eof:
                yield from self.readline()
        else:
            while not self._at_eof:
                yield from self.read_chunk(self.chunk_size)


class MultipartReader(_MultipartReader):

    part_reader_cls = BodyPartReader


class BodyPartWriter(_BodyPartWriter):

    def calc_content_length(self):
        has_encoding = (
            CONTENT_ENCODING in self.headers
            and self.headers[CONTENT_ENCODING] != 'identity'
            or CONTENT_TRANSFER_ENCODING in self.headers
        )
        if has_encoding:
            raise ValueError('Cannot calculate content length')

        if CONTENT_LENGTH not in self.headers:
            raise ValueError('No content length')

        total = 0
        for keyvalue in self.headers.items():
            total += sum(map(lambda i: len(i.encode('latin1')), keyvalue))
            total += 4  # key-value delimiter and \r\n
        total += 4  # delimiter of headers from body

        total += int(self.headers[CONTENT_LENGTH])

        return total


class MultipartWriter(_MultipartWriter):

    part_writer_cls = BodyPartWriter

    def calc_content_length(self):
        total = 0
        len_boundary = len(self.boundary)
        for part in self.parts:
            total += len_boundary + 4  # -- and \r\n
            total += part.calc_content_length()
        total += len_boundary + 6  # -- and --\r\n
        return total
