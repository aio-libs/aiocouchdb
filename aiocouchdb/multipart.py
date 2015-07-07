# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

# flake8: noqa

from aiohttp.multipart import (
    MultipartReader,
    MultipartWriter as _MultipartWriter,
    BodyPartReader,
    BodyPartWriter as _BodyPartWriter,
)
from aiocouchdb.hdrs import (
    CONTENT_ENCODING,
    CONTENT_LENGTH,
    CONTENT_TRANSFER_ENCODING,
)


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
