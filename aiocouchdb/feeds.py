# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiohttp
import json


class Feed(object):
    """Wrapper over :class:`HttpResponse` content to stream continuous response
    by emitted chunks."""

    def __init__(self, resp, *, loop=None):
        self._queue = asyncio.Queue(loop=loop)
        self._eof = False
        self._resp = resp
        asyncio.Task(self._loop(), loop=loop)

    @asyncio.coroutine
    def _loop(self):
        try:
            while True:
                chunk = yield from self._resp.content.read()
                chunk = chunk.strip()
                if not chunk:
                    continue
                self._queue.put_nowait(chunk)
        except aiohttp.EofStream:
            self.close()
        except Exception as exc:
            self._queue.put_nowait(exc)
            self.close(True)

    @asyncio.coroutine
    def next(self):
        """Emits next response chunk or ``None`` is feed is empty.

        :rtype: bytearray
        """
        if not self.is_active():
            return None
        item = yield from self._queue.get()
        if isinstance(item, BaseException):
            yield from self._queue.get()
            raise item from None
        return item

    def is_active(self):
        """Checks if the feed is still able to emit any data."""
        return not (self._eof and self._queue.empty())

    def close(self, force=False):
        self._queue.put_nowait(None)
        self._eof = True
        self._resp.close(force=force)


class JsonFeed(Feed):
    """As :class:`Feed`, but for chunked JSON response."""

    @asyncio.coroutine
    def next(self):
        """Assumes that each emitted chunk is valid JSON object and decodes
        them before return."""
        value = yield from super().next()
        if value is not None:
            return json.loads(value.decode('utf-8'))
