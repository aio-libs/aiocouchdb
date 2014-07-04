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
        self._active = True
        self._resp = resp
        asyncio.Task(self._loop(), loop=loop)

    @asyncio.coroutine
    def _loop(self):
        try:
            while self._active:
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
        """Emits the next response chunk or ``None`` is feed is empty.

        :rtype: bytearray
        """
        if not self.is_active():
            return None
        chunk = yield from self._queue.get()
        if isinstance(chunk, BaseException):
            yield from self._queue.get()
            raise chunk from None
        return chunk

    def is_active(self):
        """Checks if the feed is still able to emit any data.

        :rtype: bool
        """
        return self._active or not self._queue.empty()

    def close(self, force=False):
        """Closes feed and the related request connection.

        :param bool force: In case of True, close connection instead of release.
                           See :meth:`aiohttp.client.ClientResponse.close` for
                           the details
        """
        self._queue.put_nowait(None)
        self._active = False
        self._resp.close(force=force)


class JsonFeed(Feed):
    """As :class:`Feed`, but for chunked JSON response. Assumes that each
    received chunk is valid JSON object and decodes them before emit."""

    @asyncio.coroutine
    def next(self):
        """Decodes feed chunk with JSON before emit it.

        :rtype: dict
        """
        chunk = yield from super().next()
        if chunk is not None:
            return json.loads(chunk.decode('utf-8'))


class ViewFeed(Feed):
    """Like :class:`JsonFeed`, but uses CouchDB view response specifics."""

    _total_rows = None
    _offset = None
    _update_seq = None

    @asyncio.coroutine
    def next(self):
        """Emits view result row.

        :rtype: dict
        """
        chunk = yield from super().next()
        if chunk is None:
            return chunk
        elif chunk.startswith(b'{"total_rows"'):
            chunk += b']}'
            event = json.loads(chunk.decode('utf-8'))
            self._total_rows = event['total_rows']
            self._offset = event.get('offset')
            return (yield from self.next())
        elif chunk.startswith((b'{"rows"', b']}')):
            return (yield from self.next())
        else:
            return json.loads(chunk.strip(b',').decode('utf-8'))

    @property
    def offset(self):
        """Returns view results offset."""
        return self._offset

    @property
    def total_rows(self):
        """Returns total rows in view."""
        return self._total_rows

    @property
    def update_seq(self):
        """Returns update sequence for a view."""
        return self._update_seq


class EventSourceFeed(Feed):
    """Handles `EventSource`_ response.

    .. _EventSource: http://www.w3.org/TR/eventsource/
    """

    @asyncio.coroutine
    def next(self):
        """Emits decoded EventSource event.

        :rtype: dict
        """
        chunk = (yield from super().next())
        if chunk is None:
            return chunk
        chunk = chunk.decode('utf-8')
        event = dict([item.split(': ', 1) for item in chunk.split('\n')])
        if 'data' in event:
            event['data'] = json.loads(event['data'])
        return event
