# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import asyncio.futures
import asyncio.queues
from asyncio.tasks import coroutine


__all__ = (
    'QueueClosed',
    'WorkQueue',
)


class QueueClosed(Exception):
    """Exception raised when the WorkQueue.put() or WorkQueue.put_nowait()
    method is called on a WorkQueue object which is closed."""


class WorkQueue(asyncio.queues.Queue):
    """Like :class:`asyncio.queue.Queue`, but with ability to get multiple items
    from the queue in single :meth:`get` call and close queue."""

    #: Sentinel object which returns on get when queue is closed.
    CLOSED = type('CLOSED', (object,), {'__slots__': tuple()})()

    def __init__(self, maxsize=0, *, loop=None):
        super().__init__(maxsize, loop=loop)
        self._closed = False

    def _get(self, max_items: int=None) -> list:
        if max_items is None:
            max_items = self.qsize()
        return [self._queue.popleft()
                for _ in range(min(max_items, self.qsize()))]

    def is_closed(self) -> bool:
        """Checks if queue is closed."""
        return self._closed

    def close(self):
        """Closes a queue."""
        self._consume_done_putters()
        self._consume_done_getters()
        self._closed = True

    def get_all(self):
        self._consume_done_putters()
        return self.get(self.qsize())

    @coroutine
    def get(self, maxitems: int=1) -> list:
        """Remove and return multiple items (up to maxitems amount) from
        the queue.

        If there are less than requested amount of items in queue, then they
        all wll be returned.

        If queue is empty, then caller will wait until anything becomes
        available for get.

        If queue is closed, special :attr:`CLOSED` sentinel object will be
        returned immediately if no more items is left.
        """
        self._consume_done_putters()
        if self._putters:
            assert self.full(), 'queue not full, why are putters waiting?'
            item, putter = self._putters.popleft()
            self._put(item)

            # When a getter runs and frees up a slot so this putter can
            # run, we need to defer the put for a tick to ensure that
            # getters and putters alternate perfectly. See
            # ChannelTest.test_wait.
            self._loop.call_soon(putter._set_result_unless_cancelled, None)

            return self._get(maxitems)

        elif self.qsize():
            return self._get(maxitems)

        elif self.is_closed():
            return self.CLOSED

        else:
            waiter = asyncio.futures.Future(loop=self._loop)

            self._getters.append(waiter)
            return (yield from waiter)

    def get_nowait(self, maxitems: int=1) -> list:
        """Remove and return multiple items (up to maxitems amount) from
        the queue immediately."""
        self._consume_done_putters()
        if self._putters:
            assert self.full(), 'queue not full, why are putters waiting?'
            item, putter = self._putters.popleft()
            self._put(item)
            # Wake putter on next tick.

            # getter cannot be cancelled, we just removed done putters
            putter.set_result(None)

            return self._get(maxitems)

        elif self.qsize():
            return self._get(maxitems)

        elif self._closed:
            return self.CLOSED

        else:
            raise asyncio.queues.QueueEmpty

    @coroutine
    def put(self, item):
        self._consume_done_getters()

        if self._closed:
            raise QueueClosed

        yield from super().put(item)

    def put_nowait(self, item):
        self._consume_done_getters()

        if self._closed:
            raise QueueClosed

        super().put_nowait(item)
