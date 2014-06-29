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
import unittest
import unittest.mock as mock
from io import BytesIO

import aiocouchdb.feeds

URL = 'http://localhost:5984'


class FeedTestCase(unittest.TestCase):

    def setUp(self):
        self.transport = unittest.mock.Mock()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_read_chunks(self):
        def read():
            chunk = data.read(5)
            if chunk:
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(chunk)
                return fut
            raise aiohttp.EofStream
        data = BytesIO(b'foo\r\nbar\r\n')
        content = mock.Mock()
        content.read = read

        feed = aiocouchdb.feeds.Feed(content, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(b'foo', result)

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(b'bar', result)

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_ignore_empty_chunks(self):
        def read():
            chunk = data.read(6)
            if chunk:
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(chunk)
                return fut
            raise aiohttp.EofStream
        data = BytesIO(b'data\r\n\r\n\r\n\r\ndata\r\n')
        content = mock.Mock()
        content.read = read

        feed = aiocouchdb.feeds.Feed(content, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(b'data', result)

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(b'data', result)

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_json_chunks(self):
        def read():
            chunk = data.read(15)
            if chunk:
                fut = asyncio.Future(loop=self.loop)
                fut.set_result(chunk)
                return fut
            raise aiohttp.EofStream
        data = BytesIO(b'{"foo": true}\r\n{"bar": null}\r\n')
        content = mock.Mock()
        content.read = read

        feed = aiocouchdb.feeds.JsonFeed(content, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual({'foo': True}, result)

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual({'bar': None}, result)

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_empty_json_feed(self):
        def read():
            raise aiohttp.EofStream
        content = mock.Mock()
        content.read = read

        feed = aiocouchdb.feeds.JsonFeed(content, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(None, result)

        self.assertFalse(feed.is_active())

    def test_calling_inactive_feed_returns_none(self):
        def read():
            raise aiohttp.EofStream
        content = mock.Mock()
        content.read = read

        feed = aiocouchdb.feeds.JsonFeed(content, loop=self.loop)
        self.assertTrue(feed.is_active())

        self.loop.run_until_complete(feed.next())

        self.assertFalse(feed.is_active())

        result = self.loop.run_until_complete(feed.next())
        self.assertEqual(None, result)
