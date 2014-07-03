# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.feeds
import aiocouchdb.tests.utils as utils


class FeedTestCase(utils.TestCase):

    def test_read_chunks(self):
        resp = self.mock_response(data=[b'foo\r\n', b'bar\r\n'])

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.run_loop(feed.next())
        self.assertEqual(b'foo', result)

        result = self.run_loop(feed.next())
        self.assertEqual(b'bar', result)

        result = self.run_loop(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_ignore_empty_chunks(self):
        resp = self.mock_response(data=[b'foo\r\n', b'\r\n',  b'\r\n'
                                        b'\r\n',  b'\r\n', b'bar\r\n'])

        feed = aiocouchdb.feeds.Feed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.run_loop(feed.next())
        self.assertEqual(b'foo', result)

        result = self.run_loop(feed.next())
        self.assertEqual(b'bar', result)

        result = self.run_loop(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_json_chunks(self):
        resp = self.mock_response(data=[b'{"foo": true}\r\n',
                                        b'{"bar": null}\r\n'])

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.run_loop(feed.next())
        self.assertEqual({'foo': True}, result)

        result = self.run_loop(feed.next())
        self.assertEqual({'bar': None}, result)

        result = self.run_loop(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())

    def test_read_empty_json_feed(self):
        resp = self.mock_response()

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.run_loop(feed.next())
        self.assertEqual(None, result)

        self.assertFalse(feed.is_active())

    def test_calling_inactive_feed_returns_none(self):
        resp = self.mock_response()

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        self.run_loop(feed.next())

        self.assertFalse(feed.is_active())

        result = self.run_loop(feed.next())
        self.assertEqual(None, result)

    def test_close_resp_on_feed_end(self):
        resp = self.mock_response()

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        self.run_loop(feed.next())

        self.assertFalse(feed.is_active())
        self.assertTrue(resp.close.called)

    def test_force_close_resp_on_error(self):
        resp = self.mock_response(err=ValueError)

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        self.assertRaises(ValueError,
                          self.loop.run_until_complete,
                          feed.next())

        self.assertFalse(feed.is_active())
        resp.close.assert_called_with(force=True)


class JsonFeedTestCase(utils.TestCase):

    def test_read_json_chunks(self):
        resp = self.mock_response(data=[b'"foo"\r\n', b'{"bar": "baz"}\r\n'])

        feed = aiocouchdb.feeds.JsonFeed(resp, loop=self.loop)
        self.assertTrue(feed.is_active())

        result = self.run_loop(feed.next())
        self.assertEqual('foo', result)

        result = self.run_loop(feed.next())
        self.assertEqual({'bar': 'baz'}, result)

        result = self.run_loop(feed.next())
        self.assertEqual(None, result)
        self.assertFalse(feed.is_active())
