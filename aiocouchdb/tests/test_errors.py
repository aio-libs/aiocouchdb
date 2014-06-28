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

import aiocouchdb.client
import aiocouchdb.errors

URL = 'http://localhost:5984'


class HttpErrorsTestCase(unittest.TestCase):

    def setUp(self):
        self.resp = aiocouchdb.client.HttpResponse('get', URL)
        self.resp._content = b'{"error": "test", "reason": "passed"}'
        self.resp.headers = {'CONTENT-TYPE': 'application/json'}
        self.resp.status = 500
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_should_not_raise_error_on_success_response(self):
        self.resp.status = 200
        self.loop.run_until_complete(
            aiocouchdb.errors.maybe_raise_error(self.resp))

    def test_raise_aiohttp_exception(self):
        self.assertRaises(aiohttp.errors.HttpException,
                          self.loop.run_until_complete,
                          aiocouchdb.errors.maybe_raise_error(self.resp))

    def test_decode_common_error_response(self):
        try:
            self.loop.run_until_complete(
                aiocouchdb.errors.maybe_raise_error(self.resp))
        except aiocouchdb.errors.CouchHttpError as exc:
            self.assertEqual('test', exc.error)
            self.assertEqual('passed', exc.reason)
        else:
            assert False, 'exception expected'

    def test_exception_holds_response_headers(self):
        self.resp.headers['X-Foo'] = 'bar'
        try:
            self.loop.run_until_complete(
                aiocouchdb.errors.maybe_raise_error(self.resp))
        except aiocouchdb.errors.CouchHttpError as exc:
            self.assertEqual('bar', exc.headers.get('X-Foo'))
        else:
            assert False, 'exception expected'

    def test_exc_to_str(self):
        try:
            self.loop.run_until_complete(
                aiocouchdb.errors.maybe_raise_error(self.resp))
        except aiocouchdb.errors.CouchHttpError as exc:
            self.assertEqual('(test) passed', str(exc))
        else:
            assert False, 'exception expected'
