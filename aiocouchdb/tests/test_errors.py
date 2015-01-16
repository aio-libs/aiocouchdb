# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiohttp

import aiocouchdb.client
import aiocouchdb.errors

from . import utils


class HttpErrorsTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.resp = self.prepare_response(
            status=500,
            data=b'{"error": "test", "reason": "passed"}')

    def test_should_not_raise_error_on_success_response(self):
        self.resp.status = 200
        yield from aiocouchdb.errors.maybe_raise_error(self.resp)

    def test_raise_aiohttp_exception(self):
        with self.assertRaises(aiohttp.errors.HttpProcessingError):
            yield from aiocouchdb.errors.maybe_raise_error(self.resp)

    def test_decode_common_error_response(self):
        try:
            yield from aiocouchdb.errors.maybe_raise_error(self.resp)
        except aiocouchdb.errors.HttpErrorException as exc:
            self.assertEqual('test', exc.error)
            self.assertEqual('passed', exc.reason)
        else:
            assert False, 'exception expected'

    def test_exception_holds_response_headers(self):
        self.resp.headers['X-Foo'] = 'bar'
        try:
            yield from aiocouchdb.errors.maybe_raise_error(self.resp)
        except aiocouchdb.errors.HttpErrorException as exc:
            self.assertEqual('bar', exc.headers.get('X-Foo'))
        else:
            assert False, 'exception expected'

    def test_exc_to_str(self):
        try:
            yield from aiocouchdb.errors.maybe_raise_error(self.resp)
        except aiocouchdb.errors.HttpErrorException as exc:
            self.assertEqual('(test) passed', str(exc))
        else:
            assert False, 'exception expected'
