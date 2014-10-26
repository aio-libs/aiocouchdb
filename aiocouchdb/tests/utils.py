# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import contextlib
import datetime
import functools
import os
import random
import unittest
import unittest.mock as mock
import uuid as _uuid
from collections import deque, defaultdict

import aiohttp
import aiocouchdb.client
import aiocouchdb.server
from aiocouchdb.client import urljoin


TARGET = os.environ.get('AIOCOUCHDB_TARGET', 'mock')


def run_in_loop(f):
    @functools.wraps(f)
    def wrapper(testcase, *args, **kwargs):
        coro = asyncio.coroutine(f)
        future = asyncio.wait_for(coro(testcase, *args, **kwargs),
                                  timeout=testcase.timeout)
        return testcase.loop.run_until_complete(future)
    return wrapper


class MetaAioTestCase(type):

    def __new__(cls, name, bases, attrs):
        for key, obj in attrs.items():
            if key.startswith('test_'):
                attrs[key] = run_in_loop(obj)
        return super().__new__(cls, name, bases, attrs)


class TestCase(unittest.TestCase, metaclass=MetaAioTestCase):

    url = 'http://localhost:5984'
    timeout = 5
    target = TARGET

    def setUp(self):
        def tracer(f):
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                current_task = asyncio.Task.current_task(loop=self.loop)
                self._req_per_task[current_task].append((args, kwargs))
                return f(*args, **kwargs)
            return wrapper
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.patch = mock.patch(
            'aiohttp.request',
            wraps=tracer(aiohttp.request) if self.target != 'mock' else None)
        self.request = self.patch.start()
        self.set_response(self.prepare_response())
        self._req_per_task = defaultdict(list)

        self.server = aiocouchdb.server.Server(self.url)
        self.cookie = None

    def tearDown(self):
        self.patch.stop()
        self.loop.close()

    def future(self, obj):
        fut = asyncio.Future(loop=self.loop)
        fut.set_result(obj)
        return fut

    def prepare_response(self, *,
                         cookies=None,
                         data=b'',
                         err=None,
                         headers=None,
                         status=200):
        def side_effect(*args, **kwargs):
            fut = asyncio.Future(loop=self.loop)
            if queue:
                resp.content.at_eof.return_value = False
                fut.set_result(queue.popleft())
            elif err:
                fut.set_exception(err)
            else:
                resp.content.at_eof.return_value = True
                fut.set_result(b'')
            return fut
        headers = headers or {}
        headers.setdefault('CONTENT-TYPE', 'application/json')
        cookies = cookies or {}

        queue = deque(data if isinstance(data, list) else [data])

        resp = aiocouchdb.client.HttpResponse('', '')
        resp.status = status
        resp.headers = headers
        resp.cookies = cookies
        resp.content = unittest.mock.Mock()
        resp.content._buffer = bytearray()
        resp.content.at_eof.return_value = False
        resp.content.read.side_effect = side_effect
        resp.close = mock.Mock()

        return resp

    @contextlib.contextmanager
    def response(self, *,
                 cookies=None,
                 data=b'',
                 err=None,
                 headers=None,
                 status=200):
        resp = self.prepare_response(cookies=cookies,
                                     data=data,
                                     err=err,
                                     headers=headers,
                                     status=status)

        self.set_response(resp)
        yield resp
        self.set_response(self.prepare_response())

    def set_response(self, resp):
        if self.target == 'mock':
            self.request.return_value = self.future(resp)

    def assert_request_called_with(self, method, *path, **kwargs):
        self.assertTrue(self.request.called and self.request.call_count >= 1)

        current_task = asyncio.Task.current_task(loop=self.loop)
        if current_task in self._req_per_task:
            call_args, call_kwargs = self._req_per_task[current_task][-1]
        else:
            call_args, call_kwargs = self.request.call_args
        self.assertEqual((method, urljoin(self.url, *path)), call_args)

        kwargs.setdefault('data', None)
        kwargs.setdefault('headers', {})
        kwargs.setdefault('params', {})
        for key, value in kwargs.items():
            self.assertIn(key, call_kwargs)
            if value is not Ellipsis:
                self.assertEqual(value, call_kwargs[key])


def modify_server(section, option, value):
    assert section != 'admins', 'use `with_fixed_admin_party` decorator'

    @asyncio.coroutine
    def apply_config_changes(server, cookie):
        oldval = yield from server.config.update(section, option, value,
                                                 auth=cookie)
        return oldval

    @asyncio.coroutine
    def revert_config_changes(server, cookie, oldval):
        if not oldval:
            yield from server.config.delete(section, option, auth=cookie)
        else:
            if not (yield from server.config.exists(section, option)):
                return
            oldval = yield from server.config.update(section, option, oldval,
                                                     auth=cookie)
            assert oldval == value, ('{} != {}'.format(oldval, value))

    def decorator(f):
        @functools.wraps(f)
        def wrapper(testcase, **kwargs):
            server, cookie = testcase.server, testcase.cookie
            oldval = yield from apply_config_changes(server, cookie)
            try:
                yield from f(testcase, **kwargs)
            finally:
                yield from revert_config_changes(server, cookie, oldval)
        return wrapper
    return decorator


def with_fixed_admin_party(username, password):
    @asyncio.coroutine
    def apply_config_changes(server, cookie):
        oldval = yield from server.config.update('admins', username, password,
                                                 auth=cookie)
        cookie = yield from server.session.open(username, password)
        return oldval, cookie

    @asyncio.coroutine
    def revert_config_changes(server, cookie, oldval):
        if not oldval:
            yield from server.config.delete('admins', username, auth=cookie)
        else:
            yield from server.config.update('admins', username, oldval,
                                            auth=cookie)

    def decorator(f):
        @functools.wraps(f)
        def wrapper(testcase, **kwargs):
            server, cookie = testcase.server, testcase.cookie
            oldval, cookie = yield from apply_config_changes(server, cookie)
            if cookie is not None:
                kwargs[username] = cookie
            try:
                yield from f(testcase, **kwargs)
            finally:
                yield from revert_config_changes(server, cookie, oldval)
        return wrapper
    return decorator


def using_database(dbarg='db'):
    @asyncio.coroutine
    def create_database(server, cookie):
        db = server[dbname()]
        yield from db.create(auth=cookie)
        return db

    @asyncio.coroutine
    def drop_database(db, cookie):
        yield from db.delete(auth=cookie)

    def decorator(f):
        @functools.wraps(f)
        def wrapper(testcase, **kwargs):
            server, cookie = testcase.server, testcase.cookie

            with testcase.response(data=b'{"ok": true}'):
                db = yield from create_database(server, cookie)

            assert dbarg not in kwargs, \
                'conflict: both {} and {} are referenced as {}'.format(
                    db, kwargs[dbarg], dbarg
                )

            kwargs[dbarg] = db

            try:
                yield from f(testcase, **kwargs)
            finally:
                with testcase.response(data=b'{"ok": true}'):
                    yield from drop_database(db, cookie)
        return wrapper
    return decorator


@asyncio.coroutine
def populate_database(db, docs_count):
    def generate_docs(count):
        for _ in range(count):
            dt = datetime.datetime.fromtimestamp(
                random.randint(1234567890, 2345678901)
            )
            dta = [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second]
            doc = {
                '_id': uuid(),
                'created_at': dta,
                'num': random.randint(0, 10),
                'type': random.choice(['a', 'b', 'c'])
            }
            yield doc

    if not (yield from db.exists()):
        yield from db.create()

    return (yield from db.bulk_docs(generate_docs(docs_count)))


def uuid():
    return _uuid.uuid4().hex


def dbname(idx=None, prefix='test/aiocouchdb'):
    if idx:
        return '/'.join((prefix, idx, uuid()))
    else:
        return '/'.join((prefix, uuid()))


def run_for(*targets):
    def decorator(f):
        @functools.wraps(f)
        @unittest.skipIf(TARGET not in targets,
                         'runs only for targets: %s' % ', '.join(targets))
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper
    return decorator


def skip_for(*targets):
    def decorator(f):
        @functools.wraps(f)
        @unittest.skipIf(TARGET in targets,
                         'skips for targets: %s' % ', '.join(targets))
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapper
    return decorator
