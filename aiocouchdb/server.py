# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from .client import Resource
from .errors import maybe_raise_error
from .feeds import Feed, JsonFeed


class Server(object):
    """Implementation of :ref:`CouchDB Server API <api/server>`."""

    resource_class = Resource

    def __init__(self, url='http://localhost:5984', *, resource_class=None):
        if resource_class is not None:
            self.resource_class = resource_class
        if not isinstance(url, self.resource_class):
            url = self.resource_class(url)
        self.resource = url
        self._config = Config(self.resource)

    @asyncio.coroutine
    def info(self):
        """Returns server :ref:`meta information and welcome message
        <api/server/root>`.

        :rtype: dict
        """
        resp = yield from self.resource.get()
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def active_tasks(self):
        """Returns list of :ref:`active tasks <api/server/active_tasks>`
        which runs on server.

        :rtype: list
        """
        resp = yield from self.resource.get('_active_tasks')
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def all_dbs(self):
        """Returns list of available :ref:`databases <api/server/all_dbs>`
        on server.

        :rtype: list
        """
        resp = yield from self.resource.get('_all_dbs')
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @property
    def config(self):
        """Proxy to the related :class:`~aiocouchdb.server.Config` instance."""
        return self._config

    @asyncio.coroutine
    def db_updates(self, *, feed=None, timeout=None, heartbeat=None):
        """Emits :ref:`databases events <api/server/db_updates>` for
        the related server instance.

        :param str feed: Feed type
        :param int timeout: Timeout in milliseconds
        :param bool heartbeat: Whenever use heartbeats to keep connection alive

        Depending on feed type returns:

        - :class:`dict` - for default or ``longpoll`` feed
        - :class:`aiocouchdb.feeds.JsonFeed` - for ``continuous`` feed
        - :class:`aiocouchdb.feeds.Feed` - for ``eventsource`` feed
        """
        params = {}
        if feed:
            params['feed'] = feed
        if timeout:
            params['timeout'] = timeout
        if heartbeat:
            params['heartbeat'] = heartbeat
        resp = yield from self.resource.get('_db_updates', params=params)
        yield from maybe_raise_error(resp)
        if feed == 'continuous':
            return JsonFeed(resp.content)
        elif feed == 'eventsource':
            return Feed(resp.content)
        else:
            return (yield from resp.json())

    @asyncio.coroutine
    def log(self, *, bytes=None, offset=None):
        """Returns a chunk of data from the tail of :ref:`CouchDB's log
        <api/server/log>` file.

        :param int bytes: Bytes to return
        :param int offset: Offset in bytes where the log tail should be started

        :rtype: str
        """
        params = {}
        if bytes:
            params['bytes'] = bytes
        if offset:
            params['offset'] = offset
        resp = yield from self.resource.get('_log', params=params)
        yield from maybe_raise_error(resp)
        return (yield from resp.read()).decode('utf-8')


class Config(object):
    """Implements :ref:`/_config/* <api/config>` API. Should be used thought
    :attr:`server.config <aiocouchdb.server.Server.config>` property."""

    def __init__(self, resource):
        self.resource = resource('_config')
        
    @asyncio.coroutine
    def get(self, section=None, key=None):
        """Returns :ref:`server configuration <api/config>`. Depending on
        specified arguments returns:

        - :ref:`Complete configuration <api/config>` if ``section`` and ``key``
          are ``None``

        - :ref:`Section options <api/config/section>` if ``section``
          was specified

        - :ref:`Option value <api/config/section/key>` if both ``section``
          and ``key`` were specified

        :param str section: Section name (`optional`)
        :param str key: Option name (`optional`)

        :rtype: dict or str
        """
        path = []
        if section:
            path.append(section)
        if key:
            assert isinstance(section, str)
            path.append(key)
        resp = yield from self.resource(*path).get()
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def update(self, section, key, value):
        """Updates specific :ref:`configuration option <api/config/section/key>`
        value and returns the old one back.

        :param str section: Configuration section name
        :param str key: Option name
        :param str value: New option value

        :rtype: str
        """
        resp = yield from self.resource(section).put(key, data=value)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def remove(self, section, key):
        """Removes specific :ref:`configuration option <api/config/section/key>`
        and returns it value back.

        :param string section: Configuration section name
        :param string key: Option name

        :rtype: str
        """
        resp = yield from self.resource(section).delete(key)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())
