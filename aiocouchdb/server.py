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
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def active_tasks(self):
        """Returns list of :ref:`active tasks <api/server/active_tasks>`
        which runs on server.

        :rtype: list
        """
        resp = yield from self.resource.get('_active_tasks')
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def all_dbs(self):
        """Returns list of available :ref:`databases <api/server/all_dbs>`
        on server.

        :rtype: list
        """
        resp = yield from self.resource.get('_all_dbs')
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

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
            return JsonFeed(resp)
        elif feed == 'eventsource':
            return Feed(resp)
        else:
            return (yield from resp.json(close=True))

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
        return (yield from resp.read(close=True)).decode('utf-8')

    @asyncio.coroutine
    def replicate(self, source, target, *,
                  auth=None,
                  cancel=None,
                  continuous=None,
                  create_target=None,
                  doc_ids=None,
                  filter=None,
                  headers=None,
                  proxy=None,
                  query_params=None,
                  since_seq=None,
                  checkpoint_interval=None,
                  connection_timeout=None,
                  http_connections=None,
                  retries_per_request=None,
                  socket_options=None,
                  use_checkpoints=None,
                  worker_batch_size=None,
                  worker_processes=None):
        """:ref:`Runs a replication <api/server/replicate>` from ``source``
        to ``target``.

        :param str source: Source database name or URL
        :param str target: Target database name or URL

        :param dict auth: Authorization object for the target database
        :param bool cancel: Cancels active replication
        :param bool continuous: Runs continuous replication
        :param bool create_target: Creates target database if it not exists
        :param list doc_ids: List of specific document ids to replicate
        :param str filter: Filter function name
        :param dict headers: Custom replication request headers
        :param str proxy: Proxy server URL
        :param dict query_params: Custom query parameters for filter function
        :param since_seq: Start replication from specified sequence number

        :param int checkpoint_interval: Tweaks `checkpoint_interval`_ option
        :param int connection_timeout: Tweaks `connection_timeout`_ option
        :param int http_connections: Tweaks `http_connections`_ option
        :param int retries_per_request: Tweaks `retries_per_request`_ option
        :param str socket_options: Tweaks `socket_options`_ option
        :param bool use_checkpoints: Tweaks `use_checkpoints`_ option
        :param int worker_batch_size: Tweaks `worker_batch_size`_ option
        :param int worker_processes: Tweaks `worker_processes`_ option

        :rtype: dict

        .. _checkpoint_interval: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/checkpoint_interval
        .. _connection_timeout: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/connection_timeout
        .. _http_connections: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/http_connections
        .. _retries_per_request: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/retries_per_request
        .. _socket_options: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/socket_options
        .. _use_checkpoints: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/use_checkpoints
        .. _worker_batch_size: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/worker_batch_size
        .. _worker_processes: http://docs.couchdb.org/en/latest/config/replicator.html#replicator/worker_processes

        """
        doc = {'source': source, 'target': target}
        maybe_set_param = (
            lambda doc, *kv: (None if kv[1] is None else doc.update([kv])))
        maybe_set_param(doc, 'auth', auth)
        maybe_set_param(doc, 'cancel', cancel)
        maybe_set_param(doc, 'continuous', continuous)
        maybe_set_param(doc, 'create_target', create_target)
        maybe_set_param(doc, 'doc_ids', doc_ids)
        maybe_set_param(doc, 'filter', filter)
        maybe_set_param(doc, 'headers', headers)
        maybe_set_param(doc, 'proxy', proxy)
        maybe_set_param(doc, 'query_params', query_params)
        maybe_set_param(doc, 'since_seq', since_seq)
        maybe_set_param(doc, 'checkpoint_interval', checkpoint_interval)
        maybe_set_param(doc, 'connection_timeout', connection_timeout)
        maybe_set_param(doc, 'http_connections', http_connections)
        maybe_set_param(doc, 'retries_per_request', retries_per_request)
        maybe_set_param(doc, 'socket_options', socket_options)
        maybe_set_param(doc, 'use_checkpoints', use_checkpoints)
        maybe_set_param(doc, 'worker_batch_size', worker_batch_size)
        maybe_set_param(doc, 'worker_processes', worker_processes)
        resp = yield from self.resource.post('_replicate', data=doc)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def restart(self):
        """:ref:`Restarts <api/server/restart>` server instance.

        :rtype: dict
        """
        resp = yield from self.resource.post('_restart')
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))


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
        return (yield from resp.json(close=True))

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
        return (yield from resp.json(close=True))

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
        return (yield from resp.json(close=True))
