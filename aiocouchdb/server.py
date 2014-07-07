# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from .authn import CookieAuthProvider
from .client import Resource
from .database import Database
from .errors import maybe_raise_error
from .feeds import EventSourceFeed, JsonFeed


class Server(object):
    """Implementation of :ref:`CouchDB Server API <api/server>`."""

    database_class = Database

    def __init__(self, url_or_resource='http://localhost:5984', *,
                 database_class=None):
        if database_class is not None:
            self.database_class = database_class
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource
        self._session = Session(self.resource)
        self._config = Config(self.resource)

    @asyncio.coroutine
    def info(self, *, auth=None):
        """Returns server :ref:`meta information and welcome message
        <api/server/root>`.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.get(auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def active_tasks(self, *, auth=None):
        """Returns list of :ref:`active tasks <api/server/active_tasks>`
        which runs on server.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: list
        """
        resp = yield from self.resource.get('_active_tasks', auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def all_dbs(self, *, auth=None):
        """Returns list of available :ref:`databases <api/server/all_dbs>`
        on server.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: list
        """
        resp = yield from self.resource.get('_all_dbs', auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @property
    def config(self):
        """Proxy to the related :class:`~aiocouchdb.server.Config` instance."""
        return self._config

    @asyncio.coroutine
    def database(self, dbname, *, auth=None):
        """Returns :class:`~aiocouchdb.database.Database` instance against
        specified ``dbname``.

        If database is missed or isn't accessible for provided credentials
        this method raises :exc:`aiocouchdb.errors.HttpErrorException`
        for related response status code.

        :param str dbname: Database name
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: :class:`aiocouchdb.database.Database`
        """
        db_resource = self.resource(dbname)
        resp = yield from db_resource.head(auth=auth)
        yield from maybe_raise_error(resp)
        yield from resp.read()
        return self.database_class(db_resource)

    @asyncio.coroutine
    def db_updates(self, *, feed=None, timeout=None, heartbeat=None, auth=None):
        """Emits :ref:`databases events <api/server/db_updates>` for
        the related server instance.

        :param str feed: Feed type
        :param int timeout: Timeout in milliseconds
        :param bool heartbeat: Whenever use heartbeats to keep connection alive
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        Depending on feed type returns:

        - :class:`dict` - for default or ``longpoll`` feed
        - :class:`aiocouchdb.feeds.JsonFeed` - for ``continuous`` feed
        - :class:`aiocouchdb.feeds.Feed` - for ``eventsource`` feed
        """
        params = {}
        if feed is not None:
            params['feed'] = feed
        if timeout is not None:
            params['timeout'] = timeout
        if heartbeat is not None:
            params['heartbeat'] = heartbeat
        resp = yield from self.resource.get('_db_updates',
                                            auth=auth, params=params)
        yield from maybe_raise_error(resp)
        if feed == 'continuous':
            return JsonFeed(resp)
        elif feed == 'eventsource':
            return EventSourceFeed(resp)
        else:
            return (yield from resp.json())

    @asyncio.coroutine
    def log(self, *, bytes=None, offset=None, auth=None):
        """Returns a chunk of data from the tail of :ref:`CouchDB's log
        <api/server/log>` file.

        :param int bytes: Bytes to return
        :param int offset: Offset in bytes where the log tail should be started
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: str
        """
        params = {}
        if bytes is not None:
            params['bytes'] = bytes
        if offset is not None:
            params['offset'] = offset
        resp = yield from self.resource.get('_log',  auth=auth, params=params)
        yield from maybe_raise_error(resp)
        return (yield from resp.read()).decode('utf-8')

    @asyncio.coroutine
    def replicate(self, source, target, *,
                  auth=None,
                  authobj=None,
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

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
                      (don't confuse with ``authobj`` which belongs to
                      replication options)

        :param dict authobj: Authentication object for the target database
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
        maybe_set_param(doc, 'auth', authobj)
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
        resp = yield from self.resource.post('_replicate', auth=auth, data=doc)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def restart(self, *, auth=None):
        """:ref:`Restarts <api/server/restart>` server instance.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_restart', auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @property
    def session(self):
        """Proxy to the related :class:`~aiocouchdb.server.Session` instance."""
        return self._session

    @asyncio.coroutine
    def stats(self, metric=None, *, auth=None, flush=None, range=None):
        """Returns :ref:`server statistics <api/server/stats>`.

        :param str metric: Metrics name in format ``group/name`. For instance,
                           ``httpd/requests``. If omitted, all metrics
                           will be returned
        :param bool flush: If ``True``, collects samples right for this request
        :param int range: `Sampling range`_
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Sampling range: http://docs.couchdb.org/en/latest/config/misc.html#stats/samples
        """
        path = ['_stats']
        params = {}
        if metric is not None:
            if '/' in metric:
                path.extend(metric.split('/', 1))
            else:
                raise ValueError('invalid metric name. try "httpd/requests"')
        if flush is not None:
            params['flush'] = flush
        if range is not None:
            params['range'] = range
        resource = self.resource(*path)
        resp = yield from resource.get(auth=auth, params=params)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def uuids(self, *, auth=None, count=None):
        """Returns :ref:`UUIDs <api/server/uuids>` generated on server.

        :param int count: Amount of UUIDs to generate
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: list
        """
        params = {}
        if count is not None:
            params['count'] = count
        resp = yield from self.resource.get('_uuids', auth=auth, params=params)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())['uuids']


class Config(object):
    """Implements :ref:`/_config/* <api/config>` API. Should be used via
    :attr:`server.config <aiocouchdb.server.Server.config>` property."""

    def __init__(self, resource):
        self.resource = resource('_config')

    @asyncio.coroutine
    def get(self, section=None, key=None, *, auth=None):
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
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict or str
        """
        path = []
        if section is not None:
            path.append(section)
        if key is not None:
            assert isinstance(section, str)
            path.append(key)
        resp = yield from self.resource(*path).get(auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def update(self, section, key, value, *, auth=None):
        """Updates specific :ref:`configuration option <api/config/section/key>`
        value and returns the old one back.

        :param str section: Configuration section name
        :param str key: Option name
        :param str value: New option value
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: str
        """
        resp = yield from self.resource(section).put(key, auth=auth, data=value)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def remove(self, section, key, *, auth=None):
        """Removes specific :ref:`configuration option <api/config/section/key>`
        and returns it value back.

        :param string section: Configuration section name
        :param string key: Option name
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: str
        """
        resp = yield from self.resource(section).delete(key, auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())


class Session(object):
    """Implements :ref:`/_session <api/auth/session>` API.  Should be used
    via :attr:`server.session <aiocouchdb.server.Server.session>` property."""

    cookie_auth_provider_class = CookieAuthProvider

    def __init__(self, resource):
        self.resource = resource('_session')

    @asyncio.coroutine
    def open(self, name, password):
        """Opens session for cookie auth provider and returns the auth provider
        back for usage in further requests.

        :param str name: Username
        :param str password: User's password

        :rtype: :class:`aiocouchdb.authn.CookieAuthProvider`
        """
        auth = self.cookie_auth_provider_class()
        doc = {'name': name, 'password': password}
        resp = yield from self.resource.post(auth=auth, data=doc)
        yield from maybe_raise_error(resp)
        yield from resp.read()
        return auth

    @asyncio.coroutine
    def info(self, *, auth=None):
        """Returns information about authenticated user.
        Usable for any :class:`~aiocouchdb.authn.AuthProvider`.

        :rtype: dict
        """
        resp = yield from self.resource.get(auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def close(self, *, auth=None):
        """Closes active cookie session.
        Uses for :class:`aiocouchdb.authn.CookieAuthProvider`."""
        resp = yield from self.resource.delete(auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json())
