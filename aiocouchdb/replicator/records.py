# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import base64
from collections import namedtuple, Sequence

from aiocouchdb.authn import (
    NoAuthProvider,
    BasicAuthProvider,
    ProxyAuthProvider,
    OAuthProvider
)
from aiocouchdb.hdrs import (
    AUTHORIZATION,
    X_AUTH_COUCHDB_USERNAME,
    X_AUTH_COUCHDB_ROLES,
    X_AUTH_COUCHDB_TOKEN
)
from aiocouchdb.client import extract_credentials
from aiohttp.multidict import CIMultiDict

__all__ = (
    'PeerInfo',
    'ReplicationTask',
)


class PeerInfo(namedtuple('Peer', ['url', 'headers', 'auth'])):
    """Represents replication peer with it URL, headers to apply on request and
    authentication provider to use."""

    __slots__ = ()

    def __new__(cls, info):
        """Constructs a peer instance from URL string or dict object.

        :raises: :exc:`RuntimeError` in case of invalid configuration
        """

        if isinstance(info, str):
            url, auth = cls._maybe_extract_credentials(info)
            headers = CIMultiDict()
            auth = auth or NoAuthProvider()

        elif isinstance(info, dict):
            url, auth = cls._maybe_extract_credentials(info['url'])

            headers = CIMultiDict(info.get('headers', {}))

            basic_auth = cls._maybe_extract_basic_auth(headers)
            proxy_auth = cls._maybe_extract_proxy_auth(headers)
            oauth_auth = cls._maybe_extract_oauth_auth(info)

            auths = list(filter(None,
                                [auth, basic_auth, proxy_auth, oauth_auth]))
            if len(auths) > 1:
                raise RuntimeError('Authentication conflict: {}'.format(auths))

            auth = auths[0] if auths else NoAuthProvider()

        else:
            raise TypeError('Invalid type of info: {} ; expected str or dict'
                            ''.format(type(info)))

        return super().__new__(cls, url, headers, auth)

    @staticmethod
    def _maybe_extract_credentials(url: str) -> (str, BasicAuthProvider):
        url, credentials = extract_credentials(url)
        auth = BasicAuthProvider(*credentials) if credentials else None
        return url, auth

    @staticmethod
    def _maybe_extract_basic_auth(headers: CIMultiDict) -> BasicAuthProvider:
        if AUTHORIZATION not in headers:
            return

        if not headers[AUTHORIZATION].startswith('Basic '):
            return

        authhdr = headers.pop(AUTHORIZATION)

        token = authhdr.split('Basic ', 1)[-1].encode()
        credentials = base64.b64decode(token).decode().split(':')
        return BasicAuthProvider(*credentials)

    @staticmethod
    def _maybe_extract_proxy_auth(headers: CIMultiDict) -> ProxyAuthProvider:
        if X_AUTH_COUCHDB_USERNAME not in headers:
            return

        user = headers.pop(X_AUTH_COUCHDB_USERNAME)
        roles = headers.pop(X_AUTH_COUCHDB_ROLES, None)
        token = headers.pop(X_AUTH_COUCHDB_TOKEN, None)
        return ProxyAuthProvider(user, roles, token)

    @staticmethod
    def _maybe_extract_oauth_auth(peer: dict) -> OAuthProvider:
        if 'auth' not in peer:
            return

        if 'oauth' not in peer['auth']:
            return

        try:
            return OAuthProvider(
                consumer_key=peer['auth']['oauth']['consumer_key'],
                consumer_secret=peer['auth']['oauth']['consumer_secret'],
                resource_key=peer['auth']['oauth']['token'],
                resource_secret=peer['auth']['oauth']['token_secret'])
        except ImportError:
            return None  # no-cover


class ReplicationTask(namedtuple('ReplicationTask', [
    'source',
    'target',
    'rep_id',
    'cancel',
    'continuous',
    'create_target',
    'doc_ids',
    'filter',
    'proxy',
    'query_params',
    'since_seq',
    'user_ctx',
    'view',
    'checkpoint_interval',
    'connection_timeout',
    'http_connections',
    'retries_per_request',
    'socket_options',
    'use_checkpoints',
    'worker_batch_size',
    'worker_processes'
])):
    """ReplicationTask is a special object that mimics CouchDB Replication
    document in the way to describe replication process task."""

    __slots__ = ()

    def __new__(cls, source, target, *,
                rep_id: str=None,
                cancel: bool=None,
                continuous: bool=None,
                create_target: bool=None,
                doc_ids: Sequence=None,
                filter: str=None,
                proxy: str=None,
                query_params: dict=None,
                since_seq: (int, str)=None,
                user_ctx: dict=None,
                view: str=None,
                checkpoint_interval: int=None,
                connection_timeout: int=None,
                http_connections: int=None,
                retries_per_request: int=None,
                socket_options: str=None,
                use_checkpoints: bool=None,
                worker_batch_size: int=None,
                worker_processes: int=None):
        """Creates a new replication task object based on provided parameters.

        :param source: Source database URL str or dict object
        :param target: Target database URL str or dict object

        :param str rep_id: Replication ID
        :param bool cancel: Whenever need to cancel active replication
        :param bool continuous: Runs continuous replication
        :param bool create_target: Creates target database if it not exists
        :param list doc_ids: List of specific document ids to replicate,
                             requires `filter` argument to be set as
                             ``_doc_ids``
        :param str filter: Filter function name
        :param str proxy: Proxy server URL
        :param dict query_params: Custom query parameters for filter function
        :param since_seq: Start replication from specified sequence number
        :param dict user_ctx: User context object for the local peer auth
        :param str view: View function as filter, requires `filter` argument
                         to be set as ``_view``

        :param int checkpoint_interval: Minimal time in seconds before the
            next checkpoint could be made
        :param int connection_timeout: Peer request connection timeout
            in seconds
        :param int http_connections: Amount of connections to run against
            the Source peer
        :param int retries_per_request: Amount of retries for non-fatal errors
            before give up and crash
        :param socket_options: Custom socket options (not used)
        :param bool use_checkpoints: Allows to record checkpoints if ``True``
        :param int worker_batch_size: Maximum amount of changes feed events
            to process by worker with the single iteration
        :param int worker_processes: Amount of workers to spawn

        :returns: Returns a `Replicator Task` namedtuple instance.
        :rtype: ReplicationTask
        """

        source = PeerInfo(source)
        target = PeerInfo(target)

        params = locals()

        for param, value in params.items():
            if value is None:
                continue

            if param not in ReplicationTask.__new__.__annotations__:
                continue

            typespec = ReplicationTask.__new__.__annotations__[param]
            if isinstance(value, typespec):
                continue

            raise TypeError('{} expected to be {}, got {}'.format(
                param, typespec, type(value)))

        if cancel:
            if rep_id is None:
                raise ValueError('It is not possible to cancel replication'
                                 ' without ID')

        if doc_ids:
            if filter is None:
                params['filter'] = '_doc_ids'
            elif filter != '_doc_ids':
                raise ValueError('If doc_ids argument specified the filter one'
                                 ' should be set as "_doc_ids"')

        if view:
            if filter is None:
                params['filter'] = '_view'
            elif filter != '_view':
                raise ValueError('If view argument specified the filter one'
                                 ' should be set as "_view"')

        if filter and not filter.startswith('_') and '/' not in filter:
            raise ValueError('Invalid filter.'
                             ' Must match `ddocname/filtername` or `_.*`'
                             ' pattern')

        return super().__new__(cls, *(params[key] for key in cls._fields))

    def __repr__(self):
        return '<{}.{}({})>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            ', '.join('='.join((key, str(value)))
                      for key, value in zip(self._fields, self)
                      if value is not None))
