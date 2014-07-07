# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json

from .client import Resource
from .feeds import (
    ViewFeed,
    ChangesFeed, LongPollChangesFeed,
    ContinuousChangesFeed, EventSourceChangesFeed
)
from .errors import maybe_raise_error


class Database(object):
    """Implementation of :ref:`CouchDB Database API <api/db>`."""

    def __init__(self, url_or_resource):
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource
        self._security = Security(self.resource)

    @asyncio.coroutine
    def exists(self, *, auth=None):
        """Checks if `database exists`_ on server. Assumes success on receiving
        response with `200 OK` status.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _database exists: http://docs.couchdb.org/en/latest/api/database/common.html#head--db
        """
        resp = yield from self.resource.head(auth=auth)
        yield from resp.read(close=True)
        return resp.status == 200

    @asyncio.coroutine
    def info(self, *, auth=None):
        """Returns `database information`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _database information: http://docs.couchdb.org/en/latest/api/database/common.html#get--db
        """
        resp = yield from self.resource.get(auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def create(self, *, auth=None):
        """`Creates a database`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _Creates a database: http://docs.couchdb.org/en/latest/api/database/common.html#put--db
        """
        resp = yield from self.resource.put(auth=auth)
        yield from maybe_raise_error(resp)
        status = yield from resp.json(close=True)
        return status['ok']


    @asyncio.coroutine
    def delete(self, *, auth=None):
        """`Deletes a database`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _Deletes a database: http://docs.couchdb.org/en/latest/api/database/common.html#delete--db
        """
        resp = yield from self.resource.delete(auth=auth)
        yield from maybe_raise_error(resp)
        status = yield from resp.json(close=True)
        return status['ok']

    @asyncio.coroutine
    def all_docs(self, *keys,
                 auth=None,
                 attachments=None,
                 conflicts=None,
                 descending=None,
                 endkey=None,
                 endkey_docid=None,
                 include_docs=None,
                 inclusive_end=None,
                 limit=None,
                 skip=None,
                 stale=None,
                 startkey=None,
                 startkey_docid=None,
                 update_seq=None):
        """Iterates over :ref:`all documents view <api/db/all_docs>`.

        :param str keys: List of document ids to fetch. This method is smart
                         enough to use `GET` or `POST` request depending on
                         amount of ``keys``

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool attachments: Includes attachments content into documents.
                                 **Warning**: use with caution!
        :param bool conflicts: Includes conflicts information into documents
        :param bool descending: Return rows in descending by key order
        :param str endkey: Stop fetching rows when the specified key is reached
        :param str endkey_docid: Stop fetching rows when the specified
                                 document ID is reached
        :param str include_docs: Include document body for each row
        :param bool inclusive_end: When ``False``, doesn't includes ``endkey``
                                   in returned rows
        :param int limit: Limits the number of the returned rows by
                          the specified number
        :param int skip: Skips specified number of rows before starting
                         to return the actual result
        :param str stale: Allow to fetch the rows from a stale view, without
                          triggering index update. Supported values: ``ok``
                          and ``update_after``
        :param str startkey: Return rows starting with the specified key
        :param str startkey_docid: Return rows starting with the specified
                                   document ID
        :param bool update_seq: Include an ``update_seq`` value into view
                                results header

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('attachments', attachments)
        maybe_set_param('conflicts', conflicts)
        maybe_set_param('descending', descending)
        maybe_set_param('endkey', endkey)
        maybe_set_param('endkey_docid', endkey_docid)
        maybe_set_param('include_docs', include_docs)
        maybe_set_param('inclusive_end', inclusive_end)
        maybe_set_param('limit', limit)
        maybe_set_param('skip', skip)
        maybe_set_param('stale', stale)
        maybe_set_param('startkey', startkey)
        maybe_set_param('startkey_docid', startkey_docid)
        maybe_set_param('update_seq', update_seq)

        data = None
        if len(keys) > 2:
            data = {'keys': keys}
            request = self.resource.post
        else:
            maybe_set_param('key', keys[0] if keys else None)
            request = self.resource.get

        # CouchDB requires these params have valid JSON value
        for param in ('key', 'startkey', 'endkey'):
            if param in params:
                params[param] = json.dumps(params[param])

        resp = yield from request('_all_docs', auth=auth, data=data,
                                  params=params)
        yield from maybe_raise_error(resp)
        return ViewFeed(resp)

    def bulk_docs(self, docs, *, auth=None, all_or_nothing=None,
                  new_edits=None):
        """:ref:`Updates multiple documents <api/db/bulk_docs>` using a single
        request.

        :param Iterable docs: Sequence of document objects (:class:`dict`)
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param bool all_or_nothing: Sets the database commit mode to use
            :ref:`all-or-nothing <api/db/bulk_docs/semantics>` semantics
        :param bool new_edits: If `False`, prevents the database from
                               assigning them new revision for updated documents

        :rtype: list
        """
        def chunkify(docs, all_or_nothing):
            # stream docs one by one to reduce footprint from jsonifying all
            # of them in single shot. useful when docs is generator of docs
            if all_or_nothing is True:
                yield b'{"all_or_nothing": true, "docs": ['
            else:
                yield b'{"docs": ['
            idocs = iter(docs)
            yield json.dumps(next(idocs)).encode('utf-8')
            for doc in idocs:
                yield b',' + json.dumps(doc).encode('utf-8')
            yield b']}'
        params = {} if new_edits is None else {'new_edits': new_edits}
        chunks = chunkify(docs, all_or_nothing)
        resp = yield from self.resource.post(
            '_bulk_docs', auth=auth, data=chunks, params=params)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    def changes(self, *doc_ids,
                auth=None,
                att_encoding_info=None,
                attachments=None,
                conflicts=None,
                descending=None,
                feed=None,
                filter=None,
                heartbeat=None,
                include_docs=None,
                limit=None,
                since=None,
                style=None,
                timeout=None,
                view=None):
        """Emits :ref:`database changes events<api/db/changes>`.

        :param str doc_ids: Document IDs to filter for. This method is smart
                            enough to use `GET` or `POST` request depending
                            if any ``doc_ids`` were provided or not and
                            automatically sets ``filter`` param to ``_doc_ids``
                            value.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes the Base64-encoded content of an
                                 attachments in the documents
        :param bool conflicts: Includes conflicts information in the documents
        :param bool descending: Return changes in descending order
        :param str feed: :ref:`Changes feed type <changes>`
        :param str filter: Filter function name
        :param int heartbeat: Period in milliseconds after which an empty
                              line will be sent from server as the result
                              to keep connection alive
        :param bool include_docs: Includes the associated document for each
                                  emitted event
        :param int limit: Limits a number of returned events by the specified
                          value
        :param since: Starts listening changes feed since given
                      `update sequence` value
        :param str style: Changes feed output style: ``all_docs``, ``main_only``
        :param int timeout: Period in milliseconds to await for new changes
                            before close the feed. Works for continuous feeds
        :param str view: View function name which would be used as filter.
                         Implicitly sets ``filter`` param to ``_view`` value

        :rtype: :class:`aiocouchdb.feeds.ChangesFeed`
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('att_encoding_info', att_encoding_info)
        maybe_set_param('attachments', attachments)
        maybe_set_param('conflicts', conflicts)
        maybe_set_param('descending', descending)
        maybe_set_param('feed', feed)
        maybe_set_param('filter', filter)
        maybe_set_param('heartbeat', heartbeat)
        maybe_set_param('include_docs', include_docs)
        maybe_set_param('limit', limit)
        maybe_set_param('since', since)
        maybe_set_param('style', style)
        maybe_set_param('timeout', timeout)
        maybe_set_param('view', view)

        if doc_ids:
            data = {'doc_ids': doc_ids}
            if 'filter' not in params:
                params['filter'] = '_doc_ids'
            else:
                assert params['filter'] == '_doc_ids'
            request = self.resource.post
        else:
            data = None
            request = self.resource.get

        if 'view' in params:
            if 'filter' not in params:
                params['filter'] = '_view'
            else:
                assert params['filter'] == '_view'

        resp = yield from request('_changes', auth=auth, data=data,
                                  params=params)
        yield from maybe_raise_error(resp)

        if feed == 'continuous':
            return ContinuousChangesFeed(resp)
        elif feed == 'eventsource':
            return EventSourceChangesFeed(resp)
        elif feed == 'longpoll':
            return LongPollChangesFeed(resp)
        else:
            return ChangesFeed(resp)

    @asyncio.coroutine
    def compact(self, ddoc_name=None, *, auth=None):
        """Initiates :ref:`database <api/db/compact>`
        or :ref:`view index <api/db/compact/ddoc>` compaction.

        :param str ddoc_name: Design document name. If specified initiates
                              view index compaction instead of database
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        path = ['_compact']
        if ddoc_name is not None:
            path.append(ddoc_name)
        resp = yield from self.resource(*path).post(auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def ensure_full_commit(self, *, auth=None):
        """Ensures that all bits are :ref:`committed on disk
        <api/db/ensure_full_commit>`.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_ensure_full_commit', auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def missing_revs(self, id_revs, *, auth=None):
        """Returns :ref:`document missed revisions <api/db/missing_revs>`
        in the database by given document-revisions mapping.

        :param dict id_revs: Mapping between document ID and list of his
                             revisions to search for.
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_missing_revs',
                                             auth=auth, data=id_revs)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def purge(self, id_revs, *, auth=None):
        """:ref:`Permanently removes specified document revisions
        <api/db/purge>` from the database.

        :param dict id_revs: Mapping between document ID and list of his
                             revisions to purge.
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_purge',
                                             auth=auth, data=id_revs)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def revs_diff(self, id_revs, *, auth=None):
        """Returns :ref:`document revisions difference <api/db/revs_diff>`
        in the database by given document-revisions mapping.

        :param dict id_revs: Mapping between document ID and list of his
                             revisions to compare.
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_revs_diff',
                                             auth=auth, data=id_revs)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @asyncio.coroutine
    def revs_limit(self, count=None, *, auth=None):
        """Returns the :ref:`limit of database revisions <api/db/revs_limit>`
        to store or updates it if ``count`` parameter was specified.

        :param int count: Amount of revisions to store
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: int or dict
        """
        if count is None:
            resp = yield from self.resource.get('_revs_limit', auth=auth)
        else:
            resp = yield from self.resource.put('_revs_limit',
                                                 auth=auth, data=count)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    @property
    def security(self):
        """Proxy to the related :class:`~aiocouchdb.database.Security`
        instance."""
        return self._security

    @asyncio.coroutine
    def temp_view(self, map_fun, red_fun=None, language=None, *,
                  auth=None,
                  att_encoding_info=None,
                  attachments=None,
                  conflicts=None,
                  descending=None,
                  endkey=None,
                  endkey_docid=None,
                  group=None,
                  group_level=None,
                  include_docs=None,
                  inclusive_end=None,
                  keys=None,
                  limit=None,
                  reduce=None,
                  skip=None,
                  stale=None,
                  startkey=None,
                  startkey_docid=None,
                  update_seq=None):
        """Executes :ref:`temporary view <api/db/temp_view>` and returns
        it results according specified parameters.

        :param str map_fun: Map function source code
        :param str red_fun: Reduce function source code
        :param str language: Query server language to process the view

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes attachments content into documents.
                                 **Warning**: use with caution!
        :param bool conflicts: Includes conflicts information into documents
        :param bool descending: Return rows in descending by key order
        :param str endkey: Stop fetching rows when the specified key is reached
        :param str endkey_docid: Stop fetching rows when the specified
                                 document ID is reached
        :param bool group: Reduces the view result grouping by unique keys
        :param int group_level: Reduces the view result grouping the keys
                                with defined level
        :param str include_docs: Include document body for each row
        :param bool inclusive_end: When ``False``, doesn't includes ``endkey``
                                   in returned rows
        :param list keys: List of view keys to fetch
        :param int limit: Limits the number of the returned rows by
                          the specified number
        :param bool reduce: Defines is the reduce function needs to be applied
                            or not
        :param int skip: Skips specified number of rows before starting
                         to return the actual result
        :param str stale: Allow to fetch the rows from a stale view, without
                          triggering index update. Supported values: ``ok``
                          and ``update_after``
        :param str startkey: Return rows starting with the specified key
        :param str startkey_docid: Return rows starting with the specified
                                   document ID
        :param bool update_seq: Include an ``update_seq`` value into view
                                results header

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('att_encoding_info', att_encoding_info)
        maybe_set_param('attachments', attachments)
        maybe_set_param('conflicts', conflicts)
        maybe_set_param('descending', descending)
        maybe_set_param('endkey', endkey)
        maybe_set_param('endkey_docid', endkey_docid)
        maybe_set_param('include_docs', include_docs)
        maybe_set_param('inclusive_end', inclusive_end)
        maybe_set_param('group', group)
        maybe_set_param('group_level', group_level)
        maybe_set_param('keys', keys)
        maybe_set_param('limit', limit)
        maybe_set_param('reduce', reduce)
        maybe_set_param('skip', skip)
        maybe_set_param('stale', stale)
        maybe_set_param('startkey', startkey)
        maybe_set_param('startkey_docid', startkey_docid)
        maybe_set_param('update_seq', update_seq)

        data = {'map': map_fun}
        if red_fun is not None:
            data['reduce'] = red_fun
        if language is not None:
            data['language'] = language

        # CouchDB requires these params have valid JSON value
        for param in ('keys', 'startkey', 'endkey'):
            if param in params:
                params[param] = json.dumps(params[param])

        resp = yield from self.resource.post('_temp_view', auth=auth, data=data,
                                             params=params)
        yield from maybe_raise_error(resp)
        return ViewFeed(resp)

    @asyncio.coroutine
    def view_cleanup(self, *, auth=None):
        """:ref:`Removes outdated views <api/db/view_cleanup>` index files.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.post('_view_cleanup', auth=auth)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))


class Security(object):
    """Provides set of methods to work with :ref:`database security API
    <api/db/security>`. Should be used via :attr:`database.security
    <aiocouchdb.database.Database.security>` property."""

    def __init__(self, resource):
        self.resource = resource('_security')

    @asyncio.coroutine
    def get(self, *, auth=None):
        """`Returns database security object`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Returns database security object: http://docs.couchdb.org/en/latest/api/database/security.html#get--db-_security
        """
        resp = yield from self.resource.get(auth=auth)
        yield from maybe_raise_error(resp)
        secobj = (yield from resp.json(close=True))
        if not secobj:
            secobj = {
                'admins': {
                    'users': [],
                    'roles': []
                },
                'members': {
                    'users': [],
                    'roles': []
                }
            }
        return secobj

    @asyncio.coroutine
    def update(self, *, auth=None, admins=None, members=None, merge=False):
        """`Updates database security object`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param dict admins: Mapping of administrators users/roles
        :param dict members: Mapping of members users/roles
        :param bool merge: Merges admins/members mappings with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict

        .. _Updates database security object: http://docs.couchdb.org/en/latest/api/database/security.html#put--db-_security
        """
        secobj = yield from self.get(auth=auth)
        for role, section in [('admins', admins), ('members', members)]:
            if section is None:
                continue
            if merge:
                for key, group in section.items():
                    items = secobj[role][key]
                    for item in group:
                        if item in items:
                            continue
                        items.append(item)
            else:
                secobj[role].update(section)
        resp = yield from self.resource.put(auth=auth, data=secobj)
        yield from maybe_raise_error(resp)
        return (yield from resp.json(close=True))

    def update_admins(self, *, auth=None, users=None, roles=None, merge=False):
        """Helper for :meth:`~aiocouchdb.database.Security.update` method to
        update only database administrators leaving members as is.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param list users: List of user names
        :param list roles: List of role names
        :param bool merge: Merges user/role lists with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict
        """
        admins = {
            'users': [] if users is None else users,
            'roles': [] if roles is None else roles
        }
        return self.update(auth=auth, admins=admins, merge=merge)

    def update_members(self, *, auth=None, users=None, roles=None, merge=False):
        """Helper for :meth:`~aiocouchdb.database.Security.update` method to
        update only database members leaving administrators as is.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param list users: List of user names
        :param list roles: List of role names
        :param bool merge: Merges user/role lists with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict
        """
        members = {
            'users': [] if users is None else users,
            'roles': [] if roles is None else roles
        }
        return self.update(auth=auth, members=members, merge=merge)
