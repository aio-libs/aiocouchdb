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
