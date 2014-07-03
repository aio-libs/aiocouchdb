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
from .feeds import JsonViewFeed
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

        :rtype: :class:`aiocouchdb.feeds.JsonViewFeed`
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
            data = {'keys': list(keys)}
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
        return JsonViewFeed(resp)

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
