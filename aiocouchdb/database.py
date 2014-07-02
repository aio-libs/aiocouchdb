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
