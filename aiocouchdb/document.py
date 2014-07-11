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


class Document(object):
    """Implementation of :ref:`CouchDB Document API <api/doc>`."""

    def __init__(self, url_or_resource):
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource

    @asyncio.coroutine
    def exists(self, rev=None, *, auth=None):
        """Checks if `document exists`_ in the database. Assumes success
        on receiving response with `200 OK` status.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _document exists: http://docs.couchdb.org/en/latest/api/document/common.html#head--db-docid
        """
        resp = yield from self.resource.head(auth=auth, params={'rev': rev})
        yield from resp.read()
        return resp.status == 200

    @asyncio.coroutine
    def modified(self, rev, *, auth=None):
        """Checks if `document was modified`_ in database since specified
        revision.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool

        .. _document was modified: http://docs.couchdb.org/en/latest/api/document/common.html#head--db-docid
        """
        qrev = '"%s"' % rev
        resp = yield from self.resource.head(auth=auth,
                                             headers={'IF-NONE-MATCH': qrev})
        yield from resp.maybe_raise_error()
        return resp.status != 304
