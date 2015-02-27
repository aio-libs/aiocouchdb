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
from .document import Document
from .views import View


class DesignDocument(object):
    """Implementation of :ref:`CouchDB Design Document API <api/ddoc>`."""

    #: Default :class:`~aiocouchdb.document.Document` instance class.
    document_class = Document
    #: :class:`Views requesting  helper<aiocouchdb.views.Views>`
    view_class = View

    def __init__(self, url_or_resource, *,
                 docid=None,
                 document_class=None,
                 view_class=None):
        if document_class is not None:
            self.document_class = document_class
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        if view_class is not None:
            self.view_class = view_class
        self.resource = url_or_resource
        self._document = self.document_class(self.resource, docid=docid)

    def __getitem__(self, attname):
        return self._document[attname]

    @property
    def id(self):
        """Returns a document id specified in class constructor."""
        return self.doc.id

    @property
    def name(self):
        """Returns a document id specified in class constructor."""
        docid = self.doc.id
        if docid is not None and '/' in docid:
            return docid.split('/', 1)[1]
        return None

    @property
    def doc(self):
        """Returns :class:`~aiocouchdb.designdoc.DesignDocument.document_class`
        instance to operate with design document as with regular CouchDB
        document.

        :rtype: :class:`~aiocouchdb.document.Document`
        """
        return self._document

    @asyncio.coroutine
    def info(self, *, auth=None):
        """:ref:`Returns view index information <api/ddoc/info>`.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        resp = yield from self.resource.get('_info', auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def list(self, list_name, view_name=None, *keys,
             auth=None,
             headers=None,
             data=None,
             params=None,
             format=None,
             att_encoding_info=None,
             attachments=None,
             conflicts=None,
             descending=None,
             endkey=...,
             endkey_docid=None,
             group=None,
             group_level=None,
             include_docs=None,
             inclusive_end=None,
             limit=None,
             reduce=None,
             skip=None,
             stale=None,
             startkey=...,
             startkey_docid=None,
             update_seq=None):
        """Calls a :ref:`list function <api/ddoc/list>` and returns a raw
        response object.

        :param str list_name: List function name
        :param str view_name: View function name

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param dict headers: Additional request headers
        :param data: Request payload
        :param dict params: Additional request query parameters
        :param str format: List function output format

        For other parameters see
        :meth:`aiocouchdb.designdoc.DesignDocument.view` method docstring.

        :rtype: :class:`~aiocouchdb.client.HttpResponse`
        """
        assert headers is None or isinstance(headers, dict)
        assert params is None or isinstance(params, dict)
        assert data is None or isinstance(data, dict)

        view_params = locals()
        for key in ('self', 'list_name', 'view_name', 'auth',
                    'headers', 'data', 'params'):
            view_params.pop(key)

        view_params, data = self.view_class.handle_keys_param(view_params, data)
        view_params = self.view_class.prepare_params(view_params)

        if params is None:
            params = view_params
        else:
            params.update(view_params)

        method = 'GET' if data is None else 'POST'

        path = ['_list', list_name]
        if view_name:
            path.extend(view_name.split('/', 1))
        resp = yield from self.resource(*path).request(method,
                                                       auth=auth,
                                                       data=data,
                                                       params=params,
                                                       headers=headers)
        return resp

    @asyncio.coroutine
    def rewrite(self, *path,
                auth=None, method=None, headers=None, data=None, params=None):
        """Requests :ref:`rewrite <api/ddoc/rewrite>` resource and returns a
        raw response object.

        :param str path: Request path by segments
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param str method: HTTP request method
        :param dict headers: Additional request headers
        :param data: Request payload
        :param dict params: Additional request query parameters

        :rtype: :class:`~aiocouchdb.client.HttpResponse`
        """
        if method is None:
            method = 'GET' if data is None else 'POST'

        resp = yield from self.resource('_rewrite', *path).request(
            method, auth=auth, data=data, params=params, headers=headers)
        return resp

    @asyncio.coroutine
    def show(self, show_name, docid=None, *,
             auth=None, method=None, headers=None, data=None, params=None,
             format=None):
        """Calls a :ref:`show function <api/ddoc/show>` and returns a raw
        response object.

        :param str show_name: Show function name
        :param str docid: Document ID

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param str method: HTTP request method
        :param dict headers: Additional request headers
        :param data: Request payload
        :param dict params: Additional request query parameters
        :param str format: Show function output format

        :rtype: :class:`~aiocouchdb.client.HttpResponse`
        """
        assert headers is None or isinstance(headers, dict)
        assert params is None or isinstance(params, dict)

        if method is None:
            method = 'GET' if data is None else 'POST'

        if format is not None:
            if params is None:
                params = {}
            assert 'format' not in params
            params['format'] = format

        path = ['_show', show_name]
        if docid is not None:
            path.append(docid)
        resp = yield from self.resource(*path).request(method,
                                                       auth=auth,
                                                       data=data,
                                                       params=params,
                                                       headers=headers)
        return resp

    @asyncio.coroutine
    def update(self, update_name, docid=None, *,
               auth=None, method=None, headers=None, data=None, params=None):
        """Calls a :ref:`show function <api/ddoc/update>` and returns a raw
        response object.

        :param str update_name: Update function name
        :param str docid: Document ID

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param str method: HTTP request method
        :param dict headers: Additional request headers
        :param data: Request payload
        :param dict params: Additional request query parameters

        :rtype: :class:`~aiocouchdb.client.HttpResponse`
        """
        assert headers is None or isinstance(headers, dict)
        assert params is None or isinstance(params, dict)

        if method is None:
            method = 'POST' if docid is None else 'PUT'

        path = ['_update', update_name]
        if docid is not None:
            path.append(docid)
        resp = yield from self.resource(*path).request(method,
                                                       auth=auth,
                                                       data=data,
                                                       params=params,
                                                       headers=headers)
        return resp

    @asyncio.coroutine
    def view(self, view_name, *keys,
             auth=None,
             feed_buffer_size=None,
             att_encoding_info=None,
             attachments=None,
             conflicts=None,
             descending=None,
             endkey=...,
             endkey_docid=None,
             group=None,
             group_level=None,
             include_docs=None,
             inclusive_end=None,
             limit=None,
             reduce=None,
             skip=None,
             stale=None,
             startkey=...,
             startkey_docid=None,
             update_seq=None):
        """Queries a :ref:`stored view <api/ddoc/view>` by the name with
        the specified parameters.

        :param str view_name: Name of view stored in the related design document
        :param str keys: List of view index keys to fetch. This method is smart
                         enough to use `GET` or `POST` request depending on
                         amount of ``keys``

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param int feed_buffer_size: Internal buffer size for fetched feed items

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes attachments content into documents.
                                 **Warning**: use with caution!
        :param bool conflicts: Includes conflicts information into documents
        :param bool descending: Return rows in descending by key order
        :param endkey: Stop fetching rows when the specified key is reached
        :param str endkey_docid: Stop fetching rows when the specified
                                 document ID is reached
        :param bool group: Reduces the view result grouping by unique keys
        :param int group_level: Reduces the view result grouping the keys
                                with defined level
        :param str include_docs: Include document body for each row
        :param bool inclusive_end: When ``False``, doesn't includes ``endkey``
                                   in returned rows
        :param int limit: Limits the number of the returned rows by
                          the specified number
        :param bool reduce: Defines is the reduce function needs to be applied
                            or not
        :param int skip: Skips specified number of rows before starting
                         to return the actual result
        :param str stale: Allow to fetch the rows from a stale view, without
                          triggering index update. Supported values: ``ok``
                          and ``update_after``
        :param startkey: Return rows starting with the specified key
        :param str startkey_docid: Return rows starting with the specified
                                   document ID
        :param bool update_seq: Include an ``update_seq`` value into view
                                results header

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        params = locals()
        for key in ('self', 'auth', 'feed_buffer_size', 'view_name'):
            params.pop(key)

        view = self.view_class(self.resource('_view', view_name))
        return (yield from view.request(auth=auth,
                                        feed_buffer_size=feed_buffer_size,
                                        params=params))
