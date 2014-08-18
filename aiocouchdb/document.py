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
import uuid

from aiohttp.multidict import CaseInsensitiveMultiDict
from collections.abc import Mapping
from .attachment import Attachment
from .client import Resource, HttpStreamResponse
from .multipart import MultipartBodyReader


class Document(object):
    """Implementation of :ref:`CouchDB Document API <api/doc>`."""

    attachment_class = Attachment

    def __init__(self, url_or_resource, *, attachment_class=None):
        if attachment_class is not None:
            self.attachment_class = attachment_class
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource

    @property
    def id(self):
        """Returns associated document ID."""
        return self.resource.url.rsplit('/', 1)[-1]

    @asyncio.coroutine
    def attachment(self, attname, *, auth=None):
        """Returns :class:`~aiocouchdb.attachment.Attachment` instance against
        specified attachment.

        If attachment isn't accessible for auth provided credentials,
        this method raises :exc:`aiocouchdb.errors.HttpErrorException`
        with the related response status code.

        :param str attname: Attachment name
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: :attr:`aiocouchdb.document.Document.attachment_class`
        """
        att_resource = self.resource(*attname.split('/'))
        resp = yield from att_resource.head(auth=auth)
        if resp.status != 404:
            yield from resp.maybe_raise_error()
        yield from resp.read()
        return self.attachment_class(att_resource)

    #: alias for :meth:`aiocouchdb.document.Document.attachment`
    att = attachment

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

    @asyncio.coroutine
    def get(self, rev=None, *,
            auth=None,
            att_encoding_info=None,
            attachments=None,
            atts_since=None,
            conflicts=None,
            deleted_conflicts=None,
            local_seq=None,
            meta=None,
            open_revs=None,
            revs=None,
            revs_info=None):
        """`Returns a document`_ object.

        :param str rev: Document revision

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param bool attachments: Includes the Base64-encoded content of an
                                 attachments in the documents
        :param list atts_since: Includes attachments that was added since
                                the specified revisions
        :param bool conflicts: Includes conflicts information in the documents
        :param bool deleted_conflicts: Includes information about deleted
                                       conflicted revisions in the document
        :param bool local_seq: Includes local sequence number in the document
        :param bool meta: Includes meta information in the document.
        :param list open_revs: Returns the specified leaf revisions.
        :param bool revs: Includes information about all known revisions
        :param bool revs_info: Includes information about all known revisions
                               and their status

        :rtype: dict or list if `open_revs` specified

        .. _Returns a document: http://docs.couchdb.org/en/latest/api/document/common.html#get--db-docid
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('att_encoding_info', att_encoding_info)
        maybe_set_param('attachments', attachments)
        maybe_set_param('atts_since', atts_since)
        maybe_set_param('conflicts', conflicts)
        maybe_set_param('deleted_conflicts', deleted_conflicts)
        maybe_set_param('local_seq', local_seq)
        maybe_set_param('meta', meta)
        maybe_set_param('open_revs', open_revs)
        maybe_set_param('rev', rev)
        maybe_set_param('revs', revs)
        maybe_set_param('revs_info', revs_info)

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        if open_revs is not None and open_revs != 'all':
            params['open_revs'] = json.dumps(open_revs)

        resp = yield from self.resource.get(auth=auth, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def get_open_revs(self, *open_revs,
                      auth=None,
                      att_encoding_info=None,
                      atts_since=None,
                      local_seq=None,
                      revs=None):
        """Returns document open revisions with their attachments.

        Unlike :func:`get(open_revs=[...]) <aiocouchdb.document.Document.get>`,
        this method works with :mimetype:`multipart/mixed` response returning
        multipart reader which is more optimized to handle large data sets with
        lesser memory footprint.

        Note, that this method always returns attachments along with leaf
        revisions.

        :param list open_revs: Leaf revisions to return. If omitted, all leaf
                               revisions will be returned

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachments stubs
        :param list atts_since: Includes attachments that was added since
                                the specified revisions
        :param bool local_seq: Includes local sequence number in each document
        :param bool revs: Includes information about all known revisions in
                          each document

        :rtype: :class:`~aiocouchdb.document.OpenRevsMultipartReader`
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('att_encoding_info', att_encoding_info)
        maybe_set_param('atts_since', atts_since)
        maybe_set_param('local_seq', local_seq)
        maybe_set_param('revs', revs)

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        params['open_revs'] = json.dumps(open_revs) if open_revs else 'all'

        resp = yield from self.resource.get(auth=auth,
                                            headers={'ACCEPT': 'multipart/*'},
                                            params=params,
                                            response_class=HttpStreamResponse)
        yield from resp.maybe_raise_error()
        reader = OpenRevsMultipartReader.from_response(resp)
        return reader

    @asyncio.coroutine
    def get_with_atts(self, rev=None, *,
                      auth=None,
                      att_encoding_info=None,
                      atts_since=None,
                      conflicts=None,
                      deleted_conflicts=None,
                      local_seq=None,
                      meta=None,
                      revs=None,
                      revs_info=None):
        """Returns document with attachments.

        This method is more optimal than :func:`get(attachments=true)
        <aiocouchdb.document.Document.get>` since it uses multipart API and
        doesn't requires to read all the attachments, extract then from JSON
        document and decode from base64.

        :param str rev: Document revision

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param bool att_encoding_info: Includes encoding information in an
                                       attachment stubs
        :param list atts_since: Includes attachments that was added since
                                the specified revisions
        :param bool conflicts: Includes conflicts information in the documents
        :param bool deleted_conflicts: Includes information about deleted
                                       conflicted revisions in the document
        :param bool local_seq: Includes local sequence number in the document
        :param bool meta: Includes meta information in the document
        :param bool revs: Includes information about all known revisions
        :param bool revs_info: Includes information about all known revisions
                               and their status

        :rtype: :class:`~aiocouchdb.document.DocAttachmentsMultipartReader`
        """
        params = {'attachments': True}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('att_encoding_info', att_encoding_info)
        maybe_set_param('atts_since', atts_since)
        maybe_set_param('conflicts', conflicts)
        maybe_set_param('deleted_conflicts', deleted_conflicts)
        maybe_set_param('local_seq', local_seq)
        maybe_set_param('meta', meta)
        maybe_set_param('rev', rev)
        maybe_set_param('revs', revs)
        maybe_set_param('revs_info', revs_info)

        if atts_since is not None:
            params['atts_since'] = json.dumps(atts_since)

        resp = yield from self.resource.get(
            auth=auth,
            headers={'ACCEPT': 'multipart/*, application/json'},
            params=params,
            response_class=HttpStreamResponse)

        yield from resp.maybe_raise_error()

        if resp.headers['CONTENT-TYPE'].startswith('application/json'):
            # WARNING! Here be Hacks!
            # If document has no attachments, CouchDB returns it as JSON
            # so we have to fake multipart response in the name of consistency.
            # However, this hack may not lasts for too long.
            data = yield from resp.read()
            boundary = str(uuid.uuid4())
            headers = dict(resp.headers.items())
            headers['CONTENT-TYPE'] = 'multipart/related;boundary=%s' % boundary
            resp.headers = CaseInsensitiveMultiDict(**headers)
            resp.content._buffer.extend(
                b'--' + boundary.encode('latin1') + b'\r\n'
                b'Content-Type: application/json\r\n'
                b'\r\n'
                + data.rstrip() + b'\r\n'
                b'--' + boundary.encode('latin1') + b'--\r\n'
            )

        return DocAttachmentsMultipartReader.from_response(resp)

    @asyncio.coroutine
    def update(self, doc, *, auth=None, batch=None, new_edits=None, rev=None):
        """`Updates a document`_ on server.

        :param dict doc: Document object. Should implement
                        :class:`~collections.abc.Mapping` interface

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :param str batch: Updates in batch mode (asynchronously).
                          This argument accepts only ``"ok"`` value.
        :param bool new_edits: Signs about new document edition. When ``False``
                               allows to create conflicts manually
        :param str rev: Document revision. Optional, since document ``_rev``
                        field is also respected

        :rtype: dict

        .. _Updates a document: http://docs.couchdb.org/en/latest/api/document/common.html#put--db-docid
        """
        params = {}
        maybe_set_param = (
            lambda *kv: (None if kv[1] is None else params.update([kv])))
        maybe_set_param('batch', batch)
        maybe_set_param('new_edits', new_edits)
        maybe_set_param('rev', rev)

        if not isinstance(doc, Mapping):
            raise TypeError('Mapping instance expected, dict - preferred')

        if '_id' in doc and doc['_id'] != self.id:
            raise ValueError('Attempt to store document with different ID: '
                             '%r ; expected: %r. May you want to .copy() it?'
                             % (doc['_id'], self.id))

        resp = yield from self.resource.put(auth=auth, data=doc, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def remove(self, rev, *, auth=None, preserve_content=None):
        """`Deletes a document`_ from server.

        By default document will be deleted using `DELETE` HTTP method.
        On this request CouchDB removes all document fields, leaving only
        system ``_id`` and ``_rev`` and adding ``"_deleted": true`` one. When
        `preserve_content` set to ``True``, document will be marked as deleted
        (by adding ``"_deleted": true`` field without removing existed ones)
        via `PUT` request. This feature costs two requests to fetch and update
        the document and also such documents consumes more space by oblivious
        reasons.

        :param str rev: Document revision
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param bool preserve_content: Whenever to preserve document content
                                      on deletion

        :rtype: dict

        .. _Deletes a document: http://docs.couchdb.org/en/latest/api/document/common.html#delete--db-docid
        """
        params = {'rev': rev}
        if preserve_content:
            doc = yield from self.get(rev=rev)
            doc['_deleted'] = True
            resp = yield from self.resource.put(auth=auth, data=doc,
                                                params=params)
        else:
            resp = yield from self.resource.delete(auth=auth, params=params)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def copy(self, newid, rev=None, *, auth=None):
        """`Copies a document`_ with the new ID within the same database.

        :param str newid: New document ID
        :param str rev: New document ID revision. Used for copying over existed
                        document
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Copies a document: http://docs.couchdb.org/en/latest/api/document/common.html#copy--db-docid
        """
        dest = newid
        if rev is not None:
            dest += '?rev=' + rev
        headers = {'DESTINATION': dest}
        resp = yield from self.resource.copy(auth=auth, headers=headers)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())


class DocAttachmentsMultipartReader(MultipartBodyReader):
    """Special multipart reader optimized for requesting single document with
    attachments. Matches output with :class:`OpenRevsMultipartReader`."""

    @asyncio.coroutine
    def next(self):
        """Emits a tuple of document object (:class:`dict`) and multipart reader
        of the followed attachments (if any).

        :rtype: tuple
        """
        # WARNING! Here be Hacks!
        part = self._last_part
        if part is not None and part.at_eof():
            self._at_eof = True

        reader = yield from super().next()

        if self._at_eof:
            return None, None

        attsreader = MultipartBodyReader(self.headers, self.content)
        self._last_part = attsreader
        attsreader._unread = reader._unread

        doc = yield from reader.json()

        return doc, attsreader


class OpenRevsMultipartReader(MultipartBodyReader):
    """Special multipart reader optimized for reading document`s open revisions
    with attachments."""

    multipart_reader_cls = MultipartBodyReader

    @asyncio.coroutine
    def next(self):
        """Emits a tuple of document object (:class:`dict`) and multipart reader
        of the followed attachments (if any).

        :rtype: tuple
        """
        reader = yield from super().next()

        if self._at_eof:
            return None, None

        if isinstance(reader, self.multipart_reader_cls):
            part = yield from reader.next()
            doc = yield from part.json()
        else:
            doc = yield from reader.json()

        return doc, reader
