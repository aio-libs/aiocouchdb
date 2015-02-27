# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from .database import Database
from .document import Document


class UserDocument(Document):
    """Represents user document for the :class:`authentication database
    <aiocouchdb.database.AuthDatabase>`."""

    doc_prefix = 'org.couchdb.user:'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self._docid is None:
            raise ValueError('docid must be specified for User documents.')

    @property
    def name(self):
        """Returns username."""
        return self.id.split(self.doc_prefix, 1)[-1]

    @asyncio.coroutine
    def register(self, password, *, auth=None, **additional_data):
        """Helper method over :meth:`aiocouchdb.document.Document.update`
        to change a user password.

        :param str password: User's password
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        data = {
            '_id': self.id,
            'name': self.name,
            'password': password,
            'roles': [],
            'type': 'user'
        }
        data.update(additional_data)
        return (yield from self.update(data, auth=auth))

    @asyncio.coroutine
    def update_password(self, password, *, auth=None):
        """Helper method over :meth:`aiocouchdb.document.Document.update`
        to change a user password.

        :param str password: New password
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict
        """
        data = yield from self.get(auth=auth)
        data['password'] = password
        return (yield from self.update(data, auth=auth))


class AuthDatabase(Database):
    """Represents system authentication database.
    Used via :attr:`aiocouchdb.server.Server.authdb`."""

    document_class = UserDocument

    def __getitem__(self, docid):
        if docid.startswith('_design/'):
            resource = self.resource(*docid.split('/', 1))
            return self.design_document_class(resource, docid=docid)
        elif docid.startswith(self.document_class.doc_prefix):
            return self.document_class(self.resource(docid), docid=docid)
        else:
            docid = self.document_class.doc_prefix + docid
            return self.document_class(self.resource(docid), docid=docid)
