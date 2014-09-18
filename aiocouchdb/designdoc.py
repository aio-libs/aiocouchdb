# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from .client import Resource
from .document import Document


class DesignDocument(object):
    """Implementation of :ref:`CouchDB Design Document API <api/ddoc>`."""

    #: Default :class:`~aiocouchdb.document.Document` instance class.
    document_class = Document

    def __init__(self, url_or_resource, *, document_class=None):
        if document_class is not None:
            self.document_class = document_class
        if isinstance(url_or_resource, str):
            url_or_resource = Resource(url_or_resource)
        self.resource = url_or_resource
        self._document = self.document_class(self.resource)

    @property
    def document(self):
        """Returns :class:`~aiocouchdb.designdoc.DesignDocument.document_class`
        instance to operate with design document as with regular CouchDB
        document.

        :rtype: :class:`~aiocouchdb.document.Document`
        """
        return self._document

    #: alias for :meth:`aiocouchdb.designdoc.DesignDocument.document`
    doc = document
