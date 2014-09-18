# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.document
import aiocouchdb.designdoc
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class DesignDocTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_ddoc = urljoin(self.url, 'db', '_design', 'ddoc')
        self.ddoc = aiocouchdb.designdoc.DesignDocument(self.url_ddoc)

    def test_init_with_url(self):
        self.assertIsInstance(self.ddoc.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_ddoc)
        ddoc = aiocouchdb.designdoc.DesignDocument(res)
        self.assertIsInstance(ddoc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_ddoc, ddoc.resource.url)

    def test_access_to_document_api(self):
        self.assertIsInstance(self.ddoc.doc, aiocouchdb.document.Document)
        self.assertIsInstance(self.ddoc.document, aiocouchdb.document.Document)

    def test_access_to_custom_document_api(self):
        class CustomDoc(object):
            def __init__(self, resource):
                pass
        ddoc = aiocouchdb.designdoc.DesignDocument('', document_class=CustomDoc)
        self.assertIsInstance(ddoc.doc, CustomDoc)
        self.assertIsInstance(ddoc.document, CustomDoc)
