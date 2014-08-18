# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.client
import aiocouchdb.attachment
import aiocouchdb.tests.utils as utils
from aiocouchdb.client import urljoin


class AttachmentTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_doc = urljoin(self.url, 'db', 'docid', 'att')
        self.att = aiocouchdb.attachment.Attachment(self.url_doc)

    def test_init_with_url(self):
        self.assertIsInstance(self.att.resource, aiocouchdb.client.Resource)

    def test_init_with_resource(self):
        res = aiocouchdb.client.Resource(self.url_doc)
        doc = aiocouchdb.attachment.Attachment(res)
        self.assertIsInstance(doc.resource, aiocouchdb.client.Resource)
        self.assertEqual(self.url_doc, self.att.resource.url)
