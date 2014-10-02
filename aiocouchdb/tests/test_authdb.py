# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.authdb

from . import utils
from aiocouchdb.client import urljoin


class AuthDatabaseTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        self.url_db = urljoin(self.url, '_users')
        self.db = aiocouchdb.authdb.AuthDatabase(self.url_db)

    def test_get_doc_with_prefix(self):
        doc = self.db['test']
        self.assertEqual(doc.id, self.db.document_class.doc_prefix + 'test')

        doc = self.db[self.db.document_class.doc_prefix + 'test']
        self.assertEqual(doc.id, self.db.document_class.doc_prefix + 'test')


class UserDocumentTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()
        docid = aiocouchdb.authdb.UserDocument.doc_prefix + 'username'
        self.url_doc = urljoin(self.url, '_users', docid)
        self.doc = aiocouchdb.authdb.UserDocument(self.url_doc, docid=docid)

    def test_require_docid(self):
        with self.assertRaises(ValueError):
            aiocouchdb.authdb.UserDocument(self.url_doc)

    def test_username(self):
        self.assertEqual(self.doc.name, 'username')

    def test_register(self):
        self.run_loop(self.doc.register('s3cr1t'))
        self.assert_request_called_with(
            'PUT', '_users', self.doc.id,
            data={
                '_id': self.doc.id,
                'name': self.doc.name,
                'password': 's3cr1t',
                'roles': [],
                'type': 'user'
            })

    def test_register_with_additional_data(self):
        self.run_loop(self.doc.register('s3cr1t', email='user@example.com'))
        self.assert_request_called_with(
            'PUT', '_users', self.doc.id,
            data={
                '_id': self.doc.id,
                'email': 'user@example.com',
                'name': self.doc.name,
                'password': 's3cr1t',
                'roles': [],
                'type': 'user'
            })

    def test_change_password(self):
        self.mock_json_response(data=b'{"existed": "field"}')

        self.run_loop(self.doc.update_password('s3cr1t'))
        self.assert_request_called_with(
            'PUT', '_users', self.doc.id,
            data={
                'existed': 'field',
                'password': 's3cr1t'
            })

