# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import unittest

from .. import records
from .. import replication_id


class ReplicationIdV3TestCase(unittest.TestCase):

    def test_remote_remote(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'))
        expected = '03e49219ade6020ef20773f5d1c0f7e2'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_trailing_slash(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source/'),
            records.PeerInfo('http://localhost:5984/target'))
        expected = '03e49219ade6020ef20773f5d1c0f7e2'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_continuous(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'),
            continuous=True)
        expected = '03e49219ade6020ef20773f5d1c0f7e2+continuous'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_create_target(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'),
            create_target=True)
        expected = '03e49219ade6020ef20773f5d1c0f7e2+create_target'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_continuous_create_target(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'),
            continuous=True,
            create_target=True)
        expected = '03e49219ade6020ef20773f5d1c0f7e2+continuous+create_target'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_doc_ids(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'),
            doc_ids=['foo', 'bar', 'baz'])
        expected = 'c0da982bc1bf2a3e655aa726c7c462d7'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_filter(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'),
            filter='  function(doc, req){ return true; }  ')
        expected = '9c8a17ecabf3d962ff84edf147090a94'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_filter_query_params(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo('http://localhost:5984/source'),
            records.PeerInfo('http://localhost:5984/target'),
            filter='  function(doc, req){ return true; }',
            query_params=[('thing', '[1, 2, 3]'),
                          ('bool', 'true'),
                          ('num', '42'),
                          ('str', 'hello')])
        expected = '8a4b98acf58243fea4bbb6ad6578673b'
        self.assertEqual(rep_id, expected)

    def test_remote_remote_headers(self):
        rep_id = replication_id.v3(
            'aiocouchdb',
            records.PeerInfo({'url': 'http://localhost:5984/source',
                        'headers': {'X-Foo': 'bar'}}),
            records.PeerInfo('http://localhost:5984/target'))
        expected = 'ec1e0cd61397009a6f794e9ca5a2d725'
        self.assertEqual(rep_id, expected)
