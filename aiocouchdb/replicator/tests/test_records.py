# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import unittest

import aiocouchdb.authn

from .. import records


class PeerInfoTestCase(unittest.TestCase):

    def test_init_with_str(self):
        url, headers, auth = records.PeerInfo('http://localhost')

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.NoAuthProvider)

    def test_is_peer(self):
        peer_info = records.PeerInfo('http://localhost')

        self.assertIsInstance(peer_info, records.PeerInfo)

    def test_init_with_str_extract_credentials(self):
        url, headers, auth = records.PeerInfo('http://foo:bar@localhost')

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.BasicAuthProvider)
        self.assertEqual(('foo', 'bar'), auth.credentials())

    def test_init_with_dict(self):
        url, headers, auth = records.PeerInfo({'url': 'http://localhost'})

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.NoAuthProvider)

    def test_init_with_dict_extract_credentials(self):
        url, headers, auth = records.PeerInfo(
            {'url': 'http://foo:bar@localhost'})

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.BasicAuthProvider)
        self.assertEqual(('foo', 'bar'), auth.credentials())

    def test_init_with_dict_with_basic_auth(self):
        url, headers, auth = records.PeerInfo(
            {'url': 'http://localhost',
             'headers': {'Authorization': 'Basic Zm9vOmJhcg=='}})

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.BasicAuthProvider)
        self.assertEqual(('foo', 'bar'), auth.credentials())

    def test_init_with_dict_with_proxy_auth(self):
        url, headers, auth = records.PeerInfo(
            {'url': 'http://localhost',
             'headers': {'X-Auth-CouchDB-Username': 'root'}})

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.ProxyAuthProvider)

    def test_init_with_dict_with_oauth_auth(self):
        try:
            aiocouchdb.authn.OAuthProvider()
        except ImportError as err:
            raise unittest.SkipTest(str(err))

        url, headers, auth = records.PeerInfo(
            {'url': 'http://localhost',
             'auth': {'oauth': {'consumer_key': 'foo',
                                'consumer_secret': 'bar',
                                'token': 'baz',
                                'token_secret': 'boo'}}})

        self.assertEqual('http://localhost', url)
        self.assertEqual({}, headers)
        self.assertIsInstance(auth, aiocouchdb.authn.OAuthProvider)

    def test_bad_peer(self):
        for typ in (int, float, tuple, list, set, object, complex):
            with self.assertRaises(TypeError):
                records.PeerInfo(typ())

    def test_multiple_auths(self):
        with self.assertRaises(RuntimeError):
            records.PeerInfo(
                {'url': 'http://foo:bar@localhost',
                 'headers': {'Authorization': 'Basic Zm9vOmJhcg=='}})


class ReplicationTaskTestCase(unittest.TestCase):

    def test_new(self):
        task = records.ReplicationTask('foo', 'bar')
        self.check_fields(task)

    def test_cancel_no_id(self):
        with self.assertRaises(ValueError):
            records.ReplicationTask('foo', 'bar', cancel=True)

    def test_filter_doc_ids(self):
        task = records.ReplicationTask('foo', 'bar', doc_ids=['1', '2', '3'])
        self.check_fields(task, doc_ids=['1', '2', '3'], filter='_doc_ids')

    def test_filter_view(self):
        task = records.ReplicationTask('foo', 'bar', view='ddoc/view')
        self.check_fields(task, view='ddoc/view', filter='_view')

    def test_bad_filter(self):
        with self.assertRaises(ValueError):
            records.ReplicationTask('foo', 'bar', filter='bad')

    def test_filter_collision(self):
        with self.assertRaises(ValueError):
            records.ReplicationTask('foo', 'bar',
                                    doc_ids=['1', '2', '3'],
                                    filter='foo/bar')

    def test_filter_collision2(self):
        with self.assertRaises(ValueError):
            records.ReplicationTask('foo', 'bar',
                                    filter='foo/bar',
                                    view='bar/baz')

    def check_fields(self, task, **kwargs):
        for key, value in zip(task._fields, task):
            if key in ('source', 'target'):
                self.assertIsInstance(value, records.PeerInfo, key)
            elif key in kwargs:
                self.assertEqual(value, kwargs[key], key)
            else:
                self.assertIsNone(value, key)
