# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import http.cookies
import unittest
import unittest.mock as mock

import aiocouchdb.authn
import aiocouchdb.client

URL = 'http://localhost:5984'


class BasicAuthProviderTestCase(unittest.TestCase):

    def setUp(self):
        self.auth = aiocouchdb.authn.BasicAuthProvider()

    def test_set_credentials(self):
        self.auth.set_credentials('foo', 'bar')
        self.assertIsInstance(self.auth.credentials(),
                              aiocouchdb.authn.BasicAuthCredentials)
        self.assertEqual(('foo', 'bar'), self.auth.credentials())

    def test_missed_username(self):
        self.assertRaises(ValueError, self.auth.set_credentials, '', 'password')

    def test_missed_password(self):
        self.assertRaises(ValueError, self.auth.set_credentials, 'name', '')

    def test_require_credentials_to_set_auth_header(self):
        self.assertRaises(ValueError, self.auth.sign, URL, {})

    def test_set_auth_header(self):
        self.auth.set_credentials('foo', 'bar')
        headers = {}
        self.auth.sign(URL, headers)
        self.assertIn('AUTHORIZATION', headers)
        self.assertTrue(headers['AUTHORIZATION'].startswith('Basic'))

    def test_reset_credentials(self):
        self.auth.set_credentials('foo', 'bar')
        self.auth.reset()
        self.assertIsNone(self.auth.credentials())

    def test_cache_auth_header(self):
        self.auth.set_credentials('foo', 'bar')
        self.auth.sign(URL, {})
        self.auth._credentials = None

        headers = {}
        self.auth.sign(URL, headers)
        self.assertIn('AUTHORIZATION', headers)


class CookieAuthProviderTestCase(unittest.TestCase):

    def setUp(self):
        self.auth = aiocouchdb.authn.CookieAuthProvider()
        self.resp = mock.Mock(spec=aiocouchdb.client.HttpResponse)
        self.resp.cookies = http.cookies.SimpleCookie({'AuthSession': 'secret'})

    def test_update_cookies_from_response(self):
        self.assertIsNone(self.auth._cookies)
        self.auth.update(self.resp)
        self.assertIs(self.auth._cookies, self.resp.cookies)

    def test_reset(self):
        self.auth.update(self.resp)
        self.auth.reset()
        self.assertIsNone(self.auth._cookies)

    def test_set_no_cookies(self):
        headers = {}
        self.auth.sign(URL, headers)
        self.assertNotIn('COOKIE', headers)

    def test_set_cookies(self):
        self.auth.update(self.resp)
        headers = {}
        self.auth.sign(URL, headers)
        self.assertIn('COOKIE', headers)

    def test_merge_cookies_on_apply(self):
        self.auth.update(self.resp)
        headers = {'COOKIE': 'AuthSession=s3kr1t'}
        self.auth.sign(URL, headers)
        self.assertIn('COOKIE', headers)
        self.assertEqual('AuthSession=secret', headers['COOKIE'])


class OAuthProviderTestCase(unittest.TestCase):

    def setUp(self):
        try:
            self.auth = aiocouchdb.authn.OAuthProvider()
        except ImportError as exc:
            raise unittest.SkipTest(exc)

    def test_set_credentials(self):
        self.assertIsNone(self.auth.credentials())
        self.auth.set_credentials(consumer_key='foo', consumer_secret='bar',
                                  resource_key='baz', resource_secret='boo')
        self.assertIsInstance(self.auth.credentials(),
                              aiocouchdb.authn.OAuthCredentials)
        self.assertEqual(('foo', 'bar', 'baz', 'boo'), self.auth.credentials())

    def test_require_credentials_to_set_oauth_header(self):
        self.assertRaises(ValueError, self.auth.sign, URL, {})

    def test_set_oauth_header(self):
        self.auth.set_credentials(consumer_key='foo', consumer_secret='bar',
                                  resource_key='baz', resource_secret='boo')
        headers = {}
        self.auth.sign(URL, headers)
        self.assertIn('AUTHORIZATION', headers)
        self.assertTrue(headers['AUTHORIZATION'].startswith('OAuth'))

    def test_reset_credentials(self):
        self.auth.set_credentials(consumer_key='foo', consumer_secret='bar',
                                  resource_key='baz', resource_secret='boo')
        self.auth.reset()
        self.assertIsNone(self.auth.credentials())
