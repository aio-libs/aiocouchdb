# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import abc
import base64
import hashlib
import hmac
import http.cookies
from collections import namedtuple

from .hdrs import (
    AUTHORIZATION,
    COOKIE,
    SET_COOKIE,
    X_AUTH_COUCHDB_ROLES,
    X_AUTH_COUCHDB_TOKEN,
    X_AUTH_COUCHDB_USERNAME
)


#: BasicAuth credentials
BasicAuthCredentials = namedtuple('BasicAuthCredentials', [
    'username', 'password'])

#: OAuth credentials
OAuthCredentials = namedtuple('OAuthCredentials', [
    'consumer_key', 'consumer_secret', 'resource_key', 'resource_secret'])

#: ProxyAuth credentials
ProxyAuthCredentials = namedtuple('ProxyAuthCredentials', [
    'username', 'roles', 'secret'])


class AuthProvider(object, metaclass=abc.ABCMeta):
    """Abstract authentication provider class."""

    @abc.abstractmethod
    def reset(self):
        """Resets provider instance to default state."""
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def credentials(self):
        """Returns authentication credentials if any."""
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def set_credentials(self, *args, **kwargs):
        """Sets authentication credentials."""
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def sign(self, url, headers):
        """Applies authentication routines on further request. Mostly used
        to set right `Authorization` header or cookies to pass the challenge.

        :param str url: Request URL
        :param dict headers: Request headers
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def update(self, response):
        """Updates provider routines from the HTTP response data.

        :param response: :class:`aiocouchdb.client.HttpResponse` instance
        """
        raise NotImplementedError  # pragma: no cover


class NoAuthProvider(AuthProvider):
    """Dummy provider to apply no authentication routines."""

    def reset(self):
        pass  # pragma: no cover

    def set_credentials(self):
        pass  # pragma: no cover

    def sign(self, url, headers):
        pass  # pragma: no cover

    def update(self, response):
        pass  # pragma: no cover


class BasicAuthProvider(AuthProvider):
    """Provides authentication via BasicAuth method."""

    _auth_header = None
    _credentials = None

    def __init__(self, name=None, password=None):
        if name or password:
            self.set_credentials(name, password)

    def reset(self):
        """Resets provider instance to default state."""
        self._auth_header = None
        self._credentials = None

    def credentials(self):
        """Returns authentication credentials.

        :rtype: :class:`aiocouchdb.authn.BasicAuthCredentials`
        """
        return self._credentials

    def set_credentials(self, name, password):
        """Sets authentication credentials.

        :param str name: Username
        :param str password: User's password
        """
        if name and password:
            self._credentials = BasicAuthCredentials(name, password)
        elif not name:
            raise ValueError("Basic Auth username is missing")
        elif not password:
            raise ValueError("Basic Auth password is missing")

    def sign(self, url, headers):
        """Adds BasicAuth header to ``headers``.

        :param str url: Request URL
        :param dict headers: Request headers
        """
        if self._auth_header is None:
            if self._credentials is None:
                raise ValueError('Basic Auth credentials was not specified')
            token = base64.b64encode(
                ('%s:%s' % self._credentials).encode('utf8'))
            self._auth_header = 'Basic %s' % (token.strip().decode('utf8'))
        headers[AUTHORIZATION] = self._auth_header

    def update(self, response):
        pass  # pragma: no cover


class CookieAuthProvider(AuthProvider):
    """Provides authentication by cookies."""

    _cookies = None

    def reset(self):
        """Resets provider instance to default state."""
        self._cookies = None

    def credentials(self):
        # Reserved for future use.
        pass  # pragma: no cover

    def set_credentials(self, name, password):
        # Reserved for future use.
        pass  # pragma: no cover

    def sign(self, url, headers):
        """Adds cookies to provided ``headers``. If ``headers`` already
        contains any cookies, they would be merged with instance ones.

        :param str url: Request URL
        :param dict headers: Request headers
        """
        if self._cookies is None:
            return

        cookie = http.cookies.SimpleCookie()
        if COOKIE in headers:
            cookie.load(headers.get(COOKIE, ''))
            del headers[COOKIE]

        for name, value in self._cookies.items():
            if isinstance(value, http.cookies.Morsel):
                # use dict method because SimpleCookie class modifies value
                dict.__setitem__(cookie, name, value)
            else:
                cookie[name] = value

        headers[COOKIE] = cookie.output(header='', sep=';').strip()

    def update(self, response):
        """Updates cookies from the response.

        :param response: :class:`aiocouchdb.client.HttpResponse` instance
        """
        if response.cookies:
            self._cookies = response.cookies


class OAuthProvider(AuthProvider):
    """Provides authentication via OAuth1. Requires ``oauthlib`` package."""

    _credentials = None

    def __init__(self, *, consumer_key=None, consumer_secret=None,
                 resource_key=None, resource_secret=None):
        from oauthlib import oauth1
        self._oauth1 = oauth1
        self.set_credentials(consumer_key=consumer_key,
                             consumer_secret=consumer_secret,
                             resource_key=resource_key,
                             resource_secret=resource_secret)

    def reset(self):
        """Resets provider instance to default state."""
        self._credentials = None

    def credentials(self):
        """Returns OAuth credentials.

        :rtype: :class:`aiocouchdb.authn.OAuthCredentials`
        """
        return self._credentials

    def set_credentials(self, *, consumer_key=None, consumer_secret=None,
                        resource_key=None, resource_secret=None):
        """Sets OAuth credentials. Currently, all keyword arguments are
        required for successful auth.

        :param str consumer_key: Consumer key (consumer token)
        :param str consumer_secret: Consumer secret
        :param str resource_key: Resource key (oauth token)
        :param str resource_secret: Resource secret (oauth token secret)
        """
        creds = (consumer_key, consumer_secret, resource_key, resource_secret)
        if not all(creds):
            return
        self._credentials = OAuthCredentials(*creds)

    def sign(self, url, headers):
        """Adds OAuth1 signature to ``headers``.

        :param str url: Request URL
        :param dict headers: Request headers
        """
        if self._credentials is None:
            raise ValueError('OAuth credentials was not specified')
        client = self._oauth1.Client(
            client_key=self._credentials.consumer_key,
            client_secret=self._credentials.consumer_secret,
            resource_owner_key=self._credentials.resource_key,
            resource_owner_secret=self._credentials.resource_secret,
            signature_type=self._oauth1.SIGNATURE_TYPE_AUTH_HEADER)
        _, oauth_headers, _ = client.sign(url)
        headers[AUTHORIZATION] = oauth_headers['Authorization']

    def update(self, response):
        pass  # pragma: no cover


class ProxyAuthProvider(AuthProvider):
    """Provides CouchDB proxy authentication methods."""

    _credentials = None

    #: Controls the name of header used to specify CouchDB username
    x_auth_username = X_AUTH_COUCHDB_USERNAME
    #: Controls the name of header used to specify list of CouchDB user roles
    x_auth_roles = X_AUTH_COUCHDB_ROLES
    #: Controls the name of header used to provide authentication token
    x_auth_token = X_AUTH_COUCHDB_TOKEN

    def __init__(self, username=None, roles=None, secret=None, *,
                 x_auth_username=None, x_auth_roles=None, x_auth_token=None):
        if x_auth_username is not None:
            self.x_auth_username = x_auth_username
        if x_auth_roles is not None:
            self.x_auth_roles = x_auth_roles
        if x_auth_token is not None:
            self.x_auth_token = x_auth_token

        if username or roles or secret:
            self.set_credentials(username, roles, secret)

    def reset(self):
        """Resets provider instance to default state."""
        self._credentials = None

    def credentials(self):
        """Returns three-element tuple of defined username, roles and secret."""
        return self._credentials

    def set_credentials(self, username, roles=None, secret=None):
        """Sets ProxyAuth credentials.

        :param str username: CouchDB username
        :param list roles: List of username roles
        :param str secret: ProxyAuth secret. Should match the one which defined
                           on target CouchDB server.
        """
        if not username:
            raise ValueError('Proxy Auth username should have non-empty value')
        self._credentials = ProxyAuthCredentials(username, roles, secret)

    def sign(self, url, headers):
        """Adds ProxyAuth credentials to ``headers``.

        :param str url: Request URL
        :param dict headers: Request headers
        """
        creds = self._credentials

        if creds is None or not creds.username:
            raise ValueError('Proxy Auth username is missing')
        else:
            headers[self.x_auth_username] = creds.username

        if creds.roles is not None:
            headers[self.x_auth_roles] = ','.join(creds.roles)

        if creds.secret is not None:
            headers[self.x_auth_token] = hmac.new(
                creds.secret.encode('utf-8'),
                creds.username.encode('utf-8'),
                hashlib.sha1).hexdigest()

    def update(self, response):
        pass  # pragma: no cover
