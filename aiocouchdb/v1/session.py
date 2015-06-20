# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from aiocouchdb.authn import CookieAuthProvider


__all__ = (
    'Session',
)


class Session(object):
    """Implements :ref:`/_session <api/auth/session>` API.  Should be used
    via :attr:`server.session <aiocouchdb.v1.server.Server.session>` property.
    """

    cookie_auth_provider_class = CookieAuthProvider

    def __init__(self, resource):
        self.resource = resource('_session')

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

    @asyncio.coroutine
    def open(self, name, password):
        """Opens session for cookie auth provider and returns the auth provider
        back for usage in further requests.

        :param str name: Username
        :param str password: User's password

        :rtype: :class:`aiocouchdb.authn.CookieAuthProvider`
        """
        auth = self.cookie_auth_provider_class()
        doc = {'name': name, 'password': password}
        resp = yield from self.resource.post(auth=auth, data=doc)
        yield from resp.maybe_raise_error()
        yield from resp.release()
        return auth

    @asyncio.coroutine
    def info(self, *, auth=None):
        """Returns information about authenticated user.
        Usable for any :class:`~aiocouchdb.authn.AuthProvider`.

        :rtype: dict
        """
        resp = yield from self.resource.get(auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def close(self, *, auth=None):
        """Closes active cookie session.
        Uses for :class:`aiocouchdb.authn.CookieAuthProvider`."""
        resp = yield from self.resource.delete(auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())
