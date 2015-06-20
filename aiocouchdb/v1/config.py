# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio


__all__ = (
    'ServerConfig',
)


class ServerConfig(object):
    """Implements :ref:`/_config/* <api/config>` API. Should be used via
    :attr:`server.config <aiocouchdb.v1.server.Server.config>` property."""

    def __init__(self, resource):
        self.resource = resource('_config')

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

    @asyncio.coroutine
    def exists(self, section, key, *, auth=None):
        """Checks if :ref:`configuration option <api/config/section/key>`
        exists.

        :param str section: Section name
        :param str key: Option name
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: bool
        """
        resp = yield from self.resource(section, key).head(auth=auth)
        yield from resp.read()
        return resp.status == 200

    @asyncio.coroutine
    def get(self, section=None, key=None, *, auth=None):
        """Returns :ref:`server configuration <api/config>`. Depending on
        specified arguments returns:

        - :ref:`Complete configuration <api/config>` if ``section`` and ``key``
          are ``None``

        - :ref:`Section options <api/config/section>` if ``section``
          was specified

        - :ref:`Option value <api/config/section/key>` if both ``section``
          and ``key`` were specified

        :param str section: Section name (`optional`)
        :param str key: Option name (`optional`)
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict or str
        """
        path = []
        if section is not None:
            path.append(section)
        if key is not None:
            assert isinstance(section, str)
            path.append(key)
        resp = yield from self.resource(*path).get(auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def update(self, section, key, value, *, auth=None):
        """Updates specific :ref:`configuration option <api/config/section/key>`
        value and returns the old one back.

        :param str section: Configuration section name
        :param str key: Option name
        :param str value: New option value
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: str
        """
        resp = yield from self.resource(section).put(key, auth=auth, data=value)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    @asyncio.coroutine
    def delete(self, section, key, *, auth=None):
        """Deletes specific :ref:`configuration option <api/config/section/key>`
        and returns it value back.

        :param string section: Configuration section name
        :param string key: Option name
        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: str
        """
        resp = yield from self.resource(section).delete(key, auth=auth)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())
