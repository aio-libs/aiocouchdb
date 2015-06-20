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
    'DatabaseSecurity',
)


class DatabaseSecurity(object):
    """Provides set of methods to work with :ref:`database security API
    <api/db/security>`. Should be used via :attr:`database.security
    <aiocouchdb.v1.database.Database.security>` property."""

    def __init__(self, resource):
        self.resource = resource('_security')

    def __repr__(self):
        return '<{}.{}({}) object at {}>'.format(
            self.__module__,
            self.__class__.__qualname__,  # pylint: disable=no-member
            self.resource.url,
            hex(id(self)))

    @asyncio.coroutine
    def get(self, *, auth=None):
        """`Returns database security object`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance

        :rtype: dict

        .. _Returns database security object: http://docs.couchdb.org/en/latest/api/database/security.html#get--db-_security
        """
        resp = yield from self.resource.get(auth=auth)
        yield from resp.maybe_raise_error()
        secobj = (yield from resp.json())
        if not secobj:
            secobj = {
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    'names': [],
                    'roles': []
                }
            }
        return secobj

    @asyncio.coroutine
    def update(self, *, auth=None, admins=None, members=None, merge=False):
        """`Updates database security object`_.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param dict admins: Mapping of administrators users/roles
        :param dict members: Mapping of members users/roles
        :param bool merge: Merges admins/members mappings with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict

        .. _Updates database security object: http://docs.couchdb.org/en/latest/api/database/security.html#put--db-_security
        """
        secobj = yield from self.get(auth=auth)
        for role, section in [('admins', admins), ('members', members)]:
            if section is None:
                continue
            if merge:
                for key, group in section.items():
                    items = secobj[role][key]
                    for item in group:
                        if item in items:
                            continue
                        items.append(item)
            else:
                secobj[role].update(section)
        resp = yield from self.resource.put(auth=auth, data=secobj)
        yield from resp.maybe_raise_error()
        return (yield from resp.json())

    def update_admins(self, *, auth=None, names=None, roles=None, merge=False):
        """Helper for :meth:`~aiocouchdb.v1.database.Security.update` method to
        update only database administrators leaving members as is.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param list names: List of user names
        :param list roles: List of role names
        :param bool merge: Merges user/role lists with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict
        """
        admins = {
            'names': [] if names is None else names,
            'roles': [] if roles is None else roles
        }
        return self.update(auth=auth, admins=admins, merge=merge)

    def update_members(self, *, auth=None, names=None, roles=None, merge=False):
        """Helper for :meth:`~aiocouchdb.v1.database.Security.update` method to
        update only database members leaving administrators as is.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param list names: List of user names
        :param list roles: List of role names
        :param bool merge: Merges user/role lists with existed ones when
                           is ``True``, otherwise replaces them with the given

        :rtype: dict
        """
        members = {
            'names': [] if names is None else names,
            'roles': [] if roles is None else roles
        }
        return self.update(auth=auth, members=members, merge=merge)
