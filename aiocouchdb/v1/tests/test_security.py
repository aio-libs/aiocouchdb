# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#


from . import utils


class DatabaseSecurityTestCase(utils.DatabaseTestCase):

    def test_security_get(self):
        data = {
            'admins': {
                'names': [],
                'roles': []
            },
            'members': {
                'names': [],
                'roles': []
            }
        }

        result = yield from self.db.security.get()
        self.assert_request_called_with('GET', self.db.name, '_security')
        self.assertEqual(data, result)

    def test_security_update(self):
        data = {
            'admins': {
                'names': ['foo'],
                'roles': []
            },
            'members': {
                'names': [],
                'roles': ['bar', 'baz']
            }
        }

        yield from self.db.security.update(admins={'names': ['foo']},
                                           members={'roles': ['bar', 'baz']})
        self.assert_request_called_with('PUT', self.db.name, '_security',
                                        data=data)

    def test_security_update_merge(self):
        yield from self.db.security.update(
            admins={"names": ["foo"], "roles": []},
            members={"names": [], "roles": ["bar", "baz"]})

        with self.response(data=b'''{
            "admins": {
                "names": ["foo"],
                "roles": []
            },
            "members": {
                "names": [],
                "roles": ["bar", "baz"]
            }
        }'''):
            yield from self.db.security.update(admins={'roles': ['zoo']},
                                               members={'names': ['boo']},
                                               merge=True)
            data = {
                'admins': {
                    'names': ['foo'],
                    'roles': ['zoo']
                },
                'members': {
                    'names': ['boo'],
                    'roles': ['bar', 'baz']
                }
            }
            self.assert_request_called_with('PUT', self.db.name, '_security',
                                            data=data)

    def test_security_update_merge_duplicate(self):
        yield from self.db.security.update(
            admins={"names": ["foo"], "roles": []},
            members={"names": [], "roles": ["bar", "baz"]})

        with self.response(data=b'''{
            "admins": {
                "names": ["foo"],
                "roles": []
            },
            "members": {
                "names": [],
                "roles": ["bar", "baz"]
            }
        }'''):
            yield from self.db.security.update(admins={'names': ['foo', 'bar']},
                                               merge=True)
            data = {
                'admins': {
                    'names': ['foo', 'bar'],
                    'roles': []
                },
                'members': {
                    'names': [],
                    'roles': ['bar', 'baz']
                }
            }
            self.assert_request_called_with('PUT', self.db.name, '_security',
                                            data=data)

    def test_security_update_empty_admins(self):
        with self.response(data=b'{}'):
            yield from self.db.security.update_admins()
            data = {
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    'names': [],
                    'roles': []
                }
            }
            self.assert_request_called_with('PUT', self.db.name, '_security',
                                            data=data)

    def test_security_update_some_admins(self):
        with self.response(data=b'{}'):
            yield from self.db.security.update_admins(names=['foo'],
                                                      roles=['bar', 'baz'])
            data = {
                'admins': {
                    'names': ['foo'],
                    'roles': ['bar', 'baz']
                },
                'members': {
                    'names': [],
                    'roles': []
                }
            }
            self.assert_request_called_with('PUT', self.db.name, '_security',
                                            data=data)

    def test_security_update_empty_members(self):
        with self.response(data=b'{}'):
            yield from self.db.security.update_members()
            data = {
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    'names': [],
                    'roles': []
                }
            }
            self.assert_request_called_with('PUT', self.db.name, '_security',
                                            data=data)

    def test_security_update_some_members(self):
        with self.response(data=b'{}'):
            yield from self.db.security.update_members(names=['foo'],
                                                       roles=['bar', 'baz'])
            data = {
                'admins': {
                    'names': [],
                    'roles': []
                },
                'members': {
                    'names': ['foo'],
                    'roles': ['bar', 'baz']
                }
            }
            self.assert_request_called_with('PUT', self.db.name, '_security',
                                            data=data)
