# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import aiocouchdb.authn

from . import utils


class SessionTestCase(utils.ServerTestCase):

    @utils.with_fixed_admin_party('root', 'relax')
    def test_open_session(self, root):
        with self.response(data=b'{"ok": true}',
                           cookies={'AuthSession': 's3cr1t'}):
            auth = yield from self.server.session.open('root', 'relax')
            self.assert_request_called_with('POST', '_session',
                                            data={'name': 'root',
                                                  'password': 'relax'})
        self.assertIsInstance(auth, aiocouchdb.authn.CookieAuthProvider)
        self.assertIn('AuthSession', auth._cookies)

    def test_session_info(self):
        with self.response(data=b'{}'):
            result = yield from self.server.session.info()
            self.assert_request_called_with('GET', '_session')
        self.assertIsInstance(result, dict)

    def test_close_session(self):
        with self.response(data=b'{"ok": true}'):
            result = yield from self.server.session.close()
            self.assert_request_called_with('DELETE', '_session')
        self.assertIsInstance(result, dict)

