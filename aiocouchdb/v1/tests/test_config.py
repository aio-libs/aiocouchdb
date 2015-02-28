# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from . import utils


class ServerConfigTestCase(utils.ServerTestCase):

    def test_config(self):
        yield from self.server.config.get()
        self.assert_request_called_with('GET', '_config')

    def test_config_get_section(self):
        yield from self.server.config.get('couchdb')
        self.assert_request_called_with('GET', '_config', 'couchdb')

    def test_config_get_option(self):
        yield from self.server.config.get('couchdb', 'uuid')
        self.assert_request_called_with('GET', '_config', 'couchdb', 'uuid')

    @utils.modify_server('aiocouchdb', 'test', 'relax')
    def test_config_set_option(self):
        with self.response(data=b'"relax!"'):
            result = yield from self.server.config.update(
                'aiocouchdb', 'test', 'passed')
            self.assert_request_called_with(
                'PUT', '_config', 'aiocouchdb', 'test', data='passed')
        self.assertIsInstance(result, str)

    @utils.modify_server('aiocouchdb', 'test', 'passed')
    def test_config_del_option(self):
        with self.response(data=b'"passed"'):
            result = yield from self.server.config.delete('aiocouchdb', 'test')
            self.assert_request_called_with('DELETE',
                                            '_config', 'aiocouchdb', 'test')
        self.assertIsInstance(result, str)

    def test_config_option_exists(self):
        with self.response(status=200):
            result = yield from self.server.config.exists('couchdb', 'uuid')
            self.assert_request_called_with('HEAD', '_config',
                                            'couchdb', 'uuid')
            self.assertTrue(result)

    def test_config_option_not_exists(self):
        with self.response(status=404):
            result = yield from self.server.config.exists('foo', 'bar')
            self.assert_request_called_with('HEAD', '_config', 'foo', 'bar')
            self.assertFalse(result)
