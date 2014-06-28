# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import unittest

import aiocouchdb.server

URL = 'http://localhost:5984'


class ServerFunctionalTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.server = aiocouchdb.server.Server(URL)

    def tearDown(self):
        self.loop.close()

    def test_init_with_url(self):
        self.assertIsInstance(self.server.resource, self.server.resource_class)

    def test_init_with_resource(self):
        res = self.server.resource_class(URL)
        server = aiocouchdb.server.Server(res)
        self.assertIsInstance(server.resource, server.resource_class)
        self.assertEqual(URL, self.server.resource.url)

    def test_info(self):
        result = self.loop.run_until_complete(self.server.info())
        self.assertIn('couchdb', result)
        self.assertIn('version', result)

    def test_active_tasks(self):
        result = self.loop.run_until_complete(self.server.active_tasks())
        self.assertIsInstance(result, list)

    def test_all_dbs(self):
        result = self.loop.run_until_complete(self.server.all_dbs())
        self.assertIsInstance(result, list)


class ServerConfigFunctionalTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.server = aiocouchdb.server.Server(URL)

    def tearDown(self):
        self.loop.close()

    def test_config(self):
        result = self.loop.run_until_complete(self.server.config.get())
        self.assertIsInstance(result, dict)
        self.assertIn('couchdb', result)

    def test_config_get_section(self):
        result = self.loop.run_until_complete(
            self.server.config.get('couchdb'))
        self.assertIsInstance(result, dict)
        self.assertIn('uuid', result)

    def test_config_get_option(self):
        result = self.loop.run_until_complete(
            self.server.config.get('couchdb', 'uuid'))
        self.assertIsInstance(result, str)

    def test_config_set_option(self):
        result = self.loop.run_until_complete(
            self.server.config.update('test', 'aiocouchdb', 'passed'))
        self.assertEqual('', result)

    def test_config_del_option(self):
        self.loop.run_until_complete(
            self.server.config.update('test', 'aiocouchdb', 'passed'))
        result = self.loop.run_until_complete(
            self.server.config.remove('test', 'aiocouchdb'))
        self.assertEqual('passed', result)
