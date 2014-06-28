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
