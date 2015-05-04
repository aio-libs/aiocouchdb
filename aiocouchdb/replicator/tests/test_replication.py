# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from unittest.mock import Mock, MagicMock

from aiocouchdb.tests import utils
from .. import abc
from .. import records
from .. import replication


class ReplicationTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()

        self.source = MagicMock(name='source', spec=abc.ISourcePeer)
        self.target = MagicMock(name='target', spec=abc.ITargetPeer)
        self.repl = replication.Replication(
            rep_task=records.ReplicationTask('http://localhost:5984/source',
                                             'http://localhost:5984/target'),
            source_peer_class=Mock(),
            target_peer_class=Mock())

    def test_verify_peers(self):
        yield from self.repl.verify_peers(self.source, self.target, False)
        self.assertTrue(self.source.info.called)
        self.assertTrue(self.target.exists.called)
        self.assertFalse(self.target.create.called)
        self.assertTrue(self.target.info.called)

    def test_verify_peers_create_target(self):
        self.target.exists.return_value = self.future(False)

        yield from self.repl.verify_peers(self.source, self.target, True)
        self.assertTrue(self.source.info.called)
        self.assertTrue(self.target.exists.called)
        self.assertTrue(self.target.create.called)
        self.assertTrue(self.target.info.called)
