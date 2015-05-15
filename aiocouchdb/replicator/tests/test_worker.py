# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from unittest.mock import MagicMock

from aiocouchdb.tests import utils
from .. import abc
from .. import worker


class ReplicationWorkerTestCase(utils.TestCase):

    def setUp(self):
        super().setUp()

        self.source = MagicMock(name='source', spec=abc.ISourcePeer)
        self.target = MagicMock(name='target', spec=abc.ITargetPeer)
        self.worker = worker.ReplicationWorker(
            # we don't plan to use .start() here
            source=None,
            target=None,
            reports_queue=None,
            changes_queue=None)

    def test_find_missing_revs(self):
        self.target.revs_diff.return_value = self.future({})

        idrevs = yield from self.worker.find_missing_revs(self.target, [])
        self.assertTrue(self.target.revs_diff.called)
        self.assertEqual({}, idrevs)

    def test_find_missing_revs_fold_changes(self):
        self.target.revs_diff.return_value = self.future({})
        changes = [{'id': 'foo', 'changes': [{'rev': '1-ABC'}]},
                   {'id': 'bar', 'changes': [{'rev': '1-ABC'}]},
                   {'id': 'foo', 'changes': [{'rev': '1-ABC'},
                                             {'rev': '1-CDE'}]}]

        yield from self.worker.find_missing_revs(self.target, changes)
        self.assertEqual({'foo': ['1-ABC', '1-CDE'], 'bar': ['1-ABC']},
                         self.target.revs_diff.call_args[0][0])

    def test_find_missing_revs_fold_revs_diff(self):
        self.target.revs_diff.return_value = self.future({
            'foo': {'missing': ['1-ABC'], 'possible_ancestors': ['1-CDE']},
            'bar': {'missing': ['1-QWE']}
        })

        idrevs = yield from self.worker.find_missing_revs(self.target, [])
        self.assertEqual({'foo': (['1-ABC'], ['1-CDE']),
                          'bar': (['1-QWE'], [])},
                         idrevs)
