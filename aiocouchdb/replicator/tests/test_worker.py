# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
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

    def test_update_doc(self):
        yield from self.worker.update_doc(self.target, {}, None)
        self.assertTrue(self.target.update_doc.called)

    def test_update_docs(self):
        self.target.update_docs.return_value = self.future([])
        yield from self.worker.update_docs(self.target, [])
        self.assertTrue(self.target.update_docs.called)

    def test_fetch_doc_open_revs(self):
        @asyncio.coroutine
        def side_effect(docid, open_revs, coro, *args, **kwargs):
            for doc, atts in docsatts:
                yield from coro(doc, atts)
        self.source.open_doc_revs.side_effect = side_effect

        docid = 'docid'
        docsatts = [({'_id': docid, '_rev': '1-ABC'}, None),
                    ({'_id': docid, '_rev': '1-CDE'}, None),
                    ({'_id': docid, '_rev': '1-EDC'}, None)]
        revs = [doc['_rev'] for doc, _ in docsatts]

        batch = yield from self.worker.fetch_doc_open_revs(
            source=self.source,
            target=self.target,
            docid=docid,
            revs=revs,
            possible_ancestors=[])
        self.assertTrue(self.source.open_doc_revs.called)

        args, kwargs = self.source.open_doc_revs.call_args
        self.assertEqual((docid, revs), args[:2])
        self.assertEqual({'atts_since': [], 'latest': True, 'revs': True},
                         kwargs)
        self.assertEqual(batch, list(list(zip(*docsatts))[0]))  # oh my...

    def test_fetch_doc_open_revs_with_atts(self):
        @asyncio.coroutine
        def side_effect(docid, open_revs, coro, *args, **kwargs):
            for doc, atts in docsatts:
                yield from coro(doc, atts)
        self.source.open_doc_revs.side_effect = side_effect

        docid = 'docid'
        docsatts = [({'_id': docid, '_rev': '1-ABC'}, None),
                    ({'_id': docid, '_rev': '1-CDE'}, object())]
        revs = [doc['_rev'] for doc, _ in docsatts]

        batch = yield from self.worker.fetch_doc_open_revs(
            source=self.source,
            target=self.target,
            docid='docid',
            revs=revs,
            possible_ancestors=[])
        self.assertTrue(self.source.open_doc_revs.called)
        self.assertTrue(self.target.update_doc.called)
        self.assertEqual(batch, [docsatts[0][0]])

    def test_batch_docs_loop(self):
        docs_queue = asyncio.Queue()
        asyncio.async(self.worker.batch_docs_loop(docs_queue, self.target,
                                                  buffer_size=3))
        yield from docs_queue.put([{'_id': 'foo'}])
        self.assertFalse(self.target.update_docs.called)

        yield from docs_queue.put([{'_id': 'bar'}])
        self.assertFalse(self.target.update_docs.called)

        yield from docs_queue.put([{'_id': 'baz'}])
        yield from asyncio.sleep(0.001)  # context switch
        self.assertTrue(self.target.update_docs.called)

    def test_batch_docs_loop_break_with_buffer(self):
        docs_queue = asyncio.Queue()
        asyncio.async(self.worker.batch_docs_loop(docs_queue, self.target,
                                                  buffer_size=3))
        yield from docs_queue.put([{'_id': 'foo'}])
        self.assertFalse(self.target.update_docs.called)

        yield from docs_queue.put(None)
        yield from asyncio.sleep(0.001)  # context switch
        self.assertTrue(self.target.update_docs.called)

    def test_batch_docs_loop_break_without_buffer(self):
        docs_queue = asyncio.Queue()
        asyncio.async(self.worker.batch_docs_loop(docs_queue, self.target,
                                                  buffer_size=3))
        yield from docs_queue.put(None)
        yield from asyncio.sleep(0.001)  # context switch
        self.assertFalse(self.target.update_docs.called)

    def test_readers_loop(self):
        inbox = asyncio.Queue()
        outbox = asyncio.Queue()
        max_conns = 4
        max_items = max_conns * 2
        future = asyncio.Future()
        self.source.open_doc_revs.return_value = future

        readers_loop = asyncio.async(self.worker.readers_loop(
            inbox, outbox, max_conns))

        for _ in range(max_items):
            yield from inbox.put((self.source, self.target, '', [], []))
        yield from inbox.put(None)

        yield from asyncio.sleep(0.01)

        self.assertEqual(max_items - max_conns, inbox.qsize())
        future.set_result(None)

        yield from readers_loop
        self.assertEqual(max_items, outbox.qsize())
