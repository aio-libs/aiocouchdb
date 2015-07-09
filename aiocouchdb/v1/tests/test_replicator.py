# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

import aiocouchdb.replicator
import aiocouchdb.replicator.log as log
from . import utils
from .. import replicator


log.activate_debug_logging()


class ReplicatorTestCase(utils.ServerTestCase):

    timeout = 60

    @asyncio.coroutine
    def assert_all_docs_replicated(self, source, target, *docs):
        if docs:
            for idx, rev in docs:
                assert (yield from target[idx].exists(rev=rev)), (idx, rev)
        else:
            src_info = yield from source.info()
            tgt_info = yield from target.info()
            self.assertEqual(src_info['doc_count'], tgt_info['doc_count'])

    @asyncio.coroutine
    def assert_checkpoint_is_made(self, rep_id, source, target):
        update_seq = (yield from source.info())['update_seq']
        source_checkpoint = source['_local/' + rep_id]
        target_checkpoint = target['_local/' + rep_id]

        assert (yield from source_checkpoint.exists())
        assert (yield from target_checkpoint.exists())

        source_checkpoint_doc = (yield from source_checkpoint.get())
        target_checkpoint_doc = (yield from target_checkpoint.get())

        self.assertEqual(source_checkpoint_doc['source_last_seq'],
                         target_checkpoint_doc['source_last_seq'])

        self.assertEqual(source_checkpoint_doc['source_last_seq'], update_seq)

    @asyncio.coroutine
    def assert_stats(self, rep_stats, *, missing_checked, missing_found,
                     docs_read, docs_written, doc_write_failures):
        self.assertEqual(doc_write_failures, rep_stats.doc_write_failures)
        self.assertEqual(docs_read, rep_stats.docs_read)
        self.assertEqual(docs_written, rep_stats.docs_written)
        self.assertEqual(missing_checked, rep_stats.missing_checked)
        self.assertEqual(missing_found, rep_stats.missing_found)

    @asyncio.coroutine
    def wait_for_checkpoint(self, checkpoint_interval):
        yield from asyncio.sleep(checkpoint_interval + 2)

    @utils.using_database('source')
    @utils.using_database('target')
    @utils.skip_for('mock')
    def test_simple_replication(self, source, target):
        docs_count = 100
        yield from utils.populate_database(source, docs_count)
        rep_task = aiocouchdb.replicator.ReplicationTask(
            source=source.resource.url,
            target=target.resource.url
        )
        rep = aiocouchdb.replicator.Replication(
            'aiocouchdb', rep_task, replicator.Peer, replicator.Peer)

        state = yield from (yield from rep.start())

        yield from self.assert_all_docs_replicated(source, target)
        yield from self.assert_checkpoint_is_made(state.rep_id, source, target)
        self.assert_stats(state.stats,
                          docs_read=docs_count,
                          docs_written=docs_count,
                          missing_checked=docs_count,
                          missing_found=docs_count,
                          doc_write_failures=0)

    @utils.using_database('source')
    @utils.using_database('target')
    @utils.skip_for('mock')
    def test_incremental_replication(self, source, target):
        checkpoint_interval = 1
        rep_task = aiocouchdb.replicator.ReplicationTask(
            source=source.resource.url,
            target=target.resource.url,
            checkpoint_interval=checkpoint_interval,
            continuous=True,
        )
        rep = aiocouchdb.replicator.Replication(
            'aiocouchdb', rep_task, replicator.Peer, replicator.Peer)
        yield from rep.start()

        doc_a = yield from source.doc('doc1')
        idx_1 = doc_a.id
        rev_1 = (yield from doc_a.update({'name': 'A'}))['rev']

        yield from self.wait_for_checkpoint(checkpoint_interval)
        yield from self.assert_checkpoint_is_made(rep.id, source, target)
        yield from self.assert_all_docs_replicated(
            source, target, (idx_1, rev_1))
        self.assert_stats(rep.state.stats,
                          docs_read=1,
                          docs_written=1,
                          missing_checked=1,
                          missing_found=1,
                          doc_write_failures=0)

        yield from source.bulk_docs([
            {'_id': 'doc2', '_rev': '3-ABC'},
            {'_id': 'doc2', '_rev': '2-CDE'},
            {'_id': 'doc2', '_rev': '2-QWE'}
        ], new_edits=False)

        yield from self.wait_for_checkpoint(checkpoint_interval)
        yield from self.assert_checkpoint_is_made(rep.id, source, target)
        self.assert_stats(rep.state.stats,
                          docs_read=2,
                          docs_written=2,
                          missing_checked=5,
                          missing_found=5,
                          doc_write_failures=0)
        yield from self.assert_all_docs_replicated(
            source, target,
            ('doc2', '2-QWE'),
            ('doc2', '3-ABC'),
            ('doc2', '2-CDE'))

        yield from target.bulk_docs([{'_id': 'doc3'}])
        rev_3 = (yield from source['doc3'].update({}))['rev']

        yield from self.wait_for_checkpoint(checkpoint_interval)
        yield from self.assert_checkpoint_is_made(rep.id, source, target)
        self.assert_stats(rep.state.stats,
                          docs_read=2,
                          docs_written=2,
                          missing_checked=5,
                          missing_found=5,
                          doc_write_failures=0)
        yield from self.assert_all_docs_replicated(
            source, target, ('doc3', rev_3))

    @utils.using_database('source')
    @utils.using_database('target')
    @utils.skip_for('mock')
    def test_attachment_replication(self, source, target):
        checkpoint_interval = 1
        rep_task = aiocouchdb.replicator.ReplicationTask(
            source=source.resource.url,
            target=target.resource.url,
            checkpoint_interval=checkpoint_interval,
            continuous=True,
        )
        rep = aiocouchdb.replicator.Replication(
            'aiocouchdb', rep_task, replicator.Peer, replicator.Peer)
        yield from rep.start()

        doc_a = yield from source.doc('doc1')
        idx_1 = doc_a.id
        rev_1 = (yield from doc_a.update(
            {}, atts={'тест': b'passed', 'passed': 'тест'.encode()}))['rev']

        yield from self.wait_for_checkpoint(checkpoint_interval)
        yield from self.assert_checkpoint_is_made(rep.id, source, target)
        yield from self.assert_all_docs_replicated(
            source, target, (idx_1, rev_1))
        self.assert_stats(rep.state.stats,
                          docs_read=1,
                          docs_written=1,
                          missing_checked=1,
                          missing_found=1,
                          doc_write_failures=0)
