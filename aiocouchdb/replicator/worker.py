# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import binascii
import json
import logging
import os

from collections import defaultdict
from functools import partial

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationStats
from .work_queue import WorkQueue


__all__ = (
    'ReplicationWorker',
)


log = logging.getLogger(__name__)


class ReplicationWorker(object):
    """Replication worker is a base unit that does all the hard work on transfer
    documents from Source peer the Target one.

    :param source: Source peer
    :param target: Target peer
    :param changes_queue: A queue from where new changes events will be fetched
    :param reports_queue: A queue to where worker will send all reports about
                          replication progress
    :param int batch_size: Amount of events to get from `changes_queue`
                           to process
    :param int max_conns: Amount of simultaneous connection to make against
                          peers at the same time
    """

    # couch_replicator_worker actually uses byte sized buffer for remote source,
    # but that's kind of strange and forces to run useless json encoding.
    # We'll stay with the buffer configuration that it uses for local source.
    # However, both are still not configurable.
    docs_buffer_size = 10

    def __init__(self,
                 rep_id: str,
                 source: ISourcePeer,
                 target: ITargetPeer,
                 changes_queue: WorkQueue,
                 reports_queue: WorkQueue, *,
                 batch_size: int,
                 max_conns: int):
        self.source = source
        self.target = target
        self.changes_queue = changes_queue
        self.reports_queue = reports_queue
        self.batch_size = batch_size
        self.max_conns = max_conns
        self._id = binascii.hexlify(os.urandom(4)).decode()
        self._rep_id = rep_id
        self._stats = defaultdict(int)

    @property
    def id(self) -> str:
        """Returns Worker ID."""
        return self._id

    @property
    def rep_id(self) -> str:
        """Returns associated Replication ID."""
        return self._rep_id

    def start(self):
        """Starts Replication worker."""
        return asyncio.async(self.changes_fetch_loop(
            self.changes_queue, self.reports_queue, self.source, self.target,
            batch_size=self.batch_size, max_conns=self.max_conns))

    @asyncio.coroutine
    def changes_fetch_loop(self,
                           changes_queue: WorkQueue,
                           reports_queue: WorkQueue,
                           source: ISourcePeer,
                           target: ITargetPeer, *,
                           batch_size: int,
                           max_conns: int):
        # couch_replicator_worker:queue_fetch_loop/5
        while True:
            seqs_changes = yield from changes_queue.get(batch_size)

            if seqs_changes is changes_queue.CLOSED:
                break

            # Ensure that we report about the highest seq in the batch
            seqs, changes = zip(*sorted(seqs_changes))
            report_seq = seqs[-1]

            log.debug('Received batch of %d sequence(s) from %s to %s',
                      len(changes), seqs[0], seqs[-1],
                      extra={'rep_id': self.rep_id, 'worker_id': self.id})

            self._stats.clear()
            # Notify checkpoints_loop that we start work on the batch
            stats = ReplicationStats().update(**self._stats)
            yield from reports_queue.put((False, report_seq, stats))

            docid_missing = yield from self.find_missing_revs(target, changes)

            if docid_missing:
                log.debug('Found %d missing revs for %d docs',
                          sum(map(len, docid_missing.values())),
                          len(docid_missing),
                          extra={'rep_id': self.rep_id, 'worker_id': self.id})

                yield from self.remote_process_batch(
                    source, target, docid_missing, max_conns=max_conns)

            stats = ReplicationStats().update(**self._stats)
            yield from reports_queue.put((True, report_seq, stats))

    @asyncio.coroutine
    def find_missing_revs(self, target: ITargetPeer, changes: list) -> dict:
        # couch_replicator_worker:find_missing/2
        # Unlike couch_replicator we also remove duplicate revs from diff
        # request which may eventually when the same document with the conflicts
        # had updated multiple times within the same batch slice.
        docid_revs = defaultdict(list)
        seen = set()
        for docinfo in changes:
            docid = docinfo['id']
            for change in docinfo['changes']:
                rev = change['rev']
                if (docid, rev) in seen:
                    continue
                seen.add((docid, rev))
                docid_revs[docid].append(rev)
                self._stats['missing_checked'] += 1

        revs_diff = yield from target.revs_diff(docid_revs)

        docid_missing = {}
        for docid, content in revs_diff.items():
            self._stats['missing_found'] += len(content['missing'])
            docid_missing[docid] = (content['missing'],
                                    content.get('possible_ancestors', []))
        return docid_missing

    @asyncio.coroutine
    def remote_process_batch(self,
                             source: ISourcePeer,
                             target: ITargetPeer,
                             idrevs: dict, *,
                             max_conns: int):
        # couch_replicator_worker:remote_process_batch/2
        # Well, this isn't true remote_process_batch/2 reimplementation since
        # we don't need to provide here any protection from producing long URL
        # as we don't even know if the target will use HTTP protocol.
        #
        # As the side effect, we request all the conflicts from the source in
        # single API call.
        #
        # Protection of possible long URLs should be done on ISource
        # implementation side.

        readers_inbox = asyncio.Queue()
        batch_docs_queue = asyncio.Queue()

        readers_loop_task = asyncio.async(self.readers_loop(
            readers_inbox, batch_docs_queue, max_conns))

        batch_docs_loop_task = asyncio.async(self.batch_docs_loop(
            batch_docs_queue, target, buffer_size=self.docs_buffer_size))

        for docid, (missing, possible_ancestors) in idrevs.items():
            yield from readers_inbox.put((
                source, target, docid, missing, possible_ancestors))

        # We've done for readers
        yield from readers_inbox.put(None)

        # Ensure that all readers are done
        yield from readers_loop_task

        # Ask to flush all the remain in buffer docs
        yield from batch_docs_queue.put(None)

        # Ensure all docs are flushed
        yield from batch_docs_loop_task

    @asyncio.coroutine
    def readers_loop(self,
                     inbox: asyncio.Queue,
                     outbox: asyncio.Queue,
                     max_conns: int):
        # handle_call({fetch_doc, ...}, From, State)

        tasks = set()
        semaphore = asyncio.Semaphore(max_conns)
        lock = self.spawn_lock(semaphore, tasks)
        while tasks:
            done, pending = yield from asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                if not task.exception():
                    continue
                exc = task.exception()
                for task in pending:
                    task.cancel()
                raise exc  # Pop exception out to cause worker death as well

            if lock in done:
                args = yield from inbox.get()
                if args is None:
                    # Force lock discard from tasks.
                    # There is no warranty that done callback will be called
                    # exact right after coroutine is done, so we have a chance
                    # to deadlock because of this branch.
                    tasks.discard(lock)
                    continue
                self.spawn_reader(self.fetch_doc_open_revs, args,
                                  outbox=outbox,
                                  semaphore=semaphore,
                                  tasks=tasks)
                lock = self.spawn_lock(semaphore, tasks)

    def spawn_lock(self, semaphore: asyncio.Semaphore, tasks: set):
        acquire = asyncio.async(semaphore.acquire())
        acquire.add_done_callback(tasks.discard)
        tasks.add(acquire)
        return acquire

    def spawn_reader(self, coro, args, *,
                     outbox: asyncio.Queue,
                     semaphore: asyncio.Semaphore,
                     tasks: set):
        reader = asyncio.async(coro(*args))
        reader.add_done_callback(tasks.discard)
        reader.add_done_callback(lambda _: semaphore.release())
        reader.add_done_callback(partial(
            self.handle_reader_success, outbox=outbox))
        reader.add_done_callback(partial(
            self.handle_reader_error, docid=args[2]))
        tasks.add(reader)
        return reader

    def handle_reader_success(self,
                              reader: asyncio.Future,
                              outbox: asyncio.Queue):
        # handle_info({'EXIT', Pid, normal}, State)
        if reader.exception():
            return
        assert reader.result is not None, 'that should not be'
        self._stats['docs_read'] += 1
        outbox.put_nowait(reader.result())

    def handle_reader_error(self,
                            reader: asyncio.Future,
                            docid: str):
        # handle_info({'EXIT', Pid, Reason}, State)
        if not reader.exception():
            return
        exc = reader.exception()
        log.error('Reader failed to fetch document %s', docid,
                  exc_info=(exc.__class__, exc, exc.__traceback__),
                  extra={'rep_id': self.rep_id, 'worker_id': self.id})

    @asyncio.coroutine
    def fetch_doc_open_revs(self,
                            source: ISourcePeer,
                            target: ITargetPeer,
                            docid: str,
                            revs: list,
                            possible_ancestors: list) -> list:
        # couch_replicator_worker:fetch_doc/4
        acc = []
        remote_doc_handler = partial(self.remote_doc_handler,
                                     acc=acc,
                                     target=target)
        yield from source.open_doc_revs(docid, revs, remote_doc_handler,
                                        atts_since=possible_ancestors,
                                        latest=True,
                                        revs=True)
        return acc

    @asyncio.coroutine
    def remote_doc_handler(self, doc: bytearray, atts, *, acc, target):
        # couch_replicator_worker:remote_doc_handler/2
        if atts is None:
            # remote_doc_handler({ok, #doc{atts = []}}, Acc)
            acc.append(json.loads(doc.decode()))
        else:
            # remote_doc_handler({ok, Doc}, Acc)
            # Immediately flush document with attachments received from a remote
            # source. The data of each attachment is a MultipartReader that
            # starts streaming the attachment data from the remote source,
            # therefore it's convenient to call it ASAP to avoid inactivity
            # timeouts.
            # So our reader turns into writer here.
            yield from self.update_doc(target, doc, atts)

    @asyncio.coroutine
    def batch_docs_loop(self,
                        inbox: asyncio.Queue,
                        target: ITargetPeer, *,
                        buffer_size: int):
        # handle_call({batch_doc, ...}, From, State)
        batch = []
        while True:
            docs = yield from inbox.get()

            if docs is None:
                # Like handle_call({flush, ...}, From, State)
                # but we're already know that all readers are done their work.
                if batch:
                    yield from self.update_docs(target, batch)
                break

            batch.extend(docs)

            if len(batch) >= buffer_size:
                yield from self.update_docs(target, batch)
                batch[:] = []

    @asyncio.coroutine
    def update_doc(self, target: ITargetPeer, doc: bytearray, atts):
        # couch_replicator_worker:flush_doc/2

        if log.isEnabledFor(logging.DEBUG):
            docobj = json.loads(doc.decode())
            log.debug('Flushing doc %s with %d attachment(s) (%f MiB)',
                      docobj['_id'],
                      sum(1 for att in docobj['_attachments'].values()
                          if att.get('follows') is True),
                      sum(att['length']
                          for att in docobj['_attachments'].values()
                          if att.get('follows') is True) / 1024 / 1024,
                      extra={'rep_id': self.rep_id, 'worker_id': self.id})

        err = yield from target.update_doc(doc, atts)
        self._stats['doc_write_failures' if err else 'docs_written'] += 1

    @asyncio.coroutine
    def update_docs(self, target: ITargetPeer, docs: list):
        # couch_replicator_worker:flush_docs/2
        log.debug('Flushing batch of %d docs', len(docs),
                  extra={'rep_id': self.rep_id, 'worker_id': self.id})

        errs = yield from target.update_docs(docs)
        self._stats['docs_written'] += len(docs) - len(errs)
        self._stats['doc_write_failures'] += len(errs)
