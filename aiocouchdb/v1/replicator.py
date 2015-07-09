# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json

from aiocouchdb.errors import Forbidden, Unauthorized
from aiocouchdb.hdrs import CONTENT_LENGTH, CONTENT_TYPE
from aiocouchdb.replicator.abc import ISourcePeer, ITargetPeer

from . import database


class Peer(ISourcePeer, ITargetPeer):
    """Replication Peer API for CouchDB 1.x."""

    max_delay = 300

    def __init__(self, info, *, retries: int, socket_options, timeout: int):
        self.db = database.Database(info.url)
        self.db.resource.session.auth = info.auth
        self.retries = retries
        self.socket_options = socket_options
        self.timeout = timeout

    @asyncio.coroutine
    def retry_if_failed(self, coro, *, expected_errors: tuple=()):
        return (yield from super().retry_if_failed(
            coro, self.retries,
            expected_errors=expected_errors,
            max_delay=self.max_delay,
            timeout=self.timeout))

    @asyncio.coroutine
    def exists(self):
        return (yield from self.retry_if_failed(self.db.exists()))

    @asyncio.coroutine
    def create(self):
        return (yield from self.db.create())

    @asyncio.coroutine
    def info(self):
        return (yield from self.retry_if_failed(self.db.info()))

    @asyncio.coroutine
    def ensure_full_commit(self):
        info = (yield from self.retry_if_failed(self.db.ensure_full_commit()))
        return info['instance_start_time']

    @asyncio.coroutine
    def open_doc_revs(self, docid: str, open_revs: list, callback_coro, *,
                      atts_since: list=None,
                      latest: bool=None,
                      revs: bool=None):
        doc = yield from self.db.doc(docid)
        reader = yield from doc.get_open_revs(*open_revs,
                                              atts_since=atts_since,
                                              latest=latest,
                                              revs=revs)
        while True:
            doc, atts = yield from reader.stream.next(decode_doc=False)
            if doc is None:
                break
            yield from callback_coro(doc, None if atts.at_eof() else atts)
        yield from reader.release()

    @asyncio.coroutine
    def get_filter_function_code(self, filter_name: str):
        if filter_name is None:
            return
        if filter_name.startswith('_'):
            return
        ddoc_name, func_name = filter_name.split('/', 1)
        ddoc = yield from self.db.ddoc(ddoc_name)
        return (yield from ddoc.doc.get())['filters'][func_name]

    @asyncio.coroutine
    def get_replication_log(self, rep_id: str):
        doc = self.db['_local/' + rep_id]
        if (yield from doc.exists()):
            return (yield from self.retry_if_failed(doc.get()))
        return {}

    @asyncio.coroutine
    def update_replication_log(self,
                               rep_id: str,
                               doc: dict, *,
                               rev: str=None):
        docapi = yield from self.db.doc('_local/' + rep_id)
        info = (yield from self.retry_if_failed(docapi.update(doc, rev=rev)))
        return info['rev']

    @asyncio.coroutine
    def revs_diff(self, id_revs):
        return (yield from self.retry_if_failed(self.db.revs_diff(id_revs)))

    @asyncio.coroutine
    def changes(self, changes_queue, *,
                continuous: bool=False,
                doc_ids: list=None,
                filter: str=None,
                query_params: dict=None,
                since=None,
                view: str=None):
        doc_ids = doc_ids or []
        feed = yield from self.db.changes(
            *doc_ids,
            feed='continuous' if continuous else 'normal',
            filter=filter,
            params=query_params,
            since=since,
            style='all_docs',
            view=view)
        while True:
            event = yield from feed.next()
            if event is None:
                yield from changes_queue.put((feed.last_seq, None))
                break
            yield from changes_queue.put((event['seq'], event))

    @asyncio.coroutine
    def update_doc(self, doc: bytearray, atts):
        @asyncio.coroutine
        def stream_multipart(doc, atts):
            yield binboundary + b'\r\nContent-Type: application/json\r\n\r\n'
            yield doc
            while True:
                yield b'\r\n' + binboundary
                att = yield from atts.next()
                if att is None:
                    yield b'--\r\n'
                    break
                for key, value in att.headers.items():
                    yield b'\r\n' + key.encode() + b': ' + value.encode()
                yield b'\r\n\r\n'
                while True:
                    chunk = yield from att.read_chunk()
                    if not chunk:
                        break
                    yield chunk

        docobj = json.loads(doc.decode())
        docapi = self.db[docobj['_id']]
        boundary = atts._get_boundary()
        binboundary = atts._boundary

        docpart_len = sum((
            len(binboundary),
            len(b'\r\nContent-Type: application/json\r\n\r\n'),
            len(doc)
        ))
        attsparts_len = sum(sum((
            len(binboundary) + 4,  # head and tail \r\n
            len('Content-Length: {}\r\n'
                'Content-Type: {}\r\n'
                'Content-Disposition: attachment; filename="{}"\r\n\r\n'.format(
                    info['length'],
                    info['content_type'],
                    name).encode()),
            info['length']))
            for name, info in docobj['_attachments'].items()
            if 'follows' in info)
        tail = len(binboundary) + 6  # head and tail \r\n plus --
        content_length = docpart_len + attsparts_len + tail

        resp = yield from docapi.resource.put(
            data=stream_multipart(doc, atts),
            headers={
                CONTENT_LENGTH: str(content_length),
                CONTENT_TYPE: 'multipart/related; boundary=' + boundary
            },
            params={'new_edits': False}
        )
        try:
            yield from resp.maybe_raise_error()
        except (Forbidden, Unauthorized) as err:
            return err
        else:
            yield from resp.release()

    @asyncio.coroutine
    def update_docs(self, docs):
        resp = yield from self.retry_if_failed(self.db.bulk_docs(
            docs, new_edits=False))
        return [item for item in resp if 'error' in item]
