# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#


import asyncio
import logging

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationTask


__all__ = (
    'Replication',
)


log = logging.getLogger(__name__)


class Replication(object):
    """Replication job maker."""

    def __init__(self,
                 rep_task: ReplicationTask,
                 source_peer_class,
                 target_peer_class):
        self.rep_task = rep_task
        self.source = source_peer_class(rep_task.source)
        self.target = target_peer_class(rep_task.target)

    @asyncio.coroutine
    def start(self):
        """Starts a replication."""
        # couch_replicator:do_init/1
        # couch_replicator:init_state/1
        rep_task, source, target = self.rep_task, self.source, self.target

        log.info('Starting new replication %s -> %s',
                 rep_task.source.url, rep_task.target.url,
                 extra={'rep_id': None})

        # we'll need source and target info later
        source_info, target_info = yield from self.verify_peers(
            source, target, rep_task.create_target)

        raise NotImplementedError

    @asyncio.coroutine
    def verify_peers(self, source: ISourcePeer, target: ITargetPeer,
                     create_target: bool=False) -> tuple:
        """Verifies that source and target databases are exists and accessible.

        If target is not exists (HTTP 404) it may be created in case when
        :attr:`ReplicationTask.create_target` is set as ``True``.

        Raises :exc:`aiocouchdb.error.HttpErrorException` exception depending
        from the HTTP error of initial peers requests.
        """
        source_info = yield from source.info()

        if not (yield from target.exists()):
            if create_target:
                yield from target.create()
        target_info = yield from target.info()

        return source_info, target_info
