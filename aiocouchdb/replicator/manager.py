# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationTask
from .replication import Replication


__all__ = (
    'ReplicationManager',
)


class ReplicationManager(object):
    """Replication managers starts, cancels and track replication processes."""

    replication_job_class = Replication

    def __init__(self,
                 source_peer_class: ISourcePeer,
                 target_peer_class: ITargetPeer, *,
                 replication_job_class=None):
        self.source_peer_class = source_peer_class
        self.target_peer_class = target_peer_class
        if replication_job_class is not None:
            self.replication_job_class = replication_job_class

    @asyncio.coroutine
    def execute_task(self, task: ReplicationTask):
        """Executes replication task."""
        raise NotImplementedError

    @asyncio.coroutine
    def start_replication(self, task: ReplicationTask, *,
                          source_peer_class: ISourcePeer=None,
                          target_peer_class: ITargetPeer=None):
        """Starts a new Replication process."""

        replication = self.replication_job_class(
            task,
            source_peer_class or self.source_peer_class,
            target_peer_class or self.target_peer_class)

        yield from replication.start()

    @asyncio.coroutine
    def cancel_replication(self, rep_id: str):
        """Cancels replication process."""
        raise NotImplementedError
