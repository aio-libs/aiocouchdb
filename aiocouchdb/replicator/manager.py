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
import math
import uuid

from .abc import ISourcePeer, ITargetPeer
from .records import ReplicationState, ReplicationStats, ReplicationTask
from .replication import Replication


__all__ = (
    'ReplicationManager',
    'ReplicationInterface',
)


log = logging.getLogger(__package__)

#: Replication UUID. Should be unique for each instance.
REPLICATOR_UUID = uuid.uuid4().hex


class ReplicationInterface(object):
    """Replication proxy interface for userland interactions."""

    def __init__(self, replication: Replication, task: asyncio.Task):
        self._replication = replication
        self._task = task
        task.add_done_callback(self.handle_replication_done)
        task.add_done_callback(self.handle_replication_error)

    @property
    def id(self) -> str:
        """Returns Replication ID."""
        return self._replication.id

    def cancel(self):
        """Cancels Replication task."""
        self._task.cancel()

    def is_alive(self) -> bool:
        """Checks if Replication task is still alive."""
        return not self._task.done()

    def estimate_progress(self) -> int:
        """Returns Replication estimate progress for the moment of call."""
        state = self.state()
        current_seq = state.current_through_seq.id
        source_seq = state.source_seq.id
        if isinstance(current_seq, list):
            current_seq = current_seq[0]
        if isinstance(source_seq, list):
            source_seq = source_seq[0]

        return math.trunc((current_seq * 100) / source_seq)

    def state(self) -> ReplicationState:
        """Returns Replication state for the moment of call."""
        return self._replication.state

    def stats(self) -> ReplicationStats:
        """Returns Replication statistics for the moment of call."""
        return self._replication.state.stats

    def handle_replication_done(self, task: asyncio.Future):
        if task.exception():
            return
        replication = self._replication
        log.error('Replication %s (%s -> %s) completed',
                  replication.id,
                  replication.state.rep_task.source.url,
                  replication.state.rep_task.target.url)

    def handle_replication_error(self, task: asyncio.Future):
        if not task.exception():
            return
        exc = task.exception()
        replication = self._replication
        log.error('Replication %s (%s -> %s) failed',
                  replication.id,
                  replication.state.rep_task.source.url,
                  replication.state.rep_task.target.url,
                  exc_info=(exc.__class__, exc, exc.__traceback__))


class ReplicationManager(object):
    """Replication managers starts, cancels and track replication processes."""

    replication_job_class = Replication
    replication_iface_class = ReplicationInterface

    #: Replication protocol version
    protocol_version = 3

    def __init__(self,
                 source_peer_class: ISourcePeer,
                 target_peer_class: ITargetPeer, *,
                 replication_job_class=None,
                 rep_uuid: str=None):
        self.source_peer_class = source_peer_class
        self.target_peer_class = target_peer_class
        if replication_job_class is not None:
            self.replication_job_class = replication_job_class
        self.rep_uuid = rep_uuid or REPLICATOR_UUID
        self.registry = {}

    @asyncio.coroutine
    def execute_task(self, task: ReplicationTask):
        """Executes replication task."""
        if task.cancel:
            return (yield from self.cancel(task.rep_id))
        else:
            return (yield from self.start(task))

    @asyncio.coroutine
    def start(self,
              rep_task: ReplicationTask, *,
              source_peer_class: ISourcePeer=None,
              target_peer_class: ITargetPeer=None) -> ReplicationInterface:
        """Starts a new Replication process."""

        replication = self.replication_job_class(
            self.rep_uuid,
            rep_task,
            source_peer_class or self.source_peer_class,
            target_peer_class or self.target_peer_class,
            protocol_version=self.protocol_version)

        task = yield from replication.start()

        monitor = self.replication_iface_class(replication, task)
        self.registry[replication.id] = monitor
        return monitor

    @asyncio.coroutine
    def cancel(self, rep_id: str) -> ReplicationInterface:
        """Cancels replication process."""
        self.registry[rep_id].cancel()
        return self.registry[rep_id]
