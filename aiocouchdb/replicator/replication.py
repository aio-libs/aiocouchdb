# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#


import asyncio

from .records import ReplicationTask


__all__ = (
    'Replication',
)


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
        raise NotImplementedError
