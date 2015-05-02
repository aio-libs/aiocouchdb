# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import abc


__all__ = (
    'IPeer',
    'ISourcePeer',
    'ITargetPeer',
)


class IPeer(object, metaclass=abc.ABCMeta):

    def __init__(self, peer_info):
        pass


class ISourcePeer(IPeer):
    """Source peer interface."""


class ITargetPeer(IPeer):
    """Target peer interface."""
