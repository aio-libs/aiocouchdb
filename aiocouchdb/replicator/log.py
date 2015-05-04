# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import logging


def activate_debug_logging():
    rep_log = logging.getLogger('aiocouchdb.replicator.replication')
    rep_log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s '
                                      '- %(rep_id)s - %(message)s'))
    rep_log.addHandler(ch)
