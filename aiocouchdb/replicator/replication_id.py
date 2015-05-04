# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import hashlib

from . import erlterm
from .records import PeerInfo

__all__ = (
    'v3',
)


def v3(uuid: str,
       source: PeerInfo,
       target: PeerInfo, *,
       continuous: bool=None,
       create_target: bool=None,
       doc_ids: list=None,
       filter: str=None,
       query_params: list=None) -> str:
    """Generates replication id for protocol version 3."""
    rep_id = [uuid.encode('utf-8'),
              get_peer_endpoint(source),
              get_peer_endpoint(target)]
    maybe_append_filter_info(rep_id,
                             doc_ids=doc_ids,
                             filter=filter,
                             query_params=query_params)
    rep_id = hashlib.md5(erlterm.encode(rep_id)).hexdigest()
    return maybe_append_options(rep_id, [('continuous', continuous),
                                         ('create_target', create_target)])


def get_peer_endpoint(peer: PeerInfo) -> tuple:
    url = maybe_append_trailing_slash(peer.url)
    headers = sorted(peer.headers.items())
    if url.startswith('http'):
        return (erlterm.Atom(b'remote'), url, headers)
    else:
        raise RuntimeError('local peers are not supported')


def maybe_append_trailing_slash(url: str) -> str:
    if not url.startswith('http'):
        return url
    if url.endswith('/'):
        return url
    return url + '/'


def maybe_append_filter_info(rep_id: list, *,
                             doc_ids: list=None,
                             filter: str=None,
                             query_params: list=None):
    if filter is None:
        if doc_ids:
            rep_id.append([idx.encode('utf-8') for idx in doc_ids])
    else:
        if isinstance(query_params, dict):
            query_params = sorted((key.encode('utf-8'), value.encode('utf-8'))
                                  for key, value in query_params.items())
        elif query_params:
            query_params = [(key.encode('utf-8'), value.encode('utf-8'))
                            for key, value in query_params]
        else:
            query_params = []
        rep_id.extend([filter.strip().encode('utf-8'), (query_params,)])


def maybe_append_options(rep_id: str, options: list) -> str:
    for key, value in options:
        if value:
            rep_id += '+' + key
    return rep_id
