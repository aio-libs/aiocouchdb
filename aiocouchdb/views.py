# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import json
from .feeds import ViewFeed


class View(object):
    """Views requesting helper."""

    def __init__(self, resource):
        self.resource = resource

    @asyncio.coroutine
    def request(self, *, auth=None, data=None, params=None):
        """Requests a view associated with the owned resource.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param dict data: View request payload
        :param dict params: View request query parameters

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        if params is None:
            params = {}
        else:
            params = dict((key, value)
                          for key, value in params.items()
                          if value is not None)

            keys = params.pop('keys', ())
            if keys:
                assert not isinstance(keys, (bytes, str))

                if len(keys) >= 2:
                    if data is None:
                        data = {'keys': keys}
                    else:
                        data['keys'] = keys
                elif keys:
                    params['key'] = json.dumps(keys[0])

            # CouchDB requires these params have valid JSON value
            for param in ('key', 'startkey', 'endkey'):
                if param in params:
                    params[param] = json.dumps(params[param])

        if data:
            request = self.resource.post
        else:
            request = self.resource.get

        resp = yield from request(auth=auth, data=data, params=params)
        yield from resp.maybe_raise_error()
        return ViewFeed(resp)
