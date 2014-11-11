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
    def request(self, *,
                auth=None,
                feed_buffer_size=None,
                data=None,
                params=None):
        """Requests a view associated with the owned resource.

        :param auth: :class:`aiocouchdb.authn.AuthProvider` instance
        :param int feed_buffer_size: Internal buffer size for fetched feed items
        :param dict data: View request payload
        :param dict params: View request query parameters

        :rtype: :class:`aiocouchdb.feeds.ViewFeed`
        """
        if params is not None:
            params, data = self.handle_keys_param(params, data)
            params = self.prepare_params(params)

        if data:
            request = self.resource.post
        else:
            request = self.resource.get

        resp = yield from request(auth=auth, data=data, params=params)
        yield from resp.maybe_raise_error()
        return ViewFeed(resp, buffer_size=feed_buffer_size)

    @staticmethod
    def prepare_params(params):
        json_params = {'key', 'keys', 'startkey', 'endkey'}
        params = dict(
            (key, value)
            for key, value in params.items()
            if (key in json_params and value is not Ellipsis)
            or (key not in json_params and value is not None))

        # CouchDB requires these params have valid JSON value
        for param in json_params:
            if param in params:
                params[param] = json.dumps(params[param])
        return params

    @staticmethod
    def handle_keys_param(params, data):
        keys = params.pop('keys', ())
        if keys is None or keys is Ellipsis:
            return params, data
        assert not isinstance(keys, (bytes, str))

        if len(keys) >= 2:
            if data is None:
                data = {'keys': keys}
            elif isinstance(data, dict):
                data['keys'] = keys
            else:
                params['keys'] = keys
        elif keys:
            assert params.get('key') is None
            params['key'] = keys[0]

        return params, data
