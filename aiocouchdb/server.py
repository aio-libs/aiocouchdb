# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio

from .client import Resource
from .errors import maybe_raise_error


class Server(object):
    """Implementation of :ref:`CouchDB Server API <api/server>`."""

    resource_class = Resource

    def __init__(self, url='http://localhost:5984', *, resource_class=None):
        if resource_class is not None:
            self.resource_class = resource_class
        if not isinstance(url, self.resource_class):
            url = self.resource_class(url)
        self.resource = url

    @asyncio.coroutine
    def info(self):
        """Returns server :ref:`meta information and welcome message
        <api/server/root>`.

        :rtype: dict
        """
        resp = yield from self.resource.get()
        yield from maybe_raise_error(resp)
        return (yield from resp.json())

    @asyncio.coroutine
    def active_tasks(self):
        """Returns list of :ref:`active tasks <api/server/active_tasks>`
        which runs on server.

        :rtype: list
        """
        resp = yield from self.resource.get('_active_tasks')
        yield from maybe_raise_error(resp)
        return (yield from resp.json())
