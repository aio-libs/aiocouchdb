# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import asyncio
import aiohttp
import urllib.parse


class Resource(object):
    """HTTP resource representation. Accepts full ``url`` as argument.

    >>> res = Resource('http://localhost:5984')
    >>> res
    <Resource @ 'http://localhost:5984'>

    Able to construct new Resource instance by assemble base URL and path
    sections on call:
    >>> new_res = res('foo', 'bar/baz')
    >>> assert new_res is not res
    >>> new_res.url
    'http://localhost:5984/foo/bar%2Fbaz'
    """

    def __init__(self, url):
        self.url = url

    def __call__(self, *path):
        return type(self)(urljoin(self.url, *path))

    def __repr__(self):
        return '<{} @ {!r}>'.format(type(self).__name__, self.url)

    def head(self, path=None, headers=None, params=None, **options):
        """Makes HEAD request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('HEAD', path, None, headers, params, **options)

    def get(self, path=None, headers=None, params=None, **options):
        """Makes GET request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('GET', path, None, headers, params, **options)

    def post(self, path=None, data=None, headers=None, params=None, **options):
        """Makes POST request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('POST', path, data, headers, params, **options)

    def put(self, path=None, data=None, headers=None, params=None, **options):
        """Makes PUT request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('PUT', path, data, headers, params, **options)

    def delete(self, path=None, headers=None, params=None, **options):
        """Makes DELETE request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('DELETE', path, headers, params, **options)

    def copy(self, path=None, headers=None, params=None, **options):
        """Makes COPY request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('COPY', path, headers, params, **options)

    def options(self, path=None, headers=None, params=None, **options):
        """Makes OPTIONS request to the resource. See :meth:`Resource.request`
        for arguments definition."""
        return self.request('OPTIONS', path, headers, params, **options)

    @asyncio.coroutine
    def request(self, method, path=None, data=None, headers=None, params=None,
                **options):
        """Makes a HTTP request to the resource.

        :param str method: HTTP method
        :param str path: Resource relative path (optional)
        :param bytes data: POST/PUT request payload data
        :param dict headers: Custom HTTP request headers
        :param dict params: Custom HTTP request query parameters
        :param options: Additional options for :func:`aiohttp.request` function

        :returns: :class:`aiohttp.HttpResponse` instance
        """
        url = urljoin(self.url, path) if path else self.url
        resp = yield from aiohttp.request(method, url,
                                          data=data,
                                          headers=headers,
                                          params=params,
                                          **options)
        return resp


def urljoin(base, *path):
    """Assemble a URI based on a base, any number of path segments, and query
    string parameters.

    >>> urljoin('http://example.org', '_all_dbs')
    'http://example.org/_all_dbs'

    A trailing slash on the uri base is handled gracefully:

    >>> urljoin('http://example.org/', '_all_dbs')
    'http://example.org/_all_dbs'

    And multiple positional arguments become path parts:

    >>> urljoin('http://example.org/', 'foo', 'bar')
    'http://example.org/foo/bar'

    All slashes within a path part are escaped:

    >>> urljoin('http://example.org/', 'foo/bar')
    'http://example.org/foo%2Fbar'
    >>> urljoin('http://example.org/', 'foo', '/bar/')
    'http://example.org/foo/%2Fbar%2F'

    >>> urljoin('http://example.org/', None) #doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError: argument 2 to map() must support iteration
    """
    base = base.rstrip('/')
    if not path:
        return base
    return '/'.join([base] + [urllib.parse.quote(s, '') for s in path])
