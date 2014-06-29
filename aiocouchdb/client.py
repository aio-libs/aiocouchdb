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
import json
import logging
import urllib.parse


class HttpRequest(aiohttp.client.ClientRequest):
    """:class:`aiohttp.client.ClientRequest` class with CouchDB specifics."""

    #: Default HTTP request headers.
    DEFAULT_HEADERS = {
        'ACCEPT': 'application/json',
        'ACCEPT-ENCODING': 'gzip, deflate',
        'CONTENT-TYPE': 'application/json'
    }

    def update_body_from_data(self, data):
        """Encodes ``data`` as JSON if `Content-Type`
        is :mimetype:`application/json`."""
        if self.headers.get('CONTENT-TYPE') == 'application/json':
            data = json.dumps(data)
        return super().update_body_from_data(data)

    def update_path(self, params, data):
        if isinstance(params, dict):
            params = params.copy()
            for key, value in params.items():
                if value is True:
                    params[key] = 'true'
                elif value is False:
                    params[key] = 'false'
        return super().update_path(params, data)


class HttpResponse(aiohttp.client.ClientResponse):
    """:class:`aiohttp.client.ClientResponse` class with CouchDB specifics."""

    @asyncio.coroutine
    def read(self):
        """Read response payload.
        Unlike :meth:`aiohttp.client.ClientResponse.read` doesn't decodes
        the response."""
        if self._content is None:
            buf = []
            total = 0
            try:
                while True:
                    chunk = yield from self.content.read()
                    size = len(chunk)
                    buf.append((chunk, size))
                    total += size
            except aiohttp.EofStream:
                pass

            self._content = bytearray(total)

            idx = 0
            content = memoryview(self._content)
            for chunk, size in buf:
                content[idx:idx+size] = chunk
                idx += size

        return self._content

    @asyncio.coroutine
    def read_and_close(self):
        """Read response payload and then close response."""
        super().read_and_close()

    @asyncio.coroutine
    def json(self):
        """Reads and decodes JSON response."""
        if self._content is None:
            yield from self.read()

        ctype = self.headers.get('CONTENT-TYPE', '').lower()
        if not ctype.startswith('application/json'):
            logging.warning(
                'Attempt to decode JSON with unexpected mimetype: %s', ctype)

        if not self._content.strip():
            return None

        return json.loads(self._content.decode('utf-8'))


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

    request_class = HttpRequest
    response_class = HttpResponse

    def __init__(self, url, *, request_class=None, response_class=None):
        self.url = url
        if request_class is not None:
            self.request_class = request_class
        if response_class is not None:
            self.response_class = response_class

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
                                          request_class=self.request_class,
                                          response_class=self.response_class,
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
