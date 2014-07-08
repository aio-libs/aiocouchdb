aiocouchdb
==========

CouchDB client built on top of `aiohttp`_.

Current status: **alpha**. Not all CouchDB API is implemented yet, please
consult with `CHANGES.rst` for available API calls and features.

Example
-------

.. code:: python

    import sys
    import asyncio
    from aiocouchdb import Server


    @asyncio.coroutine
    def go(url):
        server = Server(url)

        # multi-session workflow
        admin = yield from server.session.open('admin', 's3cr1t')
        user = yield from server.session.open('user', 'pass')

        admin_info = yield from server.session.info(auth=admin)
        user_info = yield from server.session.info(auth=user)
        print('admin:', admin_info)
        print('user:', user_info)

        # db_updates is admin only resource
        feed = yield from server.db_updates(feed='continuous', auth=admin,
                                            heartbeat=False, timeout=10000)
        while True:
            event = yield from feed.next()
            if event is None:  # feed exhausted
                break
            dbname = event['db_name']

            # what use raw queries? that's easy
            resp = yield from server.resource.get(dbname, auth=user)
            if resp.status == 403:
                # ignore Forbidden errors
                continue
            # but respect everyone else
            yield from resp.maybe_raise_error()
            dbinfo = yield from resp.json(close=True)  # release connection
            print(dbinfo)

        # close sessions
        assert {'ok': True} == (yield from server.session.close(auth=admin))
        assert {'ok': True} == (yield from server.session.close(auth=user))


    if __name__ == '__main__':
        if '--iocp' in sys.argv:
            from asyncio import events, windows_events
            sys.argv.remove('--iocp')
            el = windows_events.ProactorEventLoop()
            events.set_event_loop(el)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go('http://localhost:5984'))


Requirements
------------

- Python 3.3+
- `aiohttp`_
- `oauthlib`_ (optional)


License
-------

BSD


.. _aiohttp: https://github.com/KeepSafe/aiohttp
.. _oauthlib: https://github.com/idan/oauthlib
