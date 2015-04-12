Welcome to aiocouchdb's documentation!
**************************************

:source: https://github.com/aio-libs/aiocouchdb
:documentation: http://aiocouchdb.readthedocs.org/en/latest/
:license: BSD

.. toctree::
    v1/index
    common

Getting started
===============

.. contents::

If you'd some background experience with `couchdb-python`_ client, you'll find
`aiocouchdb` API a bit familiar. That project is my lovely too, but suddenly
it's completely synchronous.

At first, you need to create instance of :class:`~aiocouchdb.v1.server.Server`
object which interacts with `CouchDB Server API`_:

.. code:: python

    >>> import aiocouchdb
    >>> server = aiocouchdb.Server()
    >>> server
    <aiocouchdb.v1.server.Server(http://localhost:5984) object at 0x7f8199a80350>

As like as `couchdb-python`_ Server instance has ``resource`` attribute that
acts very familiar:

.. code:: python

    >>> server.resource
    <aiocouchdb.client.Resource(http://localhost:5984) object at 0x7f9cba5e2490>
    >>> server.resource('db', 'doc1')
    <aiocouchdb.client.Resource(http://localhost:5984/db/doc1) object at 0x7f9cb9fc2e10>

With the only exception that it's a coroutine:

.. note::
    Python doesn't supports ``yield from`` in shell, so examples below are a bit
    out of a real, but be sure - that's how they works in real.

.. code:: python

    >>> resp = yield from server.resource.get()
    <ClientResponse(http://localhost:5984) [200 OK]>
    <CIMultiDictProxy {'SERVER': 'CouchDB/1.6.1 (Erlang OTP/17)', 'DATE': 'Sun, 08 Mar 2015 15:13:12 GMT', 'CONTENT-TYPE': 'application/json', 'CONTENT-LENGTH': '139', 'CACHE-CONTROL': 'must-revalidate'}>
    >>> yield from resp.json()
    {'couchdb': 'Welcome!',
     'vendor': {'version': '1.6.1', 'name': 'The Apache Software Foundation'},
     'uuid': '0510c29b75ae33fd3975eb505db2dd12',
     'version': '1.6.1'}

The :class:`~aiocouchdb.client.Resource` object provides a tiny wrapper over
:func:`aiohttp.client.request()` function so you can use it in case of raw API
access.

But, libraries are made to hide all the implementation details and make work
with API nice and easy one and `aiocouchdb` isn't an exception.
The example above is actually what the :meth:`Server.info()
<aiocouchdb.v1.server.Server.info>` method does:

.. code:: python

    >>> yield from server.info()
    {'couchdb': 'Welcome!',
     'vendor': {'version': '1.6.1', 'name': 'The Apache Software Foundation'},
     'uuid': '0510c29b75ae33fd3975eb505db2dd12',
     'version': '1.6.1'}

Most of :class:`~aiocouchdb.v1.server.Server` and not only methods are named
similar to the real HTTP API endpoints:

.. code:: python

    >>> yield from server.all_dbs()
    ['_replicator', '_users', 'db']
    >>> yield from server.active_tasks()
    [{'database': 'db',
      'pid': '<0.10209.20>',
      'changes_done': 0,
      'progress': 0,
      'started_on': 1425805499,
      'total_changes': 1430818,
      'type': 'database_compaction',
      'updated_on': 1425805499}]

With a few exceptions like
:attr:`Server.session <aiocouchdb.v1.server.Server.session>` or
:attr:`Server.config <aiocouchdb.v1.server.Server.config>` which has complex
use-case behind and are operated by other objects.

Speaking about :attr:`aiocouchdb.v1.server.Server.session`, `aiocouchdb` 
supports `multiuser workflow` where you pass session object as an argument 
on resource request.

.. code:: python

    >>> admin = yield from server.session.open('admin', 's3cr1t')
    >>> user = yield from server.session.open('user', 'pass')

Here we just opened two session for different users. Their usage is pretty
trivial - just pass them as ``auth`` keyword parameter to every API function
call:

.. code:: python

    >>> yield from server.active_tasks(auth=admin)
    [{'database': 'db',
      'pid': '<0.10209.20>',
      'changes_done': 50413,
      'progress': 3,
      'started_on': 1425805499,
      'total_changes': 1430818,
      'type': 'database_compaction',
      'updated_on': 1425806018}]
    >>> yield from server.active_tasks(auth=user)
    Traceback:
    ...
    Unauthorized: [forbidden] You are not a server admin.

Another important moment that `aiocouchdb` raises exception on HTTP errors.
By using :class:`~aiocouchdb.client.Resource` object you'll receive raw response and may build custom
logic on processing such errors: to raise an exception or to not.

With using :meth:`Server.session.open() <aiocouchdb.v1.session.Session.open>`
you implicitly creates :class:`~aiocouchdb.authn.CookieAuthProvider` which hold
received from CouchDB cookie with authentication token.
`aiocouchdb` also provides the way to authorize via `Basic Auth`, `OAuth`
(`oauthlib`_ required) and others. Their usage is also pretty trivial:

.. code:: python

    >>> admin = aiocouchdb.BasicAuthProvider('admin', 's3cr1t')
    >>> yield from server.active_tasks(auth=admin)
    [{'database': 'db',
      'pid': '<0.10209.20>',
      'changes_done': 50413,
      'progress': 3,
      'started_on': 1425805499,
      'total_changes': 1430818,
      'type': 'database_compaction',
      'updated_on': 1425806018}]

Working with databases
======================

To create a database object which will interact with `CouchDB Database API`_
you have three ways to go:

1. Using direct object instance creation:

.. code:: python

    >>> aiocouchdb.Database('http://localhost:5984/db')
    <aiocouchdb.v1.database.Database(http://localhost:5984/db) object at 0x7ffd44d58f90>

2. Using ``__getitem__`` protocol similar to `couchdb-python`_:

.. code:: python

    >>> server['db']
    <aiocouchdb.v1.database.Database(http://localhost:5984/db) object at 0x7ffd44cf30d0>

3. Using :meth:`Server.db() <aiocouchdb.v1.server.Server.db>` method:

.. code:: python

    >>> yield from server.db('db')
    <aiocouchdb.v1.database.Database(http://localhost:5984/db) object at 0x7ffd44cf3390>

What's their difference? First method is useful when you don't have access to a
:class:`~aiocouchdb.v1.server.Server` instance, but knows database URL.
Second one returns instantly a :class:`~aiocouchdb.v1.database.Database` 
instance for the name you specified.

But the third one is smarter: it verifies that database by name you'd specified
is accessible for you and if it's not - raises an exception:

.. code:: python

    >>> yield from server.db('_users')
    Traceback:
    ...
    Unauthorized: [forbidden] You are not a server admin.
    >>> yield from server.db('_foo')
    Traceback:
    ...
    BadRequest: [illegal_database_name] Name: '_foo'. Only lowercase characters (a-z), digits (0-9), and any of the characters _, $, (, ), +, -, and / are allowed. Must begin with a letter.

This costs you an additional HTTP request, but gives the insurance that the
following methods calls will not fail by unrelated reasons.

This method doesn't raises an exception if database doesn't exists to allow
you create it:

.. code:: python

    >>> db = yield from server.db('newdb')
    >>> yield from db.exists()
    False
    >>> yield from db.create()
    True
    >>> yield from db.exists()
    True

Iterating over documents
------------------------

In `couchdb-python`_ you might done it with in the following way:

.. code:: python

    >>> for docid in db:
    ...    do_something(db[docid])

Or:

.. code:: python

    >>> for row in db.view('_all_docs'):
    ...    do_something(db[row['id']])

`aiocouchdb` does that quite differently:

.. code:: python

    >>> res = yield from db.all_docs()
    >>> while True:
    ...     rec = yield from res.next()
    ...     if rec is None:
    ...         break
    ...     do_something(rec['id'])

What's going on here?

#. You requesting `/db/_all_docs` endpoint explicitly and may pass all his query
   parameters as you need;

#. On :meth:`Database.all_docs() <aiocouchdb.v1.database.Database.all_docs>`
   call returns not a list of view results, but a special instance of
   :class:`~aiocouchdb.feeds.ViewFeed` object which fetches results one by one
   in background into internal buffer without loading whole result into memory
   in single shot. You can control this buffer size with `feed_buffer_size`
   keyword argument;

#. When all the records are processed it emits None which signs on empty feed
   and the loop breaking out;

`aiocouchdb` tries never load large streams, but process them in iterative way.
This may looks ugly for small data sets, but when you deal with the large ones
it'll save you a lot of resources.

The same loop pattern in used to process :meth:`Database.changes()
<aiocouchdb.v1.database.Database.changes>` as well.

Working with documents
======================

To work with a document you need get :class:`~aiocouchdb.v1.document.Document`
instance first - :class:`~aiocouchdb.v1.database.Database` doesn't knows
anything about `CouchDB Document API`_. The way to do this is the same as for
database:

.. code:: python

    >>> aiocouchdb.Document('http://localhost:5984/db/doc1')
    <aiocouchdb.v1.document.Document(http://localhost:5984/db/doc1) object at 0x7ff3ef7af070>
    >>> server['db']['doc1']
    <aiocouchdb.v1.document.Document(http://localhost:5984/db/doc1) object at 0x7fda92ff4850>
    >>> doc = yield from db.doc('doc1')
    <aiocouchdb.v1.document.Document(http://localhost:5984/db/doc1) object at 0x7fda981380d0>

Their difference is the same as for :class:`~aiocouchdb.v1.database.Database`
mentioned above.

.. code:: python

    >>> yield from doc.exists()
    False
    >>> meta = yield from doc.update({'hello': 'CouchDB'})
    >>> meta
    {'ok': True, 'rev': '1-7c6fb984afda7e07d030cce000dc5965', 'id': 'doc1'}
    >>> yield from doc.exists()
    True
    >>> meta = yield from doc.update({'hello': 'CouchDB'}, rev=meta['rev'])
    >>> meta
    {'ok': True, 'rev': '2-c5298951d02b03f3d6273ad5854ea729', 'id': 'doc1'}
    >>> yield from doc.get()
    {'hello': 'CouchDB',
     '_id': 'doc1',
     '_rev': '2-c5298951d02b03f3d6273ad5854ea729'}
    >>> yield from doc.delete('2-c5298951d02b03f3d6273ad5854ea729')
    {'ok': True, 'rev': '3-cfa05c76fb4a0557605d6a8b1a765055', 'id': 'doc1'}
    >>> yield from doc.exists()
    False

Pretty simple, right?

What's next?
============

There are a lot of things are left untold. Checkout :ref:`CouchDB 1x <v1>` API
for more. Happy hacking!

Changes
=======

.. include:: ../CHANGES.rst

License
=======

.. literalinclude:: ../LICENSE


.. _aiohttp: https://github.com/KeepSafe/aiohttp
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _couchdb-python: https://github.com/djc/couchdb-python
.. _oauthlib: https://github.com/idan/oauthlib

.. _CouchDB Database API: http://docs.couchdb.org/en/latest/api/database/index.html
.. _CouchDB Document API: http://docs.couchdb.org/en/latest/api/document/index.html
.. _CouchDB Server API: http://docs.couchdb.org/en/latest/api/server/index.html
