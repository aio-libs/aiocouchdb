0.5.0 (dev)
-----------

0.4.0 (2014-09-17)
------------------

- Another checkpoint release
- Implements CouchDB Attachment HTTP API
- Minimal requirements for aiohttp raised up to 0.9.1 version
- Minor fixes for Document API

0.3.0 (2014-08-18)
------------------

- Third checkpoint release
- Implements CouchDB Document HTTP API
- Support document`s multipart API (but not doc update due to COUCHDB-2295)
- Minimal requirements for aiohttp raised up to 0.9.0 version
- Better documentation

0.2.0 (2014-07-08)
------------------

- Second checkpoint release
- Implements CouchDB Database HTTP API
- Bulk docs accepts generator as an argument and streams request doc by doc
- Views are processed as stream
- Unified output for various changes feed types
- Basic Auth accepts non-ASCII credentials
- Minimal requirements for aiohttp raised up to 0.8.4 version

0.1.0 (2014-07-01)
------------------

- Initial checkpoint release
- Implements CouchDB Server HTTP API
- BasicAuth, Cookie, OAuth authentication providers
- Multi-session workflow
