0.7.0 (2015-02-18)
------------------

- Greatly improved multipart module, added multipart writer
- Document.update now supports multipart requests to upload
  multiple attachments in single request
- Added Proxy Authentication provider
- Minimal requirements for aiohttp raised up to 0.14.0 version

0.6.0 (2014-11-12)
------------------

- Adopt test suite to run against real CouchDB instance
- Database, documents and attachments now provides access to their name/id
- Remove redundant longnamed constructors
- Construct Database/Document/Attachment instances through __getitem__ protocol
- Add Document.rev method to get current document`s revision
- Add helpers to work with authentication database (_users)
- Add optional limitation of feeds buffer
- All remove(...) methods are renamed to delete(...) ones
- Add support for config option existence check
- Correctly set members for database security
- Fix requests with Accept-Ranges header against attachments
- Fix views requests when startkey/endkey should be null
- Allow to pass custom query parameters and request headers onto changes feed
  request
- Handle correctly HTTP 416 error response
- Minor code fixes and cleanup

0.5.0 (2014-09-26)
------------------

- Last checkpoint release. It's in beta now!
- Implements CouchDB Design Documents HTTP API
- Views refactoring and implementation consolidation

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
