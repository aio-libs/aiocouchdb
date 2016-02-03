0.9.1 (2016-02-03)
------------------

- Read views and changes feeds line by line, not by chunks.
  This fixes #8 and #9 issues.
- Deprecate Python 3.3 support. 0.10 will be 3.4.1+ only.


0.9.0 (2015-10-31)
------------------

- First release in aio-libs organization (:
- Add context managers for response and feeds objects to release connection
  when work with them is done
- Use own way to handle JSON responses that doesn't involves chardet usage
- Add HTTPSession object that helps to apply the same auth credentials and
  TCP connector for the all further requests made with it
- aiocouchdb now uses own request module which is basically fork of aiohttp one
- AuthProviders API upgraded for better workflow
- Fix _bulk_docs request with new_edit
- Workaround COUCHDB-2295 by calculating multipart request body
- Allow to pass event loop explicitly to every major objects
- Fix parameters for Server.replicate method
- Minor fixes for docstrings
- Quite a lot of changes in Makefile commands for better life
- Minimal requirements for aiohttp raised up to 0.17.4 version

0.8.0 (2015-03-20)
------------------

- Source tree was refactored in the way to support multiple major CouchDB
  versions as like as the other friendly forks
- Database create and delete methods now return exact the same response as
  CouchDB sends back
- Each module now contains __all__ list to normalize their exports
- API classes and Resource now has nicer __repr__ output
- Better error messages format
- Fix function_clause error on attempt to update a document with attachments
  by using multipart request
- Document.update doesn't makes document's dict invalid for further requests
  after multipart one
- Fixed accidental payload sent with HEAD/GET/DELETE requests which caused
  connection close from CouchDB side
- Added integration with Travis CI
- Code cleaned by following pylint and flake8 notices
- Added short tutorial for documentation
- Minor fixes and Makefile improvements

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
