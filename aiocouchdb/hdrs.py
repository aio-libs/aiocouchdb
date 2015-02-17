# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from aiohttp.hdrs import *
from aiohttp.multidict import upstr

#: http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.5.html
ACCEPT_RANGES = upstr('ACCEPT-RANGES')

#: http://tools.ietf.org/html/rfc2183
CONTENT_DISPOSITION = upstr('CONTENT-DISPOSITION')

#: Defines CouchDB Proxy Auth usernmae
X_AUTH_COUCHDB_USERNAME = upstr('X-Auth-CouchDB-UserName')
#: Defines CouchDB Proxy Auth list of roles separated by a comma
X_AUTH_COUCHDB_ROLES = upstr('X-Auth-CouchDB-Roles')
#: Defines CouchDB Proxy Auth token
X_AUTH_COUCHDB_TOKEN = upstr('X-Auth-CouchDB-Token')
