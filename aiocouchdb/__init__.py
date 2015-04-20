# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

# flake8: noqa

from .authn import (
    AuthProvider,
    NoAuthProvider,
    BasicAuthProvider,
    CookieAuthProvider,
    OAuthProvider,
    ProxyAuthProvider
)
from .errors import (
    HttpErrorException,
    BadRequest,
    Unauthorized,
    Forbidden,
    ResourceNotFound,
    MethodNotAllowed,
    ResourceConflict,
    PreconditionFailed,
    RequestedRangeNotSatisfiable,
    ServerError
)
from .v1.attachment import Attachment
from .v1.database import Database
from .v1.document import Document
from .v1.designdoc import DesignDocument
from .v1.server import Server
from .version import __version__, __version_info__
