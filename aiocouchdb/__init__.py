# -*- coding: utf-8 -*-
#
# Copyright (C) 2014 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from .authn import (
    AuthProvider,
    NoAuthProvider,
    BasicAuthProvider,
    CookieAuthProvider,
    OAuthProvider
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
from .attachment import Attachment
from .database import Database
from .document import Document
from .designdoc import DesignDocument
from .server import Server
from .version import __version__, __version_info__
