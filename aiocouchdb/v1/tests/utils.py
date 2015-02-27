# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from aiocouchdb.tests import utils
from aiocouchdb.tests.utils import (
    modify_server,
    populate_database,
    run_for,
    skip_for,
    using_database,
    uuid,
    with_fixed_admin_party,
    TestCase
)

from .. import attachment
from .. import database
from .. import designdoc
from .. import document
from .. import server


class ServerTestCase(utils.ServerTestCase):
    server_class = server.Server


class DatabaseTestCase(ServerTestCase, utils.DatabaseTestCase):
    database_class = database.Database


class DocumentTestCase(DatabaseTestCase, utils.DocumentTestCase):
    document_class = document.Document


class DesignDocumentTestCase(DatabaseTestCase, utils.DesignDocumentTestCase):
    designdoc_class = designdoc.DesignDocument


class AttachmentTestCase(DocumentTestCase, utils.AttachmentTestCase):
    attachment_class = attachment.Attachment
