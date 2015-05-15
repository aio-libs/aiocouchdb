# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import abc
import asyncio


__all__ = (
    'IPeer',
    'ISourcePeer',
    'ITargetPeer',
)


class IChangesFeed(object, metaclass=abc.ABCMeta):
    """Changes feed reader."""

    @abc.abstractmethod
    @asyncio.coroutine
    def next(self) -> dict:
        """Emits the next event from a changes feed. The returned dict object
        must contain the following fields:

        - **seq** - Sequence ID
        - **id** (`str`) - Document ID
        - **changes** (`list` of `dict`) - List of document changes where each
          element must contain **rev** field (`str`) with the revision value.

        :rtype: dict
        """


class IPeer(object, metaclass=abc.ABCMeta):

    def __init__(self, peer_info):
        pass

    @abc.abstractmethod
    @asyncio.coroutine
    def exists(self) -> bool:
        """Checks if Database is exists and available for further queries.

        :rtype: bool
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def info(self) -> dict:
        """Returns information about Database. This dict object MUST contains
        the following fields:

        - **instance_start_time** (`str`) - timestamp when the Database was
          opened, expressed in microseconds since the epoch.

        - **update_seq** - current database Sequence ID.

        :rtype: dict
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def get_replication_log(self, docid: str) -> dict:
        """Returns Replication log document instance by given ID. If document
        couldn't be found, should return empty dict.

        :param str docid: Replication ID

        :rtype: dict
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def update_replication_log(self, rep_id: str, doc: dict, *,
                               rev: str=None) -> str:
        """Updates a document and returns new MVCC revision value back.

        :param str rep_id: Replication ID
        :param dict doc: Replication Log document

        :rtype: str
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def ensure_full_commit(self) -> str:
        """Ensures that all changes are flushed on disk. Returns an instance
        start time.

        :rtype: str
        """


class ISourcePeer(IPeer):
    """Source peer interface."""

    @abc.abstractmethod
    @asyncio.coroutine
    def get_filter_function_code(self, filter_name: str) -> str:
        """Returns filter function code that would be applied on changes feed.

        :param str filter_name: Filter function name

        :rtype: str
        """
        # We do abstract from knowledge about design documents and the place
        # where filters are defined there.

    @abc.abstractmethod
    @asyncio.coroutine
    def open_doc_revs(self,
                      docid: str,
                      open_revs: list,
                      callback_coro, *,
                      atts_since: list=None,
                      latest: bool=None,
                      revs: bool=None):
        """Returns reader of document revisions with the attachments.

        :param str docid: Document ID
        :param list open_revs: Fetch specified conflict revisions
        :param callback_coro: Callback coroutine
        :param list atts_since: Fetch attachments from the specified revisions
        :param bool latest: Ensure that the latest revision is included
                            in response
        :param bool revs: Include information about all known revisions
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def changes(self, *,
                continuous: bool=False,
                doc_ids: list=None,
                filter: str=None,
                query_params: dict=None,
                since=None,
                view: str=None) -> IChangesFeed:
        """Starts listen changes feed."""


class ITargetPeer(IPeer):
    """Target peer interface."""

    @abc.abstractmethod
    @asyncio.coroutine
    def create(self):
        """Creates target database."""

    @abc.abstractmethod
    @asyncio.coroutine
    def revs_diff(self, idrevs: dict) -> dict:
        """Compares given document id to list of revisions mapping with
        the stored data and returns back same mapping with the revisions that
        are missed on the peer side.

        :param dict idrevs: Mapping between document id and list of revisions

        :rtype dict:
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def update_doc(self, doc: dict, atts) -> Exception:
        """Updates a document with the attachments. This update may produce
        a conflict.

        :param dict doc: Document object
        :param atts: Attachments reader object

        :returns: Exception, that happens on document update, but is not a fatal
                  one.
        """

    @abc.abstractmethod
    @asyncio.coroutine
    def update_docs(self, docs: list) -> list:
        """Performs bulk update specified list of documents in non-edit mode in
        order to create conflicts instead of raising update conflict errors.

        Returns a list of dict objects for documents which failed to update
        with the related error and reason.

        :param list docs: List of document objects (dicts)

        :rtype: list
        """
