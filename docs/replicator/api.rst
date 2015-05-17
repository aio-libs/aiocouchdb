.. _replicator/api:

=========================
aiocouchdb Replicator API
=========================

Replicator
==========

.. autoclass:: aiocouchdb.replicator.manager.ReplicationManager
  :members:

.. autoclass:: aiocouchdb.replicator.replication.Replication
  :members:

.. autoclass:: aiocouchdb.replicator.worker.ReplicationWorker
  :members:

.. autoclass:: aiocouchdb.replicator.work_queue.WorkQueue
  :members:

Records
=======

.. autoclass:: aiocouchdb.replicator.records.PeerInfo
  :members:
  :special-members:

.. autoclass:: aiocouchdb.replicator.records.ReplicationTask
  :members:
  :special-members:

.. autoclass:: aiocouchdb.replicator.records.ReplicationState
  :members:
  :special-members:

.. autoclass:: aiocouchdb.replicator.records.TsSeq
  :members:
  :special-members:

Peers Interface
===============

.. autoclass:: aiocouchdb.replicator.abc.ISourcePeer
  :members:
  :inherited-members:

.. autoclass:: aiocouchdb.replicator.abc.ITargetPeer
  :members:
  :inherited-members:
