#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is experimental module for reading from Google Cloud Spanner.
https://cloud.google.com/spanner

To read from Cloud Spanner, apply ReadFromSpanner transformation.
It will return a PCollection of list, where each element represents an
individual row returned from the read operation.
Both Query and Read APIs are supported. See more information
about "https://cloud.google.com/spanner/docs/reads".

To execute a "query", specify a "ReadFromSpanner.with_query(QUERY_STRING)"
during the construction of the transform. For example:

    records = (pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME)
                  .with_query('SELECT * FROM users'))

To use the read API, specify a "ReadFromSpanner.with_table(TABLE_NAME, COLUMNS)"
during the construction of the transform. For example:

    records = (pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME)
                  .with_table("users", ["id", "name", "email"]))

"ReadFromSpanner.with_table" also support indexes by specifying the "index"
parameter. For more information, the spanner read with index documentation:
https://cloud.google.com/spanner/docs/secondary-indexes#read-with-index


It is possible to read several PCollection of ReadOperation within a single
transaction. Apply ReadFromSpanner.create_transaction() transform, that lazily
creates a transaction. The result of this transformation can be passed to
read operation using ReadFromSpanner.with_transaction(). For Example:

    transaction = ReadFromSpanner.create_transaction(
        project_id=PROJECT_ID,
        instance_id=sINSTANCE_ID,
        database_id=DB_NAME,
        exact_staleness=datetime.timedelta(seconds=100))

    spanner_read = ReadFromSpanner(
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        database_id=DB_NAME)

    users = (pipeline
            | 'Get all users' >> spanner_read.with_transaction(transaction)
               .with_query("SELECT * FROM users"))
    tweets = (pipeline
            | 'Get all tweets' >> spanner_read.with_transaction(transaction)
             .with_query("SELECT * FROM tweets"))
"""
from __future__ import absolute_import

import collections
import warnings

from google.cloud.spanner import Client
from google.cloud.spanner import KeySet
from google.cloud.spanner_v1.database import BatchSnapshot


import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.utils.annotations import experimental

__all__ = ['ReadFromSpanner', 'ReadOperation',]


class ReadOperation(collections.namedtuple("ReadOperation",
                                           "read_operation batch_action "
                                           "transaction_action kwargs")):
  """
  Encapsulates a spanner read operation.
  """

  __slots__ = ()

  @classmethod
  def with_query(cls, sql, params=None, param_types=None):
    return cls(
        read_operation="process_query_batch",
        batch_action="generate_query_batches", transaction_action="execute_sql",
        kwargs={'sql': sql, 'params': params, 'param_types': param_types}
    )

  @classmethod
  def with_table(cls, table, columns, index="", keyset=None):
    keyset = keyset or KeySet(all_=True)
    if not isinstance(keyset, KeySet):
      raise ValueError("keyset must be an instance of class "
                       "google.cloud.spanner_v1.keyset.KeySet")
    return cls(
        read_operation="process_read_batch",
        batch_action="generate_read_batches", transaction_action="read",
        kwargs={'table': table, 'columns': columns, 'index': index,
                'keyset': keyset}
    )


class ReadFromSpanner(object):

  def __init__(self, project_id, instance_id, database_id, pool=None,
               read_timestamp=None, exact_staleness=None):
    warnings.warn("ReadFromSpanner is experimental.",FutureWarning,
                  stacklevel=2)
    self._transaction = None

    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._pool = pool
    self._snapshot_options = {}
    if read_timestamp:
      self._snapshot_options['read_timestamp'] = read_timestamp
    if exact_staleness:
      self._snapshot_options['exact_staleness'] = exact_staleness

  def with_query(self, sql, params=None, param_types=None):
    read_operation = [ReadOperation.with_query(sql, params, param_types)]
    return self.read_all(read_operation)

  def with_table(self, table, columns, index="", keyset=None):
    read_operation = [ReadOperation.with_table(
        table=table, columns=columns, index=index, keyset=keyset
    )]
    return self.read_all(read_operation)

  def read_all(self, read_operations):
    if self._transaction is None:
      return _BatchRead(project_id=self._project_id,
                        instance_id=self._instance_id,
                        database_id=self._database_id,
                        read_operations=read_operations,
                        snapshot_options=self._snapshot_options,
                        pool=self._pool)
    else:
      return _NaiveSpannerRead(project_id=self._project_id,
                               instance_id=self._instance_id,
                               database_id=self._database_id,
                               transaction=self._transaction,
                               read_operations=read_operations,
                               pool=self._pool)

  @staticmethod
  @experimental(extra_message="(ReadFromSpanner)")
  def create_transaction(project_id, instance_id, database_id, pool=None,
                         read_timestamp=None, exact_staleness=None):
    """
    Return the snapshot state for reuse in transaction.

    :type project_id: :class:`str`
    :param project_id: The ID of the project which owns the instances, tables
      and data.

    :type instance_id: :class:`str`
    :param instance_id: The ID of the instance.

    :type database_id: :class:`str`
    :param database_id: The ID of the database instance.

    :type pool: concrete subclass of
        :class:`~google.cloud.spanner_v1.pool.AbstractSessionPool`.
    :param pool: (Optional) session pool to be used by database.

    :type read_timestamp: :class:`datetime.datetime`
    :param read_timestamp: (Optional) Execute all reads at the given timestamp.

    :type exact_staleness: :class:`datetime.timedelta`
    :param exact_staleness: (Optional) Execute all reads at a timestamp that is
      ``exact_staleness`` old.

    :rtype: :class:`dict`
    :return: returns the state.
    """
    _snapshot_options = {}
    if read_timestamp:
      _snapshot_options['read_timestamp'] = read_timestamp
    if exact_staleness:
      _snapshot_options['exact_staleness'] = exact_staleness

    spanner_client = Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id, pool=pool)
    snapshot = database.batch_snapshot(**_snapshot_options)
    return snapshot.to_dict()

  def with_transaction(self, transaction):
    self._transaction = transaction
    return self


class _NaiveSpannerReadDoFn(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id, snapshot_dict,
               pool=None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._snapshot_dict = snapshot_dict
    self._pool = pool
    self._snapshot = None

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id, pool=self._pool)
    self._snapshot = BatchSnapshot.from_dict(database, self._snapshot_dict)

  def process(self, element):
    with self._snapshot._get_session().transaction() as transaction:
      for row in getattr(transaction, element.transaction_action)(
          **element.kwargs):
        yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


class _NaiveSpannerRead(PTransform):
  """
  A naive version of Spanner read that use transactions for read and execute
  sql methods from the previous state.
  """

  def __init__(self, project_id, instance_id, database_id, transaction,
               read_operations, pool=None):
    self._transaction = transaction
    self._read_operations = read_operations
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._pool = pool

  def expand(self, pbegin):
    return (pbegin
            | 'Add Read Operations' >> beam.Create(self._read_operations)
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Perform Read' >> beam.ParDo(
                _NaiveSpannerReadDoFn(project_id=self._project_id,
                                      instance_id=self._instance_id,
                                      database_id=self._database_id,
                                      snapshot_dict=self._transaction,
                                      pool=self._pool)))


class _BatchRead(PTransform):
  """
  This transform uses the Cloud Spanner BatchSnapshot to perform reads from
  multiple partitions.
  """

  def __init__(self, project_id, instance_id, database_id, read_operations,
               snapshot_options=None, pool=None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._read_operations = read_operations
    self._snapshot_options = snapshot_options or {}
    self._pool = pool

  def expand(self, pbegin):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id, pool=self._pool)
    snapshot = database.batch_snapshot(**self._snapshot_options)

    reads = [
        {"read_operation": ro.read_operation, "partitions": p}
        for ro in self._read_operations
        for p in getattr(snapshot, ro.batch_action)(**ro.kwargs)
    ]

    return (pbegin
            | 'Generate Partitions' >> beam.Create(reads)
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Read From Partitions' >> beam.ParDo(
                _ReadFromPartitionFn(self._project_id, self._instance_id,
                                     self._database_id, snapshot.to_dict())))


class _ReadFromPartitionFn(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id, snapshot_dict,
               pool=None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._snapshot_dict = snapshot_dict
    self._pool = pool
    self._snapshot = None
    self._database = None

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    self._database = instance.database(self._database_id, pool=self._pool)

  def process(self, element):
    self._snapshot = BatchSnapshot.from_dict(self._database,
                                             self._snapshot_dict)
    read_operation = element['read_operation']
    elem = element['partitions']

    for row in getattr(self._snapshot, read_operation)(elem):
      yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()
