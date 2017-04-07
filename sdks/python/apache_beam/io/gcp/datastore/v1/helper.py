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

"""Cloud Datastore helper functions."""
import sys

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import entity_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
  from googledatastore import PropertyFilter, CompositeFilter
  from googledatastore import helper as datastore_helper
  from googledatastore.connection import Datastore
  from googledatastore.connection import RPCError
  QUERY_NOT_FINISHED = query_pb2.QueryResultBatch.NOT_FINISHED
except ImportError:
  QUERY_NOT_FINISHED = None
# pylint: enable=wrong-import-order, wrong-import-position

from apache_beam.internal.gcp import auth
from apache_beam.utils import retry


def key_comparator(k1, k2):
  """A comparator for Datastore keys.

  Comparison is only valid for keys in the same partition. The comparison here
  is between the list of paths for each key.
  """

  if k1.partition_id != k2.partition_id:
    raise ValueError('Cannot compare keys with different partition ids.')

  k2_iter = iter(k2.path)

  for k1_path in k1.path:
    k2_path = next(k2_iter, None)
    if not k2_path:
      return 1

    result = compare_path(k1_path, k2_path)

    if result != 0:
      return result

  k2_path = next(k2_iter, None)
  if k2_path:
    return -1
  else:
    return 0


def compare_path(p1, p2):
  """A comparator for key path.

  A path has either an `id` or a `name` field defined. The
  comparison works with the following rules:

  1. If one path has `id` defined while the other doesn't, then the
  one with `id` defined is considered smaller.
  2. If both paths have `id` defined, then their ids are compared.
  3. If no `id` is defined for both paths, then their `names` are compared.
  """

  result = str_compare(p1.kind, p2.kind)
  if result != 0:
    return result

  if p1.HasField('id'):
    if not p2.HasField('id'):
      return -1

    return p1.id - p2.id

  if p2.HasField('id'):
    return 1

  return str_compare(p1.name, p2.name)


def str_compare(s1, s2):
  if s1 == s2:
    return 0
  elif s1 < s2:
    return -1
  else:
    return 1


def get_datastore(project):
  """Returns a Cloud Datastore client."""
  credentials = auth.get_service_credentials()
  return Datastore(project, credentials, host='batch-datastore.googleapis.com')


def make_request(project, namespace, query):
  """Make a Cloud Datastore request for the given query."""
  req = datastore_pb2.RunQueryRequest()
  req.partition_id.CopyFrom(make_partition(project, namespace))

  req.query.CopyFrom(query)
  return req


def make_partition(project, namespace):
  """Make a PartitionId for the given project and namespace."""
  partition = entity_pb2.PartitionId()
  partition.project_id = project
  if namespace is not None:
    partition.namespace_id = namespace

  return partition


def retry_on_rpc_error(exception):
  """A retry filter for Cloud Datastore RPCErrors."""
  if isinstance(exception, RPCError):
    if exception.code >= 500:
      return True
    else:
      return False
  else:
    # TODO(vikasrk): Figure out what other errors should be retried.
    return False


def fetch_entities(project, namespace, query, datastore):
  """A helper method to fetch entities from Cloud Datastore.

  Args:
    project: Project ID
    namespace: Cloud Datastore namespace
    query: Query to be read from
    datastore: Cloud Datastore Client

  Returns:
    An iterator of entities.
  """
  return QueryIterator(project, namespace, query, datastore)


def is_key_valid(key):
  """Returns True if a Cloud Datastore key is complete.

  A key is complete if its last element has either an id or a name.
  """
  if not key.path:
    return False
  return key.path[-1].HasField('id') or key.path[-1].HasField('name')


def write_mutations(datastore, project, mutations):
  """A helper function to write a batch of mutations to Cloud Datastore.

  If a commit fails, it will be retried upto 5 times. All mutations in the
  batch will be committed again, even if the commit was partially successful.
  If the retry limit is exceeded, the last exception from Cloud Datastore will
  be raised.
  """
  commit_request = datastore_pb2.CommitRequest()
  commit_request.mode = datastore_pb2.CommitRequest.NON_TRANSACTIONAL
  commit_request.project_id = project
  for mutation in mutations:
    commit_request.mutations.add().CopyFrom(mutation)

  @retry.with_exponential_backoff(num_retries=5,
                                  retry_filter=retry_on_rpc_error)
  def commit(req):
    datastore.commit(req)

  commit(commit_request)


def make_latest_timestamp_query(namespace):
  """Make a Query to fetch the latest timestamp statistics."""
  query = query_pb2.Query()
  if namespace is None:
    query.kind.add().name = '__Stat_Total__'
  else:
    query.kind.add().name = '__Stat_Ns_Total__'

  # Descending order of `timestamp`
  datastore_helper.add_property_orders(query, "-timestamp")
  # Only get the latest entity
  query.limit.value = 1
  return query


def make_kind_stats_query(namespace, kind, latest_timestamp):
  """Make a Query to fetch the latest kind statistics."""
  kind_stat_query = query_pb2.Query()
  if namespace is None:
    kind_stat_query.kind.add().name = '__Stat_Kind__'
  else:
    kind_stat_query.kind.add().name = '__Stat_Ns_Kind__'

  kind_filter = datastore_helper.set_property_filter(
      query_pb2.Filter(), 'kind_name', PropertyFilter.EQUAL, unicode(kind))
  timestamp_filter = datastore_helper.set_property_filter(
      query_pb2.Filter(), 'timestamp', PropertyFilter.EQUAL,
      latest_timestamp)

  datastore_helper.set_composite_filter(kind_stat_query.filter,
                                        CompositeFilter.AND, kind_filter,
                                        timestamp_filter)
  return kind_stat_query


class QueryIterator(object):
  """A iterator class for entities of a given query.

  Entities are read in batches. Retries on failures.
  """
  _NOT_FINISHED = QUERY_NOT_FINISHED
  # Maximum number of results to request per query.
  _BATCH_SIZE = 500

  def __init__(self, project, namespace, query, datastore):
    self._query = query
    self._datastore = datastore
    self._project = project
    self._namespace = namespace
    self._start_cursor = None
    self._limit = self._query.limit.value or sys.maxint
    self._req = make_request(project, namespace, query)

  @retry.with_exponential_backoff(num_retries=5,
                                  retry_filter=retry_on_rpc_error)
  def _next_batch(self):
    """Fetches the next batch of entities."""
    if self._start_cursor is not None:
      self._req.query.start_cursor = self._start_cursor

    # set batch size
    self._req.query.limit.value = min(self._BATCH_SIZE, self._limit)
    resp = self._datastore.run_query(self._req)
    return resp

  def __iter__(self):
    more_results = True
    while more_results:
      resp = self._next_batch()
      for entity_result in resp.batch.entity_results:
        yield entity_result.entity

      self._start_cursor = resp.batch.end_cursor
      num_results = len(resp.batch.entity_results)
      self._limit -= num_results

      # Check if we need to read more entities.
      # True when query limit hasn't been satisfied and there are more entities
      # to be read. The latter is true if the response has a status
      # `NOT_FINISHED` or if the number of results read in the previous batch
      # is equal to `_BATCH_SIZE` (all indications that there is more data be
      # read).
      more_results = ((self._limit > 0) and
                      ((num_results == self._BATCH_SIZE) or
                       (resp.batch.more_results == self._NOT_FINISHED)))
