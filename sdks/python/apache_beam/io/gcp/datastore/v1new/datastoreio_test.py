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

"""Unit tests for datastoreio."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import math
import unittest
from builtins import zip

from mock import MagicMock
from mock import call
from mock import patch

# Protect against environments where datastore library is not available.
try:
  from apache_beam.io.gcp.datastore.v1new import testing
  from apache_beam.io.gcp.datastore.v1new import helper
  from apache_beam.io.gcp.datastore.v1new import query_splitter
  from apache_beam.io.gcp.datastore.v1new.datastoreio import DeleteFromDatastore
  from apache_beam.io.gcp.datastore.v1new.datastoreio import QueryDatastore
  from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
  from apache_beam.io.gcp.datastore.v1new.datastoreio import _Mutate
  from google.cloud.datastore import client
  from google.cloud.datastore import helpers
# TODO(BEAM-4543): Remove TypeError once googledatastore dependency is removed.
except (ImportError, TypeError):
  client = None


class FakeMutation(object):
  def __init__(self, entity=None, key=None):
    """Fake mutation request object.

    Requires exactly one of entity or key to be set.

    Args:
      entity: (``google.cloud.datastore.entity.Entity``) entity representing
        this upsert mutation
      key: (``google.cloud.datastore.key.Key``) key representing
        this delete mutation
    """
    self.entity = entity
    self.key = key

  def ByteSize(self):
    if self.entity is not None:
      return helpers.entity_to_protobuf(self.entity).ByteSize()
    else:
      return self.key.to_protobuf().ByteSize()


class FakeBatch(object):
  def __init__(self, all_batch_items=None, commit_count=None):
    """Fake ``google.cloud.datastore.batch.Batch`` object.

    Args:
      all_batch_items: (list) If set, will append all entities/keys added to
        this batch.
      commit_count: (list of int) If set, will increment commit_count[0] on
        each ``commit``.
    """
    self._all_batch_items = all_batch_items
    self._commit_count = commit_count
    self.mutations = []

  def put(self, entity):
    self.mutations.append(FakeMutation(entity=entity))
    if self._all_batch_items is not None:
      self._all_batch_items.append(entity)

  def delete(self, key):
    self.mutations.append(FakeMutation(key=key))
    if self._all_batch_items is not None:
      self._all_batch_items.append(key)

  def begin(self):
    pass

  def commit(self):
    if self._commit_count:
      self._commit_count[0] += 1


@unittest.skipIf(client is None, 'Datastore dependencies are not installed')
class DatastoreioTest(unittest.TestCase):
  _PROJECT = 'project'
  _KIND = 'kind'
  _NAMESPACE = 'namespace'

  def setUp(self):
    self._mock_client = MagicMock()
    self._mock_client.project = self._PROJECT
    self._mock_client.namespace = self._NAMESPACE
    self._mock_query = MagicMock()
    self._mock_query.limit = None
    self._mock_query.order = None

    self._real_client = client.Client(
        project=self._PROJECT, namespace=self._NAMESPACE,
        # Don't do any network requests.
        _http=MagicMock())

  def test_get_estimated_size_bytes_without_namespace(self):
    self._mock_client.namespace = None
    entity_bytes = 100
    timestamp = datetime.datetime(2019, 3, 14, 15, 9, 26, 535897)
    self.check_estimated_size_bytes(entity_bytes, timestamp)

  def test_get_estimated_size_bytes_with_namespace(self):
    entity_bytes = 100
    timestamp = datetime.datetime(2019, 3, 14, 15, 9, 26, 535897)
    self.check_estimated_size_bytes(entity_bytes, timestamp, self._NAMESPACE)

  def test_SplitQueryFn_with_num_splits(self):
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      num_splits = 23
      expected_num_splits = 23

      def fake_get_splits(unused_client, query, num_splits):
        return [query] * num_splits

      with patch.object(query_splitter, 'get_splits',
                        side_effect=fake_get_splits):
        split_query_fn = QueryDatastore.SplitQueryFn(num_splits)
        split_queries = split_query_fn.process(self._mock_query)

        self.assertEqual(expected_num_splits, len(split_queries))
        self.assertEqual(0, len(self._mock_client.run_query.call_args_list))
        self.verify_unique_keys(split_queries)

  def test_SplitQueryFn_without_num_splits(self):
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      # Force SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 23
      entity_bytes = (expected_num_splits *
                      QueryDatastore._DEFAULT_BUNDLE_SIZE_BYTES)
      with patch.object(
          QueryDatastore.SplitQueryFn, 'get_estimated_size_bytes',
          return_value=entity_bytes):

        def fake_get_splits(unused_client, query, num_splits):
          return [query] * num_splits

        with patch.object(query_splitter, 'get_splits',
                          side_effect=fake_get_splits):
          split_query_fn = QueryDatastore.SplitQueryFn(num_splits)
          split_queries = split_query_fn.process(self._mock_query)

          self.assertEqual(expected_num_splits, len(split_queries))
          self.assertEqual(0, len(self._mock_client.run_query.call_args_list))
          self.verify_unique_keys(split_queries)

  def test_SplitQueryFn_with_query_limit(self):
    """A test that verifies no split is performed when the query has a limit."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      num_splits = 4
      expected_num_splits = 1
      self._mock_query.limit = 3
      split_query_fn = QueryDatastore.SplitQueryFn(num_splits)
      split_queries = split_query_fn.process(self._mock_query)

      self.assertEqual(expected_num_splits, len(split_queries))
      self._mock_client.assert_not_called()

  def test_SplitQueryFn_with_exception(self):
    """A test that verifies that no split is performed when failures occur."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      # Force SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 1
      entity_bytes = (expected_num_splits *
                      QueryDatastore._DEFAULT_BUNDLE_SIZE_BYTES)
      with patch.object(
          QueryDatastore.SplitQueryFn, 'get_estimated_size_bytes',
          return_value=entity_bytes):

        with patch.object(query_splitter, 'get_splits',
                          side_effect=query_splitter.QuerySplitterError(
                              "Testing query split error")):
          split_query_fn = QueryDatastore.SplitQueryFn(num_splits)
          split_queries = split_query_fn.process(self._mock_query)

          self.assertEqual(expected_num_splits, len(split_queries))
          self.assertEqual(self._mock_query, split_queries[0][1])
          self._mock_client.run_query.assert_not_called()
          self.assertEqual(0,
                           len(self._mock_client.run_query.call_args_list))
          self.verify_unique_keys(split_queries)

  def test_DatastoreWriteFn_with_emtpy_batch(self):
    self.check_DatastoreWriteFn(0)

  def test_DatastoreWriteFn_with_one_batch(self):
    num_entities_to_write = _Mutate._WRITE_BATCH_INITIAL_SIZE * 1 - 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_multiple_batches(self):
    num_entities_to_write = _Mutate._WRITE_BATCH_INITIAL_SIZE * 3 + 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_batch_size_exact_multiple(self):
    num_entities_to_write = _Mutate._WRITE_BATCH_INITIAL_SIZE * 2
    self.check_DatastoreWriteFn(num_entities_to_write)

  def check_DatastoreWriteFn(self, num_entities):
    """A helper function to test DatastoreWriteFn."""

    with patch.object(helper, 'get_client', return_value=self._mock_client):
      entities = testing.create_entities(num_entities)
      expected_entities = [entity.to_client_entity() for entity in entities]

      all_batch_entities = []
      commit_count = [0]
      self._mock_client.batch.side_effect = (
          lambda: FakeBatch(all_batch_items=all_batch_entities,
                            commit_count=commit_count))

      fixed_batch_size = WriteToDatastore._WRITE_BATCH_INITIAL_SIZE
      datastore_write_fn = WriteToDatastore.DatastoreWriteFn(
          self._PROJECT, fixed_batch_size=fixed_batch_size)

      datastore_write_fn.start_bundle()
      for entity in entities:
        datastore_write_fn.process(entity)
      datastore_write_fn.finish_bundle()

      self.assertListEqual([e.key for e in all_batch_entities],
                           [e.key for e in expected_entities])
      batch_count = math.ceil(num_entities / fixed_batch_size)
      self.assertEqual(batch_count, commit_count[0])

  def test_DatastoreWriteLargeEntities(self):
    """100*100kB entities gets split over two Commit RPCs."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      entities = testing.create_entities(100)
      commit_count = [0]
      self._mock_client.batch.side_effect = (
          lambda: FakeBatch(commit_count=commit_count))

      datastore_write_fn = WriteToDatastore.DatastoreWriteFn(
          self._PROJECT, fixed_batch_size=_Mutate._WRITE_BATCH_INITIAL_SIZE)
      datastore_write_fn.start_bundle()
      for entity in entities:
        entity.set_properties({'large': u'A' * 100000})
        datastore_write_fn.process(entity)
      datastore_write_fn.finish_bundle()

      self.assertEqual(2, commit_count[0])

  def verify_unique_keys(self, queries):
    """A helper function that verifies if all the queries have unique keys."""
    keys, _ = zip(*queries)
    keys = set(keys)
    self.assertEqual(len(keys), len(queries))

  def check_estimated_size_bytes(self, entity_bytes, timestamp, namespace=None):
    """A helper method to test get_estimated_size_bytes"""
    self._mock_client.query.return_value = self._mock_query
    self._mock_query.project = self._PROJECT
    self._mock_query.namespace = namespace
    self._mock_query.fetch.side_effect = [
        [{'timestamp': timestamp}],
        [{'entity_bytes': entity_bytes}],
    ]
    self._mock_query.kind = self._KIND

    split_query_fn = QueryDatastore.SplitQueryFn(num_splits=0)
    self.assertEqual(entity_bytes,
                     split_query_fn.get_estimated_size_bytes(self._mock_client,
                                                             self._mock_query))

    if namespace is None:
      ns_keyword = '_'
    else:
      ns_keyword = '_Ns_'
    self._mock_client.query.assert_has_calls([
        call(kind='__Stat%sTotal__' % ns_keyword, order=['-timestamp']),
        call().fetch(limit=1),
        call(kind='__Stat%sKind__' % ns_keyword),
        call().add_filter('kind_name', '=', self._KIND),
        call().add_filter('timestamp', '=', timestamp),
        call().fetch(limit=1),
    ])

  def test_DatastoreDeleteFn(self):
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      keys = [entity.key for entity in testing.create_entities(10)]
      expected_keys = [key.to_client_key() for key in keys]

      all_batch_keys = []
      self._mock_client.batch.side_effect = (
          lambda: FakeBatch(all_batch_items=all_batch_keys))

      fixed_batch_size = _Mutate._WRITE_BATCH_INITIAL_SIZE
      datastore_delete_fn = DeleteFromDatastore.DatastoreDeleteFn(
          self._PROJECT, fixed_batch_size=fixed_batch_size)

      datastore_delete_fn.start_bundle()
      for key in keys:
        datastore_delete_fn.process(key)
        datastore_delete_fn.finish_bundle()

      self.assertListEqual(all_batch_keys, expected_keys)


@unittest.skipIf(client is None, 'Datastore dependencies are not installed')
class DynamicWriteBatcherTest(unittest.TestCase):

  def setUp(self):
    self._batcher = _Mutate._DynamicBatchSizer()

  # If possible, keep these test cases aligned with the Java test cases in
  # DatastoreV1Test.java
  def test_no_data(self):
    self.assertEquals(_Mutate._WRITE_BATCH_INITIAL_SIZE,
                      self._batcher.get_batch_size(0))

  def test_fast_queries(self):
    self._batcher.report_latency(0, 1000, 200)
    self._batcher.report_latency(0, 1000, 200)
    self.assertEquals(_Mutate._WRITE_BATCH_MAX_SIZE,
                      self._batcher.get_batch_size(0))

  def test_slow_queries(self):
    self._batcher.report_latency(0, 10000, 200)
    self._batcher.report_latency(0, 10000, 200)
    self.assertEquals(100, self._batcher.get_batch_size(0))

  def test_size_not_below_minimum(self):
    self._batcher.report_latency(0, 30000, 50)
    self._batcher.report_latency(0, 30000, 50)
    self.assertEquals(_Mutate._WRITE_BATCH_MIN_SIZE,
                      self._batcher.get_batch_size(0))

  def test_sliding_window(self):
    self._batcher.report_latency(0, 30000, 50)
    self._batcher.report_latency(50000, 5000, 200)
    self._batcher.report_latency(100000, 5000, 200)
    self.assertEquals(200, self._batcher.get_batch_size(150000))


if __name__ == '__main__':
  unittest.main()
