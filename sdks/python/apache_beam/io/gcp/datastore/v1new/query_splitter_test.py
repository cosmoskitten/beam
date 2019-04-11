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

"""Cloud Datastore query splitter test."""

from __future__ import absolute_import

import unittest

import mock

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.gcp.datastore.v1new import testing
  from apache_beam.io.gcp.datastore.v1new import query_splitter
  from apache_beam.io.gcp.datastore.v1new import types
  from apache_beam.io.gcp.datastore.v1new.query_splitter import SplitNotPossibleError
  from google.cloud.datastore import key
# TODO(BEAM-4543): Remove TypeError once googledatastore dependency is removed.
except (ImportError, TypeError):
  query_splitter = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(query_splitter is None, 'GCP dependencies are not installed')
class QuerySplitterTest(unittest.TestCase):
  _PROJECT = 'project'
  _NAMESPACE = 'namespace'

  def test_get_splits_query_with_order(self):
    query = types.Query(kind='kind', order=['prop1'])
    with self.assertRaisesRegexp(SplitNotPossibleError, r'sort orders'):
      query_splitter.get_splits(None, query, 3)

  def test_get_splits_query_with_unsupported_filter(self):
    query = types.Query(kind='kind', filters=[('prop1', '>', 'value1')])
    with self.assertRaisesRegexp(SplitNotPossibleError, r'inequality filters'):
      query_splitter.get_splits(None, query, 2)

  def test_get_splits_query_with_limit(self):
    query = types.Query(limit=10)
    with self.assertRaisesRegexp(SplitNotPossibleError, r'limit set'):
      query_splitter.get_splits(None, query, 2)

  def test_get_splits_query_with_num_splits_of_one(self):
    query = types.Query()
    with self.assertRaisesRegexp(SplitNotPossibleError, r'num_splits'):
      query_splitter.get_splits(None, query, 1)

  def test_create_scatter_query(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 10
    scatter_query = query_splitter._create_scatter_query(query, num_splits)
    self.assertEqual(scatter_query.kind, query.kind)
    self.assertEqual(scatter_query.limit,
                     (num_splits -1) * query_splitter.KEYS_PER_SPLIT)
    self.assertEqual(scatter_query.order,
                     [query_splitter.SCATTER_PROPERTY_NAME])
    self.assertEqual(scatter_query.projection,
                     [query_splitter.KEY_PROPERTY_NAME])

  def test_get_splits_with_no_entities(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 2
    num_entities = 0
    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_two_splits(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 2
    num_entities = 97
    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_multiple_splits(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 4
    num_entities = 369
    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_num_splits_gt_entities(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 10
    num_entities = 4
    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_small_num_entities(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 4
    num_entities = 50
    self.check_get_splits(query, num_splits, num_entities)

  def check_get_splits(self, query, num_splits, num_entities):
    """A helper method to test the query_splitter get_splits method.

    Args:
      query: the query to be split
      num_splits: number of splits
      num_entities: number of scatter entities returned to the splitter.
    """
    # Test for both random long ids and string ids.
    for id_or_name in [True, False]:
      client_entities = testing.create_client_entities(num_entities, id_or_name)

      mock_client = mock.MagicMock()
      mock_client_query = mock.MagicMock()
      mock_client_query.fetch.return_value = client_entities
      with mock.patch.object(
          types.Query, '_to_client_query', return_value=mock_client_query):
        split_queries = query_splitter.get_splits(
            mock_client, query, num_splits)

      mock_client_query.fetch.assert_called_once()
      # if request num_splits is greater than num_entities, the best it can
      # do is one entity per split.
      expected_num_splits = min(num_splits, num_entities + 1)
      self.assertEqual(len(split_queries), expected_num_splits)

      # Verify no gaps in key ranges. Filters should look like:
      # query1: (__key__ < key1)
      # query2: (__key__ >= key1), (__key__ < key2)
      # ...
      # queryN: (__key__ >=keyN-1)
      prev_client_key = None
      last_query_seen = False
      for split_query in split_queries:
        self.assertFalse(last_query_seen)
        lt_key = None
        gte_key = None
        for _filter in split_query.filters:
          self.assertEqual(query_splitter.KEY_PROPERTY_NAME, _filter[0])
          if _filter[1] == '<':
            lt_key = _filter[2]
          elif _filter[1] == '>=':
            gte_key = _filter[2]

        # Case where the scatter query has no results.
        if lt_key is None and gte_key is None:
          self.assertEqual(1, len(split_queries))
          break

        if prev_client_key is None:
          self.assertIsNone(gte_key)
          self.assertIsNotNone(lt_key)
          prev_client_key = lt_key
        else:
          self.assertEqual(prev_client_key, gte_key)
          prev_client_key = lt_key
          if lt_key is None:
            last_query_seen = True

  def test_client_key_sort_key(self):
    k = key.Key('kind1', 1, project=self._PROJECT, namespace=self._NAMESPACE)
    k2 = key.Key('kind2', 'a', parent=k)
    k3 = key.Key('kind2', 'b', parent=k)
    k4 = key.Key('kind1', 'a', project=self._PROJECT, namespace=self._NAMESPACE)
    k5 = key.Key('kind1', 'a', project=self._PROJECT)
    keys = [k5, k, k4, k3, k2, k2, k]
    expected_sort = [k5, k, k, k2, k2, k3, k4]
    keys.sort(key=query_splitter.client_key_sort_key)
    self.assertEqual(expected_sort, keys)


if __name__ == '__main__':
  unittest.main()
