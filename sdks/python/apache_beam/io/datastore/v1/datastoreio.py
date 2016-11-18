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

"""A source and a sink for reading from and writing to Google Cloud Datastore"""

import logging

from googledatastore import helper as datastore_helper

from apache_beam.io.datastore.v1 import helper
from apache_beam.io.datastore.v1 import query_splitter
from apache_beam.transforms import Create
from apache_beam.transforms import DoFn
from apache_beam.transforms import FlatMap
from apache_beam.transforms import GroupByKey
from apache_beam.transforms import PTransform
from apache_beam.transforms import ParDo
from apache_beam.transforms.util import Values

__all__ = ['ReadFromDatastore']


class ReadFromDatastore(PTransform):
  """A ``PTransform`` for reading from Google Cloud Datastore.

  To read a `PCollection[Entity]` from a Cloud Datastore `Query`, use
  `ReadFromDatastore` transform by providing a `project` id and a `query` to
  read from. You can optionally provide a `namespace` and/or specify how many
  splits you want for the query through `num_splits` option.

  Note: Normally, a Runner will read from Cloud Datastore in parallel across
  many workers. However, when the `query` is configured with a `limit` or if the
  query contains inequality filters like `GREATER_THAN, LESS_THAN` etc., then
  all the returned results will be read by a single worker in order to ensure
  correct data. Since data is read from a single worker, this could have
  significant impact on the performance of the job.

  The semantics for the query splitting is defined below:
    1. Any value for `num_splits` less than or equal to 0 will be ignored, and
    the number of splits will be chosen dynamically at runtime based on the
    query data size.

    2. Any value ofr `num_splits` greater than
    `ReadFromDatastore._NUM_QUERY_SPLITS_MAX` will be capped at that value.

    3. If the `query` has a user limit set, or contains inequality filters, then
    `num_splits` will be ignored and no split will be performed.

    4. Under certain cases Cloud Datastore is unable to split query to the
    requested number of splits. In such cases we just use whatever the Cloud
    Datastore returns.

  See https://developers.google.com/datastore/ for more details on Google Cloud
  Datastore.
  """

  # An upper bound on the number of splits for a query.
  _NUM_QUERY_SPLITS_MAX = 50000
  # A lower bound on the number of splits for a query.
  _NUM_QUERY_SPLITS_MIN = 12
  # Default bundle size of 64MB.
  _DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024

  def __init__(self, project, query, namespace=None, num_splits=0):
    """Initialize the ReadFromDatastore transform.

    Args:
      project: The Project ID
      query: The Cloud Datastore query to be read from.
      namespace: An optional namespace.
      num_splits: Number of splits for the query.
    """
    super(ReadFromDatastore, self).__init__()

    if not project:
      ValueError("Project cannot be empty")
    if not query:
      ValueError("Query cannot be empty")

    self._project = project
    # using _namespace conflicts with DisplayData._namespace
    self._datastore_namespace = namespace
    self._query = query
    self._num_splits = num_splits

  def apply(self, pcoll):
    # This is a composite transform involves the following steps:
    #   1. Create a singleton of the user provided `query` and apply a `ParDo`
    #   that splits the query into `num_splits` and assign each split query a
    #   unique `int` as the key. The resulting output is of the type
    #   `PCollection[(int, Query)]`.
    #
    #   If the value of `numQuerySplits` is less than or equal to 0, then the
    #   number of splits will be computed dynamically based on the size of the
    #   data for the `query`.
    #
    #   2. The resulting `PCollection` is sharded using a `GroupByKey`
    #   operation. The queries are extracted from the (int, Iterable[Query]) and
    #   flattened to output a `PCollection[Query]`.
    #
    #   3. In the third step, a `ParDo` reads entities for each query and
    #   outputs a `PCollection[Entity]`.

    queries = (pcoll.pipeline
               | 'User Query' >> Create([self._query])
               | 'Split Query' >> ParDo(ReadFromDatastore.SplitQueryFn(
                   self._project, self._query, self._datastore_namespace,
                   self._num_splits)))

    sharded_queries = queries | GroupByKey() | Values() | FlatMap('flatten',
                                                                  lambda x: x)

    entities = sharded_queries | 'Read' >> ParDo(
        ReadFromDatastore.ReadFn(self._project, self._datastore_namespace))
    return entities

  def display_data(self):
    disp_data = {'project': self._project,
                 'query': str(self._query),
                 'num_splits': self._num_splits}

    if self._datastore_namespace is not None:
      disp_data['namespace'] = self._datastore_namespace

    return disp_data

  class SplitQueryFn(DoFn):
    """A `DoFn` that splits a given query into multiple sub-queries."""

    def __init__(self, project, query, namespace, num_splits):
      super(ReadFromDatastore.SplitQueryFn, self).__init__()
      self._datastore = None
      self._project = project
      # using _namespace conflicts with DisplayData._namespace
      self._datastore_namespace = namespace
      self._query = query
      self._num_splits = num_splits

    def start_bundle(self, context):
      self._datastore = helper.get_datastore(self._project)

    def process(self, p_context, *args, **kwargs):
      # random key to be used to group query splits.
      key = 1
      query = p_context.element

      # If query has a user set limit, then the query cannot be split.
      if query.HasField('limit'):
        return [(key, query)]

      # Compute the estimated numSplits if not specified by the user.
      if self._num_splits <= 0:
        estimated_num_splits = ReadFromDatastore.get_estimated_num_splits(
            self._project, self._datastore_namespace, self._query,
            self._datastore)
      else:
        estimated_num_splits = self._num_splits

      logging.info("Splitting the query into %d splits", estimated_num_splits)
      try:
        query_splits = query_splitter.get_splits(
            self._datastore, query, estimated_num_splits,
            helper.make_partition(self._project, self._datastore_namespace))
      except Exception:
        logging.warning("Unable to parallelize the given query: %s", query,
                        exc_info=True)
        query_splits = [(key, query)]

      sharded_query_splits = []
      for split_query in query_splits:
        sharded_query_splits.append((key, split_query))
        key += 1

      return sharded_query_splits

    def display_data(self):
      disp_data = {'project': self._project,
                   'query': str(self._query),
                   'num_splits': self._num_splits}

      if self._datastore_namespace is not None:
        disp_data['namespace'] = self._datastore_namespace

      return disp_data

  class ReadFn(DoFn):
    """A DoFn that reads entities from Cloud Datastore, for a given query."""

    def __init__(self, project, namespace=None):
      super(ReadFromDatastore.ReadFn, self).__init__()
      self._project = project
      # using _namespace conflicts with DisplayData._namespace
      self._datastore_namespace = namespace
      self._datastore = None

    def start_bundle(self, context):
      self._datastore = helper.get_datastore(self._project)

    def process(self, p_context, *args, **kwargs):
      query = p_context.element
      # Returns an iterator of entities that reads is batches.
      entities = helper.fetch_entities(self._project, self._datastore_namespace,
                                       query, self._datastore)
      return entities

    def display_data(self):
      disp_data = {'project': self._project}

      if self._datastore_namespace is not None:
        disp_data['namespace'] = self._datastore_namespace

      return disp_data

  @staticmethod
  def query_latest_statistics_timestamp(project, namespace, datastore):
    """Fetches the latest timestamp of statistics from Cloud Datastore.

    Cloud Datastore system tables with statistics are periodically updated.
    This method fethes the latest timestamp (in microseconds) of statistics
    update using the `__Stat_Total__` table.
    """
    query = helper.make_latest_timestamp_query(namespace)
    req = helper.make_request(project, namespace, query)
    resp = datastore.run_query(req)
    if len(resp.batch.entity_results) == 0:
      raise RuntimeError("Datastore total statistics unavailable.")

    entity = resp.batch.entity_results[0].entity
    return datastore_helper.micros_from_timestamp(
        entity.properties['timestamp'].timestamp_value)

  @staticmethod
  def get_estimated_size_bytes(project, namespace, query, datastore):
    """Get the estimated size of the data returned by the given query.

    Cloud Datastore provides no way to get a good estimate of how large the
    result of a query entity kind being queried, using the __Stat_Kind__ system
    table, assuming exactly 1 kind is specified in the query.
    See https://cloud.google.com/datastore/docs/concepts/stats.
    """
    kind = query.kind[0].name
    latest_timestamp = ReadFromDatastore.query_latest_statistics_timestamp(
        project, namespace, datastore)
    logging.info('Latest stats timestamp for kind %s is %s',
                 kind, latest_timestamp)

    kind_stats_query = \
        helper.make_kind_stats_query(namespace, kind, latest_timestamp)

    req = helper.make_request(project, namespace, kind_stats_query)
    resp = datastore.run_query(req)
    if len(resp.batch.entity_results) == 0:
      raise RuntimeError("Datastore statistics for kind %s unavailable" % kind)

    entity = resp.batch.entity_results[0].entity
    return datastore_helper.get_value(entity.properties['entity_bytes'])

  @staticmethod
  def get_estimated_num_splits(project, namespace, query, datastore):
    """Computes the number of splits to be performed on the given query."""
    try:
      estimated_size_bytes = \
        ReadFromDatastore.get_estimated_size_bytes(project, namespace, query,
                                                   datastore)
      logging.info('Estimated size bytes for query: %s', estimated_size_bytes)
      num_splits = int(min(ReadFromDatastore._NUM_QUERY_SPLITS_MAX, round(
          ((float(estimated_size_bytes)) /
           ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES))))

    except Exception as e:
      logging.warning('Failed to fetch estimated size bytes:\n%s', e)
      # Fallback in case estimated size is unavailable.
      num_splits = ReadFromDatastore._NUM_QUERY_SPLITS_MIN

    return max(num_splits, ReadFromDatastore._NUM_QUERY_SPLITS_MIN)
