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

"""Cloud Datastore helper functions.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import errno
import logging
import sys
import time
from builtins import object
from socket import error as SocketError

from future.builtins import next
from past.builtins import unicode

# pylint: disable=ungrouped-imports
from apache_beam.internal.gcp import auth
from apache_beam.utils import retry

# TODO: remove unused imports in try-except blocks. maybe linters can detected these?

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.api_core import exceptions
  #from google.cloud.datastore_v1.proto import datastore_pb2
  #from google.cloud.datastore_v1.proto import entity_pb2
  #from google.cloud.datastore_v1.proto import query_pb2
  from google.cloud.datastore import client
  from google.rpc import code_pb2
  #from googledatastore import PropertyFilter, CompositeFilter
  #from googledatastore import helper as datastore_helper
  #from googledatastore.connection import Datastore
  #from googledatastore.connection import RPCError
  from google.cloud.exceptions import GoogleCloudError
  from cachetools.func import ttl_cache
except ImportError:
  exceptions = None
  ttl_cache = lambda **unused_kwargs: lambda unused_fn: None
# pylint: enable=wrong-import-order, wrong-import-position

# pylint: enable=ungrouped-imports

# TODO: clean up pylint pragmas


if exceptions is not None:
  # https://cloud.google.com/datastore/docs/concepts/errors#error_codes
  _RETRYABLE_DATASTORE_ERRORS = (
    exceptions.Aborted,
    exceptions.DeadlineExceeded,
    exceptions.InternalServerError,
    exceptions.ServiceUnavailable,
  )

# TODO: remove unused functions

# TODO: remove
# def key_comparator(k1, k2):
#   """A comparator for Datastore keys.
#
#   Comparison is only valid for keys in the same partition. The comparison here
#   is between the list of paths for each key.
#
#   Args:
#     k1, k2: (``apache_beam.io.gcp.datastore.v1new.types.Key``) Keys to compare.
#   """
#
#   if k1.partition_id != k2.partition_id:
#     raise ValueError('Cannot compare keys with different partition ids.')
#
#   k2_iter = iter(k2.path)
#
#   for k1_path in k1.path:
#     k2_path = next(k2_iter, None)
#     if not k2_path:
#       return 1
#
#     result = compare_path(k1_path, k2_path)
#
#     if result != 0:
#       return result
#
#   k2_path = next(k2_iter, None)
#   if k2_path:
#     return -1
#   return 0

# TODO: remove
# def compare_path(p1, p2):
#   """A comparator for key path.
#
#   A path has either an `id` or a `name` field defined. The
#   comparison works with the following rules:
#
#   1. If one path has `id` defined while the other doesn't, then the
#   one with `id` defined is considered smaller.
#   2. If both paths have `id` defined, then their ids are compared.
#   3. If no `id` is defined for both paths, then their `names` are compared.
#   """
#
#   result = (p1.kind > p2.kind) - (p1.kind < p2.kind)
#   if result != 0:
#     return result
#
#   if p1.HasField('id'):
#     if not p2.HasField('id'):
#       return -1
#
#     return (p1.id > p2.id) - (p1.id < p2.id)
#
#   if p2.HasField('id'):
#     return 1
#
#   return (p1.name > p2.name) - (p1.name < p2.name)


@ttl_cache(maxsize=128, ttl=3600)
def get_client(project, namespace):
  """Returns a Cloud Datastore client."""
  # TODO: remove
  print('get_client(%s, %s)' % (project, namespace))
  _client = client.Client(project=project, namespace=namespace)
  _client.base_url = 'https://batch-datastore.googleapis.com'  # BEAM-1387
  return _client


def retry_on_rpc_error(exception):
  """A retry filter for Cloud Datastore RPCErrors."""
  return isinstance(exception, _RETRYABLE_DATASTORE_ERRORS)


# TODO: unit test
@retry.with_exponential_backoff(num_retries=5,
                                retry_filter=retry_on_rpc_error)
def write_mutations(batch, throttler, rpc_stats_callback, throttle_delay=1):
  """A helper function to write a batch of mutations to Cloud Datastore.

  Retries are handled by Datastore client.
  If a commit fails, it will be retried upto 5 times. All mutations in the
  batch will be committed again, even if the commit was partially successful.
  If the retry limit is exceeded, the last exception from Cloud Datastore will
  be raised.

  Args:
    batch: An instance of ``google.cloud.datastore.batch.Batch`` that has an
      in-progress batch.
    rpc_stats_callback: a function to call with arguments `successes` and
        `failures` and `throttled_secs`; this is called to record successful
        and failed RPCs to Datastore and time spent waiting for throttling.
    throttler: AdaptiveThrottler, to use to select requests to be throttled.
    throttle_delay: float, time in seconds to sleep when throttled.

  Returns:
    (int) The latency of the successful RPC in milliseconds.
  """
  # Client-side throttling.
  while throttler.throttle_request(time.time() * 1000):
    logging.info("Delaying request for %ds due to previous failures",
                 throttle_delay)
    time.sleep(throttle_delay)
    rpc_stats_callback(throttled_secs=throttle_delay)

  try:
    start_time = time.time()
    batch.commit()
    end_time = time.time()

    rpc_stats_callback(successes=1)
    throttler.successful_request(start_time * 1000)
    commit_time_ms = int((end_time-start_time) * 1000)
    return commit_time_ms
  except Exception:
    rpc_stats_callback(errors=1)
    raise


# TODO: remove
# def make_latest_timestamp_query(client, namespace):
#   """Make a Query to fetch the latest timestamp statistics."""
#
#   if namespace is None:
#     kind = '__Stat_Total__'
#   else:
#     kind = '__Stat_Ns_Total__'
#   query = client.query(kind=kind,
#                        namespace=namespace,
#                        order=["-timestamp",])
#   return query
#
#   # Only get the latest entity
#   # TODO: limit
#   query.limit.value = 1
#   return query


# TODO: remove
# def make_kind_stats_query(namespace, kind, latest_timestamp):
#   """Make a Query to fetch the latest kind statistics."""
#   kind_stat_query = query_pb2.Query()
#   if namespace is None:
#     kind_stat_query.kind.add().name = '__Stat_Kind__'
#   else:
#     kind_stat_query.kind.add().name = '__Stat_Ns_Kind__'
#
#   kind_filter = datastore_helper.set_property_filter(
#       query_pb2.Filter(), 'kind_name', PropertyFilter.EQUAL,
#       unicode(kind))
#   timestamp_filter = datastore_helper.set_property_filter(
#       query_pb2.Filter(), 'timestamp', PropertyFilter.EQUAL,
#       latest_timestamp)
#
#   datastore_helper.set_composite_filter(kind_stat_query.filter,
#                                         CompositeFilter.AND, kind_filter,
#                                         timestamp_filter)
#   return kind_stat_query
