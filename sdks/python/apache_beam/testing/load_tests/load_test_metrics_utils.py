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
Utility functions used for integrating Metrics API into load tests pipelines.
"""

from __future__ import absolute_import

import logging
import time

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

import apache_beam as beam
from apache_beam.metrics import Metrics

RUNTIME_LABEL = 'runtime'
SUBMIT_TIMESTAMP_LABEL = 'submit_timestamp'
LOAD_TEST_DATASET_NAME = 'python_load_tests'


def _get_schema_field(schema_field):
  return SchemaField(
      name=schema_field['name'],
      field_type=schema_field['type'],
      mode=schema_field['mode'])

class BigQueryConnector(object):
  def __init__(self, project_name, namespace, schema_map):
    self._namespace = namespace
    bq_client = bigquery.Client(project=project_name)
    schema = self._parse_schema(schema_map)
    schema = self._prepare_schema(schema)
    self._get_or_create_table(schema, bq_client)

  def match_and_save(self, result_list):
    rows_tuple = tuple(self._match_inserts_by_schema(result_list))
    self._insert_data(rows_tuple)

  def _match_inserts_by_schema(self, insert_list):
    for name in insert_list:
      yield self._get_element_by_schema(name, insert_list)

  def _get_element_by_schema(self, schema_name, schema_list):
    for metric in schema_list:
      if metric['label'] == schema_name:
        return metric['value']
    return None

  def _insert_data(self, rows_tuple):
    job = self._bq_table.insert_data(rows=[rows_tuple])
    if len(job) > 0 and len(job[0]['errors']) > 0:
      for err in job[0]['errors']:
        raise ValueError(err['message'])

  def _get_dataset(self, bq_client):
    bq_dataset = bq_client.dataset(LOAD_TEST_DATASET_NAME)
    if not bq_dataset.exists():
      raise ValueError(
          'Dataset {} does not exist in your project. '
          'You have to create table first.'
            .format(self._namespace))
    return bq_dataset

  def _get_or_create_table(self, bq_schemas, bq_client):
    self._bq_table = self._get_dataset(bq_client).table(self._namespace, bq_schemas)
    if self._namespace is '':
      raise ValueError('Namespace cannot be empty.')

    if not self._bq_table.exists():
      self._bq_table.create()

  def _parse_schema(self, schema_map):
    return [{'name': SUBMIT_TIMESTAMP_LABEL,
                'type': 'TIMESTAMP',
                'mode': 'REQUIRED'}] + schema_map

  def _prepare_schema(self, schemas):
    return [_get_schema_field(schema) for schema in schemas]

  def _get_schema_names(self, schemas):
    return [schema['name'] for schema in schemas]



class Monitor(object):
  def __init__(self, project_name, namespace, schema_map):
    if project_name is not None:
      self.bq = BigQueryConnector(project_name, namespace, schema_map)

  def send_metrics(self, result):
    metrics = result.metrics().query()
    counters = metrics['counters']
    counters_list = []
    if len(counters) > 0:
      counters_list = self._prepare_counter_metrics(counters)

    distributions = metrics['distributions']
    dist_list = []
    if len(distributions) > 0:
      dist_list = self._prepare_runtime_metrics(distributions)

    timestamp = {'label': SUBMIT_TIMESTAMP_LABEL, 'value': time.time()}

    insert_list = [timestamp] + dist_list + counters_list
    self.bq.match_and_save(insert_list)


  def _prepare_counter_metrics(self, counters):
    for counter in counters:
      logging.info("Counter:  %s", counter)
    counters_list = []
    for counter in counters:
      counter_commited = counter.committed
      counter_label = str(counter.key.metric.name)
      counters_list.append({'label': counter_label, 'value': counter_commited})

    return counters_list

  # prepares distributions of start and end metrics to show test runtime
  def _prepare_runtime_metrics(self, distributions):
    for dist in distributions:
      logging.info("Distribution: %s", dist)

    runtime_in_s = self._get_start_end_time(dist)
    runtime_in_s = float(runtime_in_s)
    return [{'label': RUNTIME_LABEL, 'value': runtime_in_s}]





  def _get_start_end_time(self, distribution):
    start = distribution.committed.min
    end = distribution.committed.max
    return end - start


class MeasureTime(beam.DoFn):
  def __init__(self, namespace):
    self.namespace = namespace
    self.runtime = Metrics.distribution(self.namespace, RUNTIME_LABEL)

  def start_bundle(self):
    self.runtime.update(time.time())

  def finish_bundle(self):
    self.runtime.update(time.time())

  def process(self, element):
    yield element


# Decorator to add counter metrics which counts elements lenght to method
class _CountMetrics(object):
  def __init__(self, namespace, counter_name):
    self.counter = Metrics.counter(namespace, counter_name)

  def __call__(self, fn):
    def decorated(*args):
      element = args[1]
      _, value = element
      for i in range(len(value)):
        self.counter.inc(i)
      return fn(*args)

    return decorated

count_metrics = _CountMetrics
