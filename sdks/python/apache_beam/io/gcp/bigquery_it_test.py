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

"""Unit tests for BigQuery sources and sinks."""
from __future__ import absolute_import

import base64
import logging
import random
import sys
import time
import unittest

import hamcrest as hc
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.bigquery_file_loads_test import _ELEMENTS
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.options import value_provider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None
# pylint: enable=wrong-import-order, wrong-import-position


class BigQueryStreamingInsertTransformIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_bq_streaming_inserts_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.dataset_id = '%s%s%d' % (self.BIG_QUERY_DATASET_ID,
                                  str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client = BigQueryWrapper()
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)
    logging.info("Created dataset %s in project %s",
                 self.dataset_id, self.project)

  @attr('IT')
  def test_value_provider_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)
    schema = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'}]}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_1,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_2,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
        experiments='use_beam_bq_sink')

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create([row for row in _ELEMENTS if 'language' in row])

      _ = (input
           | "WriteWithMultipleDests" >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=value_provider.StaticValueProvider(
                   str, '%s:%s' % (self.project, output_table_1)),
               schema=value_provider.StaticValueProvider(dict, schema),
               method='STREAMING_INSERTS'))
      _ = (input
           | "WriteWithMultipleDests2" >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=value_provider.StaticValueProvider(
                   str, '%s:%s' % (self.project, output_table_2)),
               method='FILE_LOADS'))

  @attr('IT')
  def test_multiple_destinations_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)

    full_output_table_1 = '%s:%s' % (self.project, output_table_1)
    full_output_table_2 = '%s:%s' % (self.project, output_table_2)

    schema1 = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'}]}
    schema2 = {'fields': [
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'foundation', 'type': 'STRING', 'mode': 'NULLABLE'}]}

    bad_record = {'language': 1, 'manguage': 2}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_1,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_2,
            data=[(d['name'], d['foundation'])
                  for d in _ELEMENTS
                  if 'foundation' in d])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
        experiments='use_beam_bq_sink')

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create(_ELEMENTS)

      input2 = p | "Broken record" >> beam.Create([bad_record])

      input = (input, input2) | beam.Flatten()

      r = (input
           | "WriteWithMultipleDests" >> beam.io.gcp.bigquery.WriteToBigQuery(
               table=lambda x: (full_output_table_1
                                if 'language' in x
                                else full_output_table_2),
               schema=lambda dest: (schema1
                                    if dest == full_output_table_1
                                    else schema2),
               method='STREAMING_INSERTS'))

      assert_that(r[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS],
                  equal_to([(full_output_table_1, bad_record)]))

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      logging.info("Deleting dataset %s in project %s",
                   self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s in project %s',
                    self.dataset_id, self.project)


class BigQueryWriteIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_write_to_table_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%d' % (self.BIG_QUERY_DATASET_ID,
                                  str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.{}" % (self.dataset_id)
    logging.info("Created dataset %s in project %s",
                 self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      logging.info("Deleting dataset %s in project %s",
                   self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s in project %s',
                    self.dataset_id, self.project)

  def create_table(self, tablename):
    table_schema = bigquery.TableSchema()
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'bytes'
    table_field.type = 'BYTES'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'date'
    table_field.type = 'DATE'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'time'
    table_field.type = 'TIME'
    table_schema.fields.append(table_field)
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=self.project,
            datasetId=self.dataset_id,
            tableId=tablename),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)

  @attr('IT')
  def test_big_query_write(self):
    output_table = 'python_write_table'
    output_table = self.output_table.format(output_table)

    input_data = [
        {'number': 1, 'str': 'abc'},
        {'number': 2, 'str': 'def'},
    ]
    table_schema = {"fields": [
        {"name": "number", "type": "INTEGER"},
        {"name": "str", "type": "STRING"}]}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT number, str FROM %s" % output_table,
            data=[(1, 'abc',), (2, 'def',)])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           schema=table_schema,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  def test_big_query_write_new_types(self):
    output_table = 'python_new_types_table'
    output_table = self.output_table.format(output_table)

    input_data = [
        {'bytes': b'xyw=', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc=', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'dec=', 'date': '3000-12-31', 'time': '23:59:59.990000'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]
    table_schema = {"fields": [
        {"name": "bytes", "type": "BYTES"},
        {"name": "date", "type": "DATE"},
        {"name": "time", "type": "TIME"}]}

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT bytes, date, time FROM %s" % output_table,
            data=[(b'xyw=', '2011-01-01', '23:59:59.999999', ),
                  (b'abc=', '2000-01-01', '00:00:00', ),
                  (b'dec=', '3000-12-31', '23:59:59.990000',),
                  (b'\xab\xac\xad', '2000-01-01', '00:00:00',)])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           schema=table_schema,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

  @attr('IT')
  @unittest.skipIf(sys.version_info[0] == 2,
                   'Writing bytes without schema is currently not supported'
                   'TODO: BEAM-6769')
  def test_big_query_write_without_schema(self):
    output_table = 'python_no_schema_table'
    self.create_table(output_table)
    output_table = self.output_table.format(output_table)

    input_data = [
        {'bytes': b'xyw=', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc=', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'dec=', 'date': '3000-12-31', 'time': '23:59:59.990000'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT bytes, date, time FROM %s" % output_table,
            data=[(b'xyw=', '2011-01-01', '23:59:59.999999',),
                  (b'abc=', '2000-01-01', '00:00:00',),
                  (b'dec=', '3000-12-31', '23:59:59.990000',),
                  (b'\xab\xac\xad', '2000-01-01', '00:00:00',)])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      (p | 'create' >> beam.Create(input_data)
       | 'write' >> beam.io.WriteToBigQuery(
           output_table,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


class BigQueryReadIntegrationTests(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_read_table_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%d' % (self.BIG_QUERY_DATASET_ID,
                                  str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.{}" % (self.dataset_id)
    logging.info("Created dataset %s in project %s",
                 self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      logging.info("Deleting dataset %s in project %s",
                   self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s in project %s',
                    self.dataset_id, self.project)

  def create_table(self, tablename):
    table_schema = bigquery.TableSchema()
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'number'
    table_field.type = 'INTEGER'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'str'
    table_field.type = 'STRING'
    table_schema.fields.append(table_field)
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=self.project,
            datasetId=self.dataset_id,
            tableId=tablename),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)
    table_data = [
        {'number': 1, 'str': 'abc'},
        {'number': 2, 'str': 'def'}
    ]
    self.bigquery_client.insert_rows(
        self.project, self.dataset_id, tablename, table_data)

  def create_table_new_types(self, tablename):
    table_schema = bigquery.TableSchema()
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'bytes'
    table_field.type = 'BYTES'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'date'
    table_field.type = 'DATE'
    table_schema.fields.append(table_field)
    table_field = bigquery.TableFieldSchema()
    table_field.name = 'time'
    table_field.type = 'TIME'
    table_schema.fields.append(table_field)
    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=self.project,
            datasetId=self.dataset_id,
            tableId=tablename),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)
    table_data = [
        {'bytes': b'xyw=', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc=', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'dec=', 'date': '3000-12-31', 'time': '23:59:59.990000'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]
    for row in table_data:
      row['bytes'] = base64.b64encode(row['bytes']).decode('utf-8')
    self.bigquery_client.insert_rows(
        self.project, self.dataset_id, tablename, table_data)


  @attr('IT')
  def test_big_query_read(self):
    output_table = 'python_write_table'
    self.create_table(output_table)
    output_table = self.output_table.format(output_table)

    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      # pylint: disable=expression-not-assigned
      result = (p | 'read' >> beam.io.Read(beam.io.BigQuerySource(
          query='SELECT number, str FROM `%s`' % output_table,
          use_standard_sql=True)))
      assert_that(result, equal_to([{'number': 1, 'str': 'abc'},
                                    {'number': 2, 'str': 'def'}]))

  @attr('IT')
  def test_big_query_read_new_types(self):
    output_table = 'python_new_types_table'
    self.create_table_new_types(output_table)
    output_table = self.output_table.format(output_table)

    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      result = (p | 'read' >> beam.io.Read(beam.io.BigQuerySource(
          query='SELECT bytes, date, time FROM `%s`' % output_table,
          use_standard_sql=True)))
      assert_that(result, equal_to([
          {'bytes': b'xyw=', 'date': '2011-01-01', 'time': '23:59:59.999999'},
          {'bytes': b'abc=', 'date': '2000-01-01', 'time': '00:00:00'},
          {'bytes': b'dec=', 'date': '3000-12-31', 'time': '23:59:59.990000'},
          {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
      ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
