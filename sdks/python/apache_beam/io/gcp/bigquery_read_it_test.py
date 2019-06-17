#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
import time
import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
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
        {'number': 2, 'str': 'def'},
        {'number': 3, 'str': '你好'},
        {'number': 4, 'str': 'привет'}
    ]
    self.bigquery_client.insert_rows(
        self.project, self.dataset_id, tablename, table_data)

  def create_table_new_types(self, table_name):
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
            tableId=table_name),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)
    table_data = [
        {'bytes': b'xyw', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd', 'date': '3000-12-31',
         'time': '23:59:59'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]
    # bigquery client expects base64 encoded bytes
    for row in table_data:
      row['bytes'] = base64.b64encode(row['bytes']).decode('utf-8')
    self.bigquery_client.insert_rows(
        self.project, self.dataset_id, table_name, table_data)

  @attr('IT')
  def test_big_query_read(self):
    table_name = 'python_write_table'
    self.create_table(table_name)
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      result = (p | 'read' >> beam.io.Read(beam.io.BigQuerySource(
          query='SELECT number, str FROM `%s`' % table_id,
          use_standard_sql=True)))
      assert_that(result, equal_to([{'number': 1, 'str': 'abc'},
                                    {'number': 2, 'str': 'def'},
                                    {'number': 3, 'str': '你好'},
                                    {'number': 4, 'str': 'привет'}]))

  @attr('IT')
  def test_big_query_read_new_types(self):
    table_name = 'python_new_types_table'
    self.create_table_new_types(table_name)
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    args = self.test_pipeline.get_full_options_as_args()

    expected_data = [
        {'bytes': b'xyw', 'date': '2011-01-01', 'time': '23:59:59.999999'},
        {'bytes': b'abc', 'date': '2000-01-01', 'time': '00:00:00'},
        {'bytes': b'\xe4\xbd\xa0\xe5\xa5\xbd', 'date': '3000-12-31',
         'time': '23:59:59'},
        {'bytes': b'\xab\xac\xad', 'date': '2000-01-01', 'time': '00:00:00'}
    ]
    # bigquery io returns bytes as base64 encoded values
    for row in expected_data:
      row['bytes'] = base64.b64encode(row['bytes'])

    with beam.Pipeline(argv=args) as p:
      result = (p | 'read' >> beam.io.Read(beam.io.BigQuerySource(
          query='SELECT bytes, date, time FROM `%s`' % table_id,
          use_standard_sql=True)))
      assert_that(result, equal_to(expected_data))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
