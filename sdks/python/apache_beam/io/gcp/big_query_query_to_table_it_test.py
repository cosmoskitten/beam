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
Integration test for Google Cloud BigQuery.
"""
from __future__ import absolute_import

import hamcrest as hc
import logging
import unittest
import time

from apache_beam.io.gcp import big_query_query_to_table_pipeline
from apache_beam.testing.test_pipeline import TestPipeline
from google.cloud import bigquery
from nose.plugins.attrib import attr

BIG_QUERY_DATASET_ID = 'python_query_to_table_'
NEW_TYPES_INPUT_TABLE = 'python_new_types_table'
NEW_TYPES_OUTPUT_SCHEMA = (
  '{"fields": [{"name": "bytes","type": "BYTES"},'
  '{"name": "date","type": "DATE"},{"name": "time","type": "TIME"}]}')
NEW_TYPES_OUTPUT_VERIFY_QUERY = ('SELECT bytes, date FROM `%s`;')
NEW_TYPES_OUTPUT_EXPECTED = [
  'xyw=', '2011-01-01',
  'abc=', '2000-01-01',
  'dec=', '3000-12-31']
LEGACY_QUERY = (
  'SELECT * FROM (SELECT "apple" as fruit), (SELECT "orange" as fruit),')
STANDARD_QUERY = (
  'SELECT * FROM (SELECT "apple" as fruit) '
  'UNION ALL (SELECT "orange" as fruit)')
NEW_TYPES_QUERY = (
  'SELECT bytes, date, time FROM [%s.%s]')
DIALECT_OUTPUT_SCHEMA = ('{"fields": [{"name": "fruit","type": "STRING"}]}')
DIALECT_OUTPUT_VERIFY_QUERY = ('SELECT fruit from `%s`;')
DIALECT_OUTPUT_EXPECTED = ['apple', 'orange']

class BigQueryQueryToTableIT(unittest.TestCase):
  # Enable nose tests running in parallel
  _multiprocess_can_split_ = True

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = bigquery.Client(project=self.project)
    self.dataset_id = BIG_QUERY_DATASET_ID + str(int(time.time()))
    self.dataset_ref = self.bigquery_client.dataset(self.dataset_id)
    self.bigquery_client.create_dataset(bigquery.Dataset(self.dataset_ref))
    self.output_table = "%s.output_table" % (self.dataset_id)
    print "output table in setup"
    print self.output_table

  def tearDown(self):
    self.bigquery_client.delete_dataset(self.dataset_ref, delete_contents=True)

  def _setup_new_types_env(self):
    schema = [bigquery.SchemaField('bytes', 'BYTES'),
              bigquery.SchemaField('date', 'DATE') ,
              bigquery.SchemaField('time', 'TIME')]
    table_ref = self.dataset_ref.table(NEW_TYPES_INPUT_TABLE)
    table = bigquery.Table(table_ref, schema=schema)
    table = self.bigquery_client.create_table(table)
    table_data = [
      (str.encode('xyw='), '2011-01-01', '23:59:59.999999'),
      (str.encode('abc='), '2000-01-01', '00:00:00'),
      (str.encode('dec='), '3000-12-31', '23:59:59.990000')
    ]
    self.bigquery_client.insert_rows(table, table_data)

  def _verify_new_types_output_table(self, query, expected_result):
    result = []
    rows = self.bigquery_client.query(query).result()
    for row in rows:
      result.append(str.decode(row.values()[0]))
      result.append(str(row.values()[1]))
    hc.assert_that(sorted(result), hc.equal_to(sorted(expected_result)))

  def _verify_legacy_output_table(self, query, expected_result):
    result = []
    rows = self.bigquery_client.query(query).result()
    for row in rows:
      result += row.values()
    hc.assert_that(sorted(result), hc.equal_to(sorted(expected_result)))

  @attr('IT')
  def test_big_query_legacy_sql(self):
    print "output table when setup the pipeline options"
    print self.output_table
    extra_opts = {'query': LEGACY_QUERY,
               'output': self.output_table,
               'output_schema': DIALECT_OUTPUT_SCHEMA,
               'use_standard_sql': False}
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)
    print "output table when trying to verify result"
    self._verify_legacy_output_table(
        DIALECT_OUTPUT_VERIFY_QUERY % self.output_table,
        DIALECT_OUTPUT_EXPECTED)

  @attr('IT')
  def test_big_query_standard_sql(self):
    extra_opts = {'query': STANDARD_QUERY,
               'output': self.output_table,
               'output_schema': DIALECT_OUTPUT_SCHEMA,
               'use_standard_sql': True}
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)
    self._verify_legacy_output_table(
        DIALECT_OUTPUT_VERIFY_QUERY % self.output_table,
        DIALECT_OUTPUT_EXPECTED)

  @attr('IT')
  def test_big_query_new_types(self):
    self._setup_new_types_env()
    extra_opts = {
      'query': NEW_TYPES_QUERY % (self.dataset_id, NEW_TYPES_INPUT_TABLE),
      'output': self.output_table,
      'output_schema': NEW_TYPES_OUTPUT_SCHEMA,
      'use_standard_sql': False}
    options = self.test_pipeline.get_full_options_as_args(**extra_opts)
    big_query_query_to_table_pipeline.run_bq_pipeline(options)
    self._verify_new_types_output_table(
        NEW_TYPES_OUTPUT_VERIFY_QUERY % self.output_table,
        NEW_TYPES_OUTPUT_EXPECTED)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
