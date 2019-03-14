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


"""IT tests that test writing data to BQ
"""

from __future__ import absolute_import

import datetime
import json
import logging
import random
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.io.gcp import bigquery_io_write_pipeline
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import compute_hash

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  pass

BIG_QUERY_DATASET_ID = 'python_write_to_table_'
OUTPUT_TABLE = 'python_write_table'
OUTPUT_SCHEMA = (
    '{"fields": [{"name": "number","type": "INTEGER"},'
    '{"name": "str","type": "STRING"}]}')
OUTPUT_VERIFY_QUERY = ('SELECT number, str FROM `%s`;')
OUTPUT_EXPECTED = [
    (1, u'abc',),
    (2, u'def',)]
NEW_TYPES_OUTPUT_TABLE = 'python_new_types_table'
NEW_TYPES_OUTPUT_SCHEMA = (
    '{"fields": [{"name": "bytes","type": "BYTES"},'
    '{"name": "date","type": "DATE"},{"name": "time","type": "TIME"}]}')
NEW_TYPES_OUTPUT_VERIFY_QUERY = ('SELECT bytes, date, time FROM `%s`;')
NEW_TYPES_OUTPUT_EXPECTED = [
    (b'xyw=', datetime.date(2011, 1, 1), datetime.time(23, 59, 59, 999999),),
    (b'abc=', datetime.date(2000, 1, 1), datetime.time(0, 0),),
    (b'dec=', datetime.date(3000, 12, 31),
     datetime.time(23, 59, 59, 999999),),
    (b'\xab\xac\xad', datetime.date(2000, 1, 1), datetime.time(0, 0),)]


class BigqueryIOWriteIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%s%d' % (BIG_QUERY_DATASET_ID, str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.{}" % (self.dataset_id)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s' % self.dataset_id)

  @attr('IT')
  def test_big_query_write(self):
      expected_checksum = compute_hash(OUTPUT_EXPECTED)
      output_table = self.output_table.format(OUTPUT_TABLE)
      verify_query = OUTPUT_VERIFY_QUERY % output_table
      pipeline_verifiers = [PipelineStateMatcher(), BigqueryMatcher(
          project=self.project,
          query=verify_query,
          checksum=expected_checksum)]
      input_data = [
          {'number':1, 'str':'abc'},
          {'number':2, 'str':'def'},
      ]
      extra_opts = {
          'input_data': json.dumps(input_data),
          'output': output_table,
          'output_schema': OUTPUT_SCHEMA,
          'on_success_matcher': all_of(*pipeline_verifiers)}
      options = self.test_pipeline.get_full_options_as_args(**extra_opts)
      bigquery_io_write_pipeline.run(options)

  @attr('IT')
  def test_big_query_write_new_types(self):
      expected_checksum = compute_hash(NEW_TYPES_OUTPUT_EXPECTED)
      output_table = self.output_table.format(OUTPUT_TABLE)
      verify_query = NEW_TYPES_OUTPUT_VERIFY_QUERY % output_table
      pipeline_verifiers = [PipelineStateMatcher(), BigqueryMatcher(
          project=self.project,
          query=verify_query,
          checksum=expected_checksum)]
      input_data = [
          {'bytes':b'xyw=', 'date':'2011-01-01', 'time':'23:59:59.999999'},
          {'bytes':b'abc=', 'date':'2000-01-01', 'time':'00:00:00'},
          {'bytes':b'dec=', 'date':'3000-12-31', 'time':'23:59:59.990000'},
          {'bytes':b'\xab\xac\xad', 'date':'2000-01-01', 'time':'00:00:00'}
      ]
      extra_opts = {
          'input_data': json.dumps(input_data),
          'output': output_table,
          'output_schema': NEW_TYPES_OUTPUT_SCHEMA,
          'on_success_matcher': all_of(*pipeline_verifiers)}
      options = self.test_pipeline.get_full_options_as_args(**extra_opts)
      bigquery_io_write_pipeline.run(options)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
