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
A Dataflow job that counts the number of rows in a BQ table.

Pipeline options are described in the ParDoTest file. A job can be configured to
simulate slow reading for a given number of rows by providing additional
'num_slow' option.
"""

from __future__ import absolute_import

import logging
import unittest
import base64
import random
import time


from apache_beam import Map, ParDo, DoFn
from apache_beam.io import Read, WriteToBigQuery, BigQueryDisposition, \
  BigQuerySource
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json, \
  BigQueryWrapper, parse_table_reference, HttpError
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.combiners import Count


class BigQueryIOReadTest(LoadTest):
  def setUp(self):
    LoadTest.setUp(self)
    self.num_slow = self.pipeline.get_option('num_slow') or 0
    self._check_for_input_data()

  def _check_for_input_data(self):
    """Checks if a BQ table with input data exists and creates it if not."""
    wrapper = BigQueryWrapper()
    project_id = self.pipeline.get_option('project')
    table_ref = parse_table_reference(self.pipeline.get_option('output'),
                                      project=project_id)
    try:
      wrapper.get_table(project_id, table_ref.datasetId, table_ref.tableId)
    except HttpError:
      self._create_input_data()

  def _create_input_data(self):
    """
    Runs an additional pipeline which creates test data and waits for its
    completion.
    """
    SCHEMA = parse_table_schema_from_json(
        '{"fields": [{"name": "data", "type": "BYTES"}]}')

    def format_record(record):
      return {'data': base64.b64encode(record[1])}

    setup_pipeline = TestPipeline()
    # pylint: disable=expression-not-assigned
    (setup_pipeline
     | 'ProduceRows' >> Read(SyntheticSource(self.parseTestPipelineOptions()))
     | 'Format' >> Map(format_record)
     | 'WriteToBigQuery' >> WriteToBigQuery(
         self.pipeline.get_option('output'),
         schema=SCHEMA,
         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=BigQueryDisposition.WRITE_EMPTY))
    setup_pipeline.run().wait_until_finish()

  class RowToStringWithSlowDown(DoFn):
    def process(self, element, num_slow=0, *args, **kwargs):
      if num_slow == 0:
        yield ['row']
      else:
        rand = random.random() * 100
        if rand < num_slow:
          time.sleep(0.01)
          yield ['slow_row']
        else:
          yield ['row']

  def test(self):
    pc = (self.pipeline
          | 'ReadFromBigQuery' >> Read(BigQuerySource(
              self.pipeline.get_option('output')))
          | 'RowToString' >> ParDo(self.RowToStringWithSlowDown(),
                                   num_slow=self.num_slow)
          | 'Count' >> Count.Globally())

    assert_that(pc, equal_to([self.input_options['num_records']]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
