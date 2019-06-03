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
A performance test for reading data from a BigQuery table.
Besides of the standard options, there are options with special meaning:
* input_dataset - BQ dataset id.
* input_table - BQ table id.
The table will be created and populated with data from Synthetic Source if it
does not exist.
* input_options - options for Synthetic Source:
num_records - number of rows to be inserted,
value_size - the length of a single row,
key_size - required option, but its value has no meaning,
* num_slow - an integer in range [0,100] used to customize slow reading
simulation.

Example test run on DataflowRunner:

python setup.py nosetests \
    --test-pipeline-options="
    --runner=TestDataflowRunner
    --project=...
    --staging_location=gs://...
    --temp_location=gs://...
    --sdk_location=.../dist/apache-beam-x.x.x.dev0.tar.gz
    --input_dataset=...
    --input_table=...
    --input_options='{
    \"num_records\": 1024,
    \"key_size\": 1,
    \"value_size\": 1024,
    }'" \
    --tests apache_beam.io.gcp.bigquery_read_perf_test
"""

from __future__ import absolute_import

import base64
import logging
import os
import unittest

from apache_beam import Map
from apache_beam.io import BigQueryDisposition
from apache_beam.io import Read
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery_io_read_pipeline import run_pipeline
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.test_pipeline import TestPipeline

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class BigQueryIOReadTest(LoadTest):
  def setUp(self):
    super(BigQueryIOReadTest, self).setUp()
    self.num_slow = self.pipeline.get_option('num_slow') or 0
    self.input_dataset = self.pipeline.get_option('input_dataset')
    self.input_table = self.pipeline.get_option('input_table')
    self._check_for_input_data()

  def _check_for_input_data(self):
    """Checks if a BQ table with input data exists and creates it if not."""
    wrapper = BigQueryWrapper()
    try:
      wrapper.get_table(self.project_id, self.input_dataset, self.input_table)
    except Exception:
      self._create_input_data()

  def _create_input_data(self):
    """
    Runs an additional pipeline which creates test data and waits for its
    completion.
    """
    SCHEMA = parse_table_schema_from_json(
        '{"fields": [{"name": "data", "type": "BYTES"}]}')

    def format_record(record):
      # Since Syntentic Source returns data as a dictionary. we should skip
      # one of the part
      return {'data': base64.b64encode(record[1])}

    p = TestPipeline()
    # pylint: disable=expression-not-assigned
    (p
     | 'Produce rows' >> Read(SyntheticSource(self.parseTestPipelineOptions()))
     | 'Format' >> Map(format_record)
     | 'Write to BigQuery' >> WriteToBigQuery(
         self.input_dataset + '.' + self.input_table,
         schema=SCHEMA,
         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=BigQueryDisposition.WRITE_EMPTY))
    p.run().wait_until_finish()

  def test(self):
    run_pipeline(self.pipeline, self.input_dataset, self.input_table,
                 self.num_slow, self.input_options['num_records'])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
