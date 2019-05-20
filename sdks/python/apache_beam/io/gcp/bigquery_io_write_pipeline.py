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
A pipeline that writes data from Synthetic Source to a BigQuery table.

Besides of the standard options, there are options with special meaning:
* output - BQ destination in the following format: 'dataset_id.table_id'.
The table will be removed after test completion,
* input_options - options for Synthetic Source:
  * num_records - number of rows to be inserted,
  * value_size - the length of a single row,
  * key_size - required option, but its value has no meaning.

Example test run on DataflowRunner:

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=.../dist/apache-beam-x.x.x.dev0.tar.gz
        --output=...
        --input_options='{
        \"num_records\": 1024,
        \"key_size\": 1,
        \"value_size\": 1024,
        }'" \
    --tests apache_beam.io.gcp.tests.bigquery_io_write_pipeline

This setup will result in a table of 1MB size.
"""

from __future__ import absolute_import

import base64
import logging
import unittest

from apache_beam import Map
from apache_beam.io import BigQueryDisposition
from apache_beam.io import Read
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_reference
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.tests import utils
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.synthetic_pipeline import SyntheticSource


class BigQueryIOWriteTest(LoadTest):
  def setUp(self):
    super(BigQueryIOWriteTest, self).setUp()
    self.output = self.pipeline.get_option('output')

  def tearDown(self):
    super(BigQueryIOWriteTest, self).tearDown()
    self._cleanup_data()

  def _cleanup_data(self):
    """Removes an output BQ table."""
    table_ref = parse_table_reference(self.output, project=self.project_id)
    utils.delete_bq_table(self.project_id, table_ref.datasetId,
                          table_ref.tableId)

  def test(self):
    SCHEMA = parse_table_schema_from_json(
        '{"fields": [{"name": "data", "type": "BYTES"}]}')

    def format_record(record):
      return {'data': base64.b64encode(record[1])}

    # pylint: disable=expression-not-assigned
    (self.pipeline
     | 'ProduceRows' >> Read(SyntheticSource(self.parseTestPipelineOptions()))
     | 'Format' >> Map(format_record)
     | 'WriteToBigQuery' >> WriteToBigQuery(
         self.output,
         schema=SCHEMA,
         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=BigQueryDisposition.WRITE_EMPTY))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
