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
A pipeline that writes data from a SyntheticSource to a BigQuery table.

Pipeline options are described in the ParDoTest file.
"""

from __future__ import absolute_import

import logging
import unittest
import base64

from apache_beam.io import Read, WriteToBigQuery, BigQueryDisposition
from apache_beam import Map
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json, \
  BigQueryWrapper, parse_table_reference
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.testing.load_tests.load_test import LoadTest


class BigQueryIOWriteTest(LoadTest):
  def tearDown(self):
    LoadTest.tearDown(self)
    self._cleanup_data()

  def _cleanup_data(self):
    """Removes an output BQ table."""
    wrapper = BigQueryWrapper()
    project_id = self.pipeline.get_option('project')
    table_ref = parse_table_reference(self.pipeline.get_option('output'),
                                      project=project_id)
    wrapper._delete_table(project_id, table_ref.datasetId, table_ref.tableId)

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
         self.pipeline.get_option('output'),
         schema=SCHEMA,
         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=BigQueryDisposition.WRITE_EMPTY))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
