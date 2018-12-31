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

"""Unittest for GCP Bigtable testing."""
from __future__ import absolute_import

import logging

import unittest

import datetime

import uuid

import random

import string

from google.cloud import bigtable
from google.cloud.bigtable import row, column_family

import apache_beam as beam
from apache_beam.runners.runner import PipelineState
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtable_io_write import BigtableWriteConfiguration
from apache_beam.io.gcp.bigtable_io_write import WriteToBigtable


def _generate_mutation_data(row_index):
  """ Generate the row data to insert in the table.
  """
  row_contents = []
  rand = random.choice(string.ascii_letters + string.digits)
  value = ''.join(rand for i in range(100))
  column_family_id = 'cf1'
  start_index = 0
  end_index = row_index
  while start_index < end_index:
    row_values = {}
    start_index += 1
    key = "beam_key%s" % ('{0:07}'.format(start_index))
    row_values["row_key"] = key
    row_values["row_content"] = []
    for column_id in range(10):
      row_content = {"column_family_id": column_family_id,
                     "column_id": ('field%s' % column_id).encode('utf-8'),
                     "value": value}
      row_values["row_content"].append(row_content)
  row_contents.append(row_values)

  return row_contents

class GenerateDirectRows(beam.DoFn):
  """ Generates an iterator of DirectRow object to process on beam pipeline.
  """

  def process(self, row_values):
    """ Process beam pipeline using an element.


    :type row_value: dict
    :param row_value: dict: dict values with row_key and row_content having
    family, column_id and value of row.

    """
    direct_row = row.DirectRow(row_key=row_values["row_key"])

    for row_value in row_values["row_content"]:
      direct_row.set_cell(row_value["column_family_id"],
                          row_value["column_id"],
                          row_value["value"],
                          datetime.datetime.now())


class BigtableIOWriteIT(unittest.TestCase):
  """ Bigtable Write Connector Test
  """
  DEFAULT_TABLE_PREFIX = "python-test"
  PROJECT_NAME = ""
  INSTANCE_NAME = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  TABLE_NAME = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  number = 500
  LOCATION_ID = "us-east1-b"

  def setUp(self):
    argv = ['--test-pipeline-options="--runner=DirectRunner"']
    self.test_pipeline = TestPipeline(is_integration_test=True, argv=argv)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')
    self.INSTANCE_NAME = self.test_pipeline.get_option('instance')
    self.PROJECT_NAME = self.project

    self.client = bigtable.Client(project=self.project, admin=True)

    self._create_instance()
    self._create_table()

  def tearDown(self):
    if self.table.exists():
      self.table.delete()

  def test_bigtable_write_python(self):
    """ Test Bigtable Connector Write
    """
    number = self.number
    config = BigtableWriteConfiguration(self.project, self.INSTANCE_NAME,
                                        self.TABLE_NAME)
    pipeline_args = self.test_pipeline.options_list
    pipeline_options = PipelineOptions(pipeline_args)

    row_values = _generate_mutation_data(number)

    with beam.Pipeline(options=pipeline_options) as pipeline:
      _ = (
          pipeline
          | 'Generate Row Values' >> beam.Create(row_values)
          | 'Generate Direct Rows' >> beam.ParDo(GenerateDirectRows())
          | 'Write to BT' >> beam.ParDo(WriteToBigtable(config)))

      result = pipeline.run()
      result.wait_until_finish()

      assert result.state == PipelineState.DONE

      if not hasattr(result, 'has_job') or result.has_job:
        read_filter = MetricsFilter().with_name('Written Row')
        query_result = result.metrics().query(read_filter)
        if query_result['counters']:
          read_counter = query_result['counters'][0]

          logging.info('Number of Rows: %d', read_counter.committed)
          assert read_counter.committed == number
  def _create_instance(self):
    """ Create the Instances Test in Bigtable
    """
    instance = self.client.instance(self.INSTANCE_NAME)
    serve_nodes = 3
    self.instance = instance.create(location_id=self.LOCATION_ID,
                                    serve_nodes=serve_nodes)

  def _create_table(self):
    """ Create the Table Test in Bigtable
    """
    table = self.instance.table(self.TABLE_NAME)

    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'cf1'
    column_families = {column_family_id: max_versions_rule}

    if not table.exists():
      self.table.create(column_families=column_families)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
