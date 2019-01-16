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

"""BigTable connector

This module implements writing to BigTable tables.
The default mode is to set row data to write to BigTable tables.
The syntax supported is described here:
https://cloud.google.com/bigtable/docs/quickstart-cbt

BigTable connector can be used as main outputs. A main output
(common case) is expected to be massive and will be split into
manageable chunks and processed in parallel. In the example below
we created a list of rows then passed to the GeneratedDirectRows
DoFn to set the Cells and then we call the WriteToBigtable to insert
those generated rows in the table.

  main_table = (p
       | 'Generate Direct Rows' >> GenerateDirectRows(number)
       | 'Write to BT' >> beam.ParDo(WriteToBigtable(config)))
"""
from __future__ import absolute_import

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.batcher import MutationsBatcher
except ImportError:
  pass


class WriteToBigtable(beam.DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line

  :type beam_options: class:`~bigtable_configuration.BigtableConfiguration`
  :param beam_options: class `~bigtable_configuration.BigtableConfiguration`
  """

  def __init__(self, beam_options):
    super(WriteToBigtable, self).__init__(beam_options)
    self.beam_options = beam_options
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self.beam_options.project_id)
      instance = client.instance(self.beam_options.instance_id)
      self.table = instance.table(self.beam_options.table_id)
    self.batcher = MutationsBatcher(self.table)

  def process(self, row):
    self.written.inc()
    self.batcher.mutate(row)

  def finish_bundle(self):
    result = self.batcher.flush()
    self.batcher = None
    return result

  def display_data(self):
    return {'projectId': DisplayDataItem(self.beam_options.project_id,
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self.beam_options.instance_id,
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self.beam_options.table_id,
                                       label='Bigtable Table Id')
           }


class BigtableConfiguration(object):
  """ Bigtable configuration variables.

  :type project_id: :class:`str` or :func:`unicode <unicode>`
  :param project_id: (Optional) The ID of the project which owns the
    instances, tables and data. If not provided, will
    attempt to determine from the environment.

  :type instance_id: str
  :param instance_id: The ID of the instance.

  :type table_id: str
  :param table_id: The ID of the table.
  """

  def __init__(self, project_id, instance_id, table_id):
    self.project_id = project_id
    self.instance_id = instance_id
    self.table_id = table_id


class WriteBigtableOptions(PipelineOptions):
  """ Create the Pipeline Options to set WriteBigtable.
  You can create and use this class in the Template, with a certainly steps.
  """
  @classmethod
  def _add_argparse_args(cls, parser):
    PipelineOptions._add_argparse_args(parser)
    parser.add_argument('--instance')
    parser.add_argument('--table')
