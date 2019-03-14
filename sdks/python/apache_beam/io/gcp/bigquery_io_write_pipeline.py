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

"""A Dataflow job that writes data in a BQ table.
"""

from __future__ import absolute_import

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input_data', dest='input_data', required=True,
                      help='Data to write to BQ')
  parser.add_argument('--output', required=True,
                      help='Output BQ table to write results to.')
  parser.add_argument('--output_schema', dest='output_schema', required=True,
                      help='Schema for output BQ table.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  table_schema = parse_table_schema_from_json(known_args.output_schema)

  p = TestPipeline(options=PipelineOptions(pipeline_args))

  # pylint: disable=expression-not-assigned
  (p | 'create' >> beam.Create(json.loads(known_args.input_data))
     | 'write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
