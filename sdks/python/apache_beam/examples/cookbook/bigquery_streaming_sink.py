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

"""A streaming word-counting workflow.

Important: streaming pipeline support in Python Dataflow is in development
and is not yet available for use.
"""

from __future__ import absolute_import

import argparse
import logging
import re


import apache_beam as beam
import apache_beam.transforms.window as window


def run(argv=None):
  """Build and run the pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic', required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
  parser.add_argument(
      '--output_table', required=True,
      help=
      ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | beam.io.ReadStringsFromPubSub(known_args.input_topic)

    # Capitalize the characters in each line.
    transformed = (lines
                   | 'Split' >> (
                       beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                       .with_output_types(unicode))
                   | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
                   | beam.WindowInto(window.FixedWindows(15, 0))
                   | 'Group' >> beam.GroupByKey()
                   | 'Count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
                   | 'Format' >> beam.Map(
                       lambda (k, v): {'word': k, 'count': v}))

    # Write to BigQuery.
    # pylint: disable=expression-not-assigned
    transformed | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output_table,
        schema='word:STRING, count:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
