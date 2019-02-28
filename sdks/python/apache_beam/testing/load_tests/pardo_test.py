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
This is ParDo load test with Synthetic Source. Besides of the standard
input options there are additional options:
* number_of_counter_operations - number of pardo operations
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_namespace (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* output (optional) - destination to save output, in case of no option
output won't be written,
* input_options - options for Synthetic Sources.

Example test run on DirectRunner:

python setup.py nosetests \
    --test-pipeline-options="
    --number_of_counter_operations=1000
    --output=gs://...
    --project=big-query-project
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=pardo
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.pardo_test

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --output=gs://...
        --number_of_counter_operations=1000
        --publish_to_big_query=true
        --metrics_dataset=python_load_tests
        --metrics_table=pardo
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.pardo_test

"""

from __future__ import absolute_import

import json
import logging
import os
import unittest

import apache_beam as beam
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
from apache_beam.testing.test_pipeline import TestPipeline

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class ParDoTest(unittest.TestCase):
  def parseTestPipelineOptions(self):
    return {'numRecords': self.input_options.get('num_records'),
            'keySizeBytes': self.input_options.get('key_size'),
            'valueSizeBytes': self.input_options.get('value_size'),
            'bundleSizeDistribution': {
                'type': self.input_options.get(
                    'bundle_size_distribution_type', 'const'
                ),
                'param': self.input_options.get(
                    'bundle_size_distribution_param', 0
                )
            },
            'forceNumInitialBundles': self.input_options.get(
                'force_initial_num_bundles', 0
            )
           }

  def setUp(self):
    self.pipeline = TestPipeline()

    self.output = self.pipeline.get_option('output')
    self.iterations = self.pipeline.get_option('number_of_counter_operations')
    self.input_options = json.loads(self.pipeline.get_option('input_options'))

    self.metrics_monitor = self.pipeline.get_option('publish_to_big_query')
    self.metrics_namespace = self.pipeline.get_option('metrics_table')

    if not self.metrics_monitor or str(self.metrics_monitor) != 'true':
      logging.info('Metrics will not be collected')
    else:
      self.metrics_monitor = MetricsReader(
          project_name=self.pipeline.get_option('project'),
          bq_table=self.metrics_namespace,
          bq_dataset=self.pipeline.get_option('metrics_dataset'),
      )

  def testParDo(self):

    class _GetElement(beam.DoFn):
      from apache_beam.testing.load_tests.load_test_metrics_utils import count_bytes

      @count_bytes
      def process(self, element, namespace, is_returning):
        if is_returning:
          yield element

    if self.iterations is None:
      num_runs = 1
    else:
      num_runs = int(self.iterations)

    with self.pipeline as p:
      pc = (p
            | 'Read synthetic' >> beam.io.Read(
                synthetic_pipeline.SyntheticSource(
                    self.parseTestPipelineOptions()
                ))
            | 'Measure time: Start' >> beam.ParDo(
                MeasureTime(self.metrics_namespace))
           )

      for i in range(num_runs):
        is_returning = (i == (num_runs-1))
        pc = (pc
              | 'Step: %d' % i >> beam.ParDo(
                  _GetElement(), self.metrics_namespace, is_returning)
             )

      if self.output is not None:
        pc = (pc
              | "Write" >> beam.io.WriteToText(self.output)
             )

      # pylint: disable=expression-not-assigned
      (pc
       | 'Measure time: End' >> beam.ParDo(MeasureTime(self.metrics_namespace))
      )

      result = p.run()
      result.wait_until_finish()

      if self.metrics_monitor is not None:
        self.metrics_monitor.publish_metrics(result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
