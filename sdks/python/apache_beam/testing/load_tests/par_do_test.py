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
To run test on DirectRunner

python setup.py nosetests \
    --test-pipeline-options="
    --iterations=1000
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.par_do_test

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --output=gc
        --iterations=1000
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.par_do_test

"""
import unittest
import json
import logging

import time

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.test_pipeline import TestPipeline


class ParDoTest(unittest.TestCase):
  def parseTestPipelineOptions(self):
    return {'numRecords': self.inputOptions.get('num_records'),
            'keySizeBytes': self.inputOptions.get('key_size'),
            'valueSizeBytes': self.inputOptions.get('value_size'),
            'bundleSizeDistribution': {
                'type': self.inputOptions.get(
                    'bundle_size_distribution_type', 'const'
                ),
                'param': self.inputOptions.get(
                    'bundle_size_distribution_param', 0
                )
            },
            'forceNumInitialBundles': self.inputOptions.get(
                'force_initial_num_bundles', 0
            )
           }

  def setUp(self):
    self.pipeline = TestPipeline(is_integration_test=True)
    self.output = self.pipeline.get_option('output')
    self.iterations = self.pipeline.get_option('iterations')
    self.inputOptions = json.loads(self.pipeline.get_option('input_options'))


  class _MeasureTime(beam.DoFn):
    def __init__(self):
      self.distribution = Metrics.distribution('time_distribution', 'time')

    def process(self, element):
      self.distribution.update(time.time())
      yield element

  class _GetElement(beam.DoFn):
    def __init__(self):
      self.counter = Metrics.counter('Counter_ns', 'name')

    def process(self, element):
      _, value = element
      for i in range(len(value)):
        self.counter.inc(i)
      yield element

  def testParDo(self):
    num_runs = int(self.iterations)

    with self.pipeline as p:
      pc = (p
            | 'Read synthetic' >> beam.io.Read(
                synthetic_pipeline.SyntheticSource(
                    self.parseTestPipelineOptions()
                ))
            | 'Measure start time' >> beam.ParDo(self._MeasureTime())
           )

      for i in range(num_runs):
        label = 'Step: %d' % i
        pc = (pc
              | label >> beam.ParDo(self._GetElement()))

      if self.output is not None:
        # pylint: disable=expression-not-assigned
        (pc
         | "Write" >> beam.io.WriteToText(self.output)
        )

      result = p.run()
      result.wait_until_finish()
      metrics = result.metrics().query()
      for counter in metrics['counters']:
        print("Counter: %s", counter)

      for dist in metrics['distributions']:
        print("Distribution: %s", dist)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
