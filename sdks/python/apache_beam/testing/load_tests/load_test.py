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

from __future__ import absolute_import

import json
import logging
import os
import unittest

from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
from apache_beam.testing.test_pipeline import TestPipeline

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class LoadTest(unittest.TestCase):
  def parseTestPipelineOptions(self, options=None):
    if not options:
      options = self.input_options

    return {
        'numRecords': options.get('num_records'),
        'keySizeBytes': options.get('key_size'),
        'valueSizeBytes': options.get('value_size'),
        'bundleSizeDistribution': {
            'type': options.get(
                'bundle_size_distribution_type', 'const'
            ),
            'param': options.get('bundle_size_distribution_param', 0)
        },
        'forceNumInitialBundles': options.get(
            'force_initial_num_bundles', 0
        )
    }

  @classmethod
  def setUpClass(cls):
    cls.pipeline = TestPipeline()
    cls.input_options = json.loads(cls.pipeline.get_option('input_options'))

    cls.publish_to_big_query = cls.pipeline.get_option('publish_to_big_query')
    cls.metrics_namespace = cls.pipeline.get_option('metrics_table')

    if not cls.publish_to_big_query or cls.publish_to_big_query != 'true':
      logging.info('Metrics will not be collected')
      cls.metrics_monitor = None
    else:
      cls.metrics_monitor = MetricsReader(
          project_name=cls.pipeline.get_option('project'),
          bq_table=cls.metrics_namespace,
          bq_dataset=cls.pipeline.get_option('metrics_dataset'),
      )

  @classmethod
  def tearDownClass(cls):
    result = cls.pipeline.run()
    result.wait_until_finish()

    if cls.metrics_monitor:
      cls.metrics_monitor.publish_metrics(result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
