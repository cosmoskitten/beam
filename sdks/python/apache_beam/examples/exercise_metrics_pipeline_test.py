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

"""A word-counting workflow."""

from __future__ import absolute_import
from __future__ import print_function

import logging
import time
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing import metric_result_matchers
from apache_beam.testing.metric_result_matchers import MetricResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline


from nose.plugins.attrib import attr
from hamcrest.library.number.ordering_comparison import greater_than





# TODO see if we can share code with fn_api_runner_test.py
SLEEP_TIME_MS = 10000
INPUT = [0, 0, 0, SLEEP_TIME_MS]


def common_metric_matchers():
  """MetricResult matchers common to all tests."""
  matchers = [
      # TODO(ajamato): Matcher for the 'pair_with_one' step's ElementCount.
      # TODO(ajamato): Matcher for the 'pair_with_one' step's MeanByteCount.
      # TODO(ajamato): Matcher for the start and finish exec times.
      # TODO(ajamato): Matcher for a user distribution tuple metric.
      MetricResultMatcher( # GroupByKey.
          name='ElementCount',
          labels={
              'original_name': 'pair_with_one-out0-ElementCount',
              'output_user_name': 'pair_with_one-out0'
          },
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
      # User Metrics.
      MetricResultMatcher(
          name='empty_lines',
          namespace='apache_beam.examples.wordcount.WordExtractingDoFn',
          step='split',
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
      MetricResultMatcher(
          name='word_lengths',
          namespace='apache_beam.examples.wordcount.WordExtractingDoFn',
          step='split',
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
      MetricResultMatcher(
          name='words',
          namespace='apache_beam.examples.wordcount.WordExtractingDoFn',
          step='split',
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
  ]
  return matchers


def fn_api_metric_matchers():
  """MetricResult matchers with adjusted step names for the FN API DF test."""
  matchers = common_metric_matchers()
  matchers.extend([
      # Execution Time Metric for the pair_with_one step.
      MetricResultMatcher(
          name='ExecutionTime_ProcessElement',
          labels={
              'step': 's9',
          },
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
  ])
  return matchers


def legacy_metric_matchers():
  """MetricResult matchers with adjusted step names for the legacy DF test."""
  matchers = common_metric_matchers()
  matchers.extend([
      # Execution Time Metric for the pair_with_one step.
      MetricResultMatcher(
          name='ExecutionTime_ProcessElement',
          labels={
              'step': 's2',
          },
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
  ])
  return matchers

class ExerciseMetricsPipelineTest(unittest.TestCase):

  def run_pipeline(self, **opts):
    test_pipeline = TestPipeline(is_integration_test=True)
    argv = test_pipeline.get_full_options_as_args(**opts)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    _ = (p
         | 'create' >> beam.Create(INPUT)
         | 'sleep' >> beam.Map(time.sleep)
         | 'map_to_common_key' >> beam.Map(lambda x: ('key', x))
         | 'group' >> beam.GroupByKey()
         | 'out_data' >> beam.FlatMap(lambda x: [
            1, 2, 3, 4, 5,
            beam.pvalue.TaggedOutput('once', x),
            beam.pvalue.TaggedOutput('twice', x),
            beam.pvalue.TaggedOutput('twice', x)]))
    result = p.run()
    result.wait_until_finish()
    return result

  @attr('IT')
  def test_metrics_it(self):
    self.run_pipeline()

  @attr('IT', 'ValidatesContainer')
  def test_metrics_fnapi_it(self):
    self.run_pipeline(experiment='beam_fn_api')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()


