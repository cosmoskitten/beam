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

"""Unit tests for the metric_result_matchers."""

from __future__ import absolute_import

import unittest

from apache_beam.testing import metric_result_matchers
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metricbase import MetricName
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from hamcrest.core.core.allof import all_of
from hamcrest.core.core.isnot import is_not
from hamcrest.library.number.ordering_comparison import greater_than
from hamcrest.library.text.isequal_ignoring_case import equal_to_ignoring_case
from hamcrest import assert_that as hc_assert_that


EVERYTHING_DISTRIBUTION = {
    'namespace': 'myNamespace',
    'name': 'myName',
    'step': 'myStep',
    'attempted': {
        'distribution': {
            'sum': 12,
            'count': 5,
            'min': 0,
            'max': 6,
        }
    },
    'committed': {
        'distribution': {
            'sum': 12,
            'count': 5,
            'min': 0,
            'max': 6,
        }
    },
    'labels' : {
        'pcollection': 'myCollection',
        'myCustomKey': 'myCustomValue'
    }
}

EVERYTHING_COUNTER = {
    'namespace': 'myNamespace',
    'name': 'myName',
    'step': 'myStep',
    'attempted': {
        'counter': 42
    },
    'committed': {
        'counter': 42
    },
    'labels': {
        'pcollection': 'myCollection',
        'myCustomKey': 'myCustomValue'
    }
}


def _create_metric_result(data_dict):
  step = data_dict['step'] if 'step' in data_dict else ''
  labels = data_dict['labels'] if 'labels' in data_dict else dict()
  values = {}
  for key in ['attempted', 'committed']:
    if key in data_dict:
      if 'counter' in data_dict[key]:
        values[key] = data_dict[key]['counter']
      elif 'distribution' in data_dict[key]:
        distribution = data_dict[key]['distribution']
        values[key] = DistributionResult(DistributionData(
            distribution['sum'],
            distribution['count'],
            distribution['min'],
            distribution['max'],
        ))
  attempted = values['attempted'] if 'attempted' in values else None
  committed = values['committed'] if 'committed' in values else None

  metric_name = MetricName(data_dict['namespace'], data_dict['name'])
  metric_key = MetricKey(step, metric_name, labels)
  return MetricResult(metric_key, committed, attempted)


class MetricResultMatchersTest(unittest.TestCase):

  def test_matches_all_for_counter(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = all_of(
        metric_result_matchers.has_namespace('myNamespace'),
        metric_result_matchers.has_name('myName'),
        metric_result_matchers.has_step_name('myStep'),
        metric_result_matchers.has_labels({
            'pcollection': 'myCollection',
            'myCustomKey': 'myCustomValue'
        }),
        metric_result_matchers.is_committed_counter(42),
        metric_result_matchers.is_attempted_counter(42)
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_none_for_counter(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = all_of(
        is_not(metric_result_matchers.has_namespace('invalidNamespace')),
        is_not(metric_result_matchers.has_name('invalidName')),
        is_not(metric_result_matchers.has_step_name('invalidStep')),
        is_not(metric_result_matchers.has_labels({
            'invalidPcollection': 'invalidCollection',
            'invalidCustomKey': 'invalidCustomValue'
        })),
        is_not(metric_result_matchers.is_committed_counter(1000)),
        is_not(metric_result_matchers.is_attempted_counter(1000))
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_all_for_distributionr(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = all_of(
        metric_result_matchers.has_namespace('myNamespace'),
        metric_result_matchers.has_name('myName'),
        metric_result_matchers.has_step_name('myStep'),
        metric_result_matchers.has_labels({
            'pcollection': 'myCollection',
            'myCustomKey': 'myCustomValue'
        }),
        metric_result_matchers.is_committed_distribution(12, 5, 0, 6),
        metric_result_matchers.is_attempted_distribution(12, 5, 0, 6)
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_none_for_distributionr(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = all_of(
        is_not(metric_result_matchers.has_namespace('invalidNamespace')),
        is_not(metric_result_matchers.has_name('invalidName')),
        is_not(metric_result_matchers.has_step_name('invalidStep')),
        is_not(metric_result_matchers.has_labels({
            'invalidPcollection': 'invalidCollection',
            'invalidCustomKey': 'invalidCustomValue'
        })),
        is_not(metric_result_matchers.is_committed_distribution(
            120, 50, 100, 60)),
        is_not(metric_result_matchers.is_attempted_distribution(
            120, 50, 100, 60))
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_key_but_not_value(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = is_not(metric_result_matchers.has_labels({
        'pcollection': 'invalidCollection'
    }))
    hc_assert_that(metric_result, matcher)

  def test_matches_counter_with_custom_matchers(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = all_of(
        metric_result_matchers.has_namespace(
            equal_to_ignoring_case('MYNAMESPACE')),
        metric_result_matchers.has_name(
            equal_to_ignoring_case('MYNAME')),
        metric_result_matchers.has_step_name(
            equal_to_ignoring_case('MYSTEP')),
        metric_result_matchers.has_labels({
            equal_to_ignoring_case('PCOLLECTION'): equal_to_ignoring_case(
                'MYCOLLECTION'),
            'myCustomKey': equal_to_ignoring_case('MYCUSTOMVALUE')
        }),
        metric_result_matchers.is_committed_counter(greater_than(0)),
        metric_result_matchers.is_attempted_counter(greater_than(0))
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_distribution_with_custom_matchers(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = all_of(
        metric_result_matchers.has_namespace(
            equal_to_ignoring_case('MYNAMESPACE')),
        metric_result_matchers.has_name(
            equal_to_ignoring_case('MYNAME')),
        metric_result_matchers.has_step_name(
            equal_to_ignoring_case('MYSTEP')),
        metric_result_matchers.has_labels({
            equal_to_ignoring_case('PCOLLECTION'): equal_to_ignoring_case(
                'MYCOLLECTION'),
            'myCustomKey': equal_to_ignoring_case('MYCUSTOMVALUE')
        }),
        metric_result_matchers.is_committed_distribution(
            greater_than(0), greater_than(0),
            greater_than(-1), greater_than(0)),
        metric_result_matchers.is_attempted_distribution(
            greater_than(0), greater_than(0),
            greater_than(-1), greater_than(0)),
    )
    hc_assert_that(metric_result, matcher)

if __name__ == '__main__':
  unittest.main()
