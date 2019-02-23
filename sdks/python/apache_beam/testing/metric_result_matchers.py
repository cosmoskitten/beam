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
"""MetricResult matches for validating metrics in PipelineResults.

example usage:
    result = my_pipeline.run()
    all_metrics = result.metrics().all_metrics()

    matchers = [
        all_of(
            metric_result_matchers.has_name('ElementCount'),
            metric_result_matchers.has_labels({
                'original_name' : 'ToIsmRecordForMultimap-out0-ElementCount',
                'output_user_name' : 'ToIsmRecordForMultimap-out0'
            }),
            metric_result_matchers.is_committed_counter(42),
            metric_result_matchers.is_attempted_counter(42)
        ),
        all_of(
            metric_result_matchers.has_name('MeanByteCount'),
            metric_result_matchers.has_labels({
                'original_name' : 'Read-out0-MeanByteCount',
                'output_user_name' : 'GroupByKey/Read-out0'
            }),
            metric_result_matchers.is_committed_counter(31),
            metric_result_matchers.is_attempted_counter(31)
        ),
        all_of(
            metric_result_matchers.has_name('ExecutionTime_ProcessElement'),
            metric_result_matchers.has_step_name('write/Write/Write'),
            metric_result_matchers.is_committed_counter(1000),
            metric_result_matchers.is_attempted_counter(1000)
        ),
    ]
    errors = metric_result_matchers.verify_all(
        result.metrics().all_metrics(), matchers)
    self.assertFalse(errors, errors)
"""

from __future__ import absolute_import

from apache_beam.typehints import with_input_types
from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult

from hamcrest import equal_to
from hamcrest.core.matcher import Matcher
from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.core.allof import all_of
from hamcrest.core import string_description

import typing
import logging


def _matcher_or_equal_to(value_or_matcher):
  """Pass-thru for matchers, and wraps value inputs in an equal_to matcher."""
  if value_or_matcher is None:
    return None
  if isinstance(value_or_matcher, Matcher):
    return value_or_matcher
  return equal_to(value_or_matcher)


class MetricResultMatcher(BaseMatcher):
  """A PyHamcrest matcher that validates counter MetricResults."""

  @with_input_types(
      namespace=typing.Tuple[str, Matcher],
      name=typing.Tuple[str, Matcher],
      step=typing.Tuple[str, Matcher],
      label_key=typing.Tuple[str, Matcher],
      label_value=typing.Tuple[str, Matcher],
      attempted_value=typing.Tuple[int, Matcher],
      committed_value=typing.Tuple[int, DistributionResult, Matcher])
  def __init__(self, namespace=None, name=None, step=None, label_key=None,
      label_value=None, attempted_value=None, committed_value=None):
    self.namespace = _matcher_or_equal_to(namespace)
    self.name = _matcher_or_equal_to(name)
    self.step = _matcher_or_equal_to(step)
    self.label_key = _matcher_or_equal_to(label_key)
    self.label_value = _matcher_or_equal_to(label_value)
    self.attempted_value = _matcher_or_equal_to(attempted_value)
    self.committed_value = _matcher_or_equal_to(committed_value)

  def _matches(self, metric_result):
    if self.namespace is not None and not self.namespace.matches(
          metric_result.key.metric.namespace):
      return False
    if self.name and not self.name.matches(metric_result.key.metric.name):
      return False
    if self.step and not self.step.matches(metric_result.key.step):
      return False
    if (self.attempted_value is not None and
        not self.attempted_value.matches(metric_result.attempted)):
      return False
    if (self.committed_value is not None and
        not self.committed_value.matches(metric_result.committed)):
      return False
    if self.label_key:
      matched_keys = [key for key in metric_result.key.labels.keys() if
                     self.label_key.matches(key)]
      matched_key = matched_keys[0] if matched_keys else None
      if not matched_key:
        return False
      label_value = metric_result.key.labels[matched_key]
      if self.label_value and not self.label_value.matches(label_value):
        return False
    return True

  def describe_to(self, description):
    if self.namespace:
      description.append_text(" namespace: ")
      self.namespace.describe_to(description)
    if self.name:
      description.append_text(" name: ")
      self.name.describe_to(description)
    if self.step:
      description.append_text(" step: ")
      self.step.describe_to(description)
    if self.label_key:
      description.append_text(" label_key: ")
      self.label_key.describe_to(description)
    if self.label_value:
      description.append_text(" label_value: ")
      self.label_value.describe_to(description)
    if self.attempted_value is not None:
      description.append_text(" attempted_value: ")
      self.attempted_value.describe_to(description)
    if self.committed_value is not None:
      description.append_text(" committed_value: ")
      self.committed_value.describe_to(description)

  def describe_mismatch(self, metric_result, mismatch_description):
    mismatch_description.append_text("was").append_value(metric_result)


class DistributionMatcher(BaseMatcher):
  """A PyHamcrest matcher that validates counter distributions."""

  @with_input_types(
      sum_value=typing.Tuple[int, Matcher],
      count_value=typing.Tuple[int, Matcher],
      min_value=typing.Tuple[int, Matcher],
      max_value=typing.Tuple[int, Matcher])
  def __init__(self, sum_value, count_value, min_value, max_value):
    self.sum_value = _matcher_or_equal_to(sum_value)
    self.count_value = _matcher_or_equal_to(count_value)
    self.min_value = _matcher_or_equal_to(min_value)
    self.max_value = _matcher_or_equal_to(max_value)

  def _matches(self, distribution_result):
    if not isinstance(distribution_result, DistributionResult):
      return False
    if self.sum_value and not self.sum_value.matches(distribution_result.sum):
      return False
    if self.count_value and not self.count_value.matches(
          distribution_result.count):
      return False
    if self.min_value and not self.min_value.matches(distribution_result.min):
      return False
    if self.max_value and not self.max_value.matches(distribution_result.max):
      return False
    return True

  def describe_to(self, description):
    if self.sum_value:
      description.append_text(" sum_value: ")
      self.sum_value.describe_to(description)
    if self.count_value:
      description.append_text(" count_value: ")
      self.count_value.describe_to(description)
    if self.min_value:
      description.append_text(" min_value: ")
      self.min_value.describe_to(description)
    if self.max_value:
      description.append_text(" max_value: ")
      self.max_value.describe_to(description)

  def describe_mismatch(self, distribution_result, mismatch_description):
    mismatch_description.append_text("was").append_value(distribution_result)

@with_input_types(typing.Tuple[str, Matcher])
def has_step_name(step):
  return MetricResultMatcher(step=step)


@with_input_types(typing.Tuple[str, Matcher])
def has_name(name):
  return MetricResultMatcher(name=name)


@with_input_types(typing.Tuple[str, Matcher], typing.Tuple[str, Matcher])
def has_namespace(namespace):
  return MetricResultMatcher(namespace=namespace)


@with_input_types(typing.Mapping[
    typing.Tuple[str, Matcher], typing.Tuple[str, Matcher]
])
def has_labels(labels):
  label_matchers = []
  for (k,v) in labels.iteritems():
    label_matchers.append(MetricResultMatcher(label_key=k, label_value=v))
  return all_of(*label_matchers)


@with_input_types(
    sum_value=typing.Tuple[int, Matcher],
    count_value=typing.Tuple[int, Matcher],
    min_value=typing.Tuple[int, Matcher],
    max_value=typing.Tuple[int, Matcher])
def is_committed_distribution(sum_value, count_value, min_value, max_value):
  return MetricResultMatcher(committed_value=DistributionMatcher(
      sum_value, count_value, min_value, max_value))


@with_input_types(
    sum_value=typing.Tuple[int, Matcher],
    count_value=typing.Tuple[int, Matcher],
    min_value=typing.Tuple[int, Matcher],
    max_value=typing.Tuple[int, Matcher])
def is_attempted_distribution(sum_value, count_value, min_value, max_value):
  return MetricResultMatcher(attempted_value=DistributionMatcher(
      sum_value, count_value, min_value, max_value))


@with_input_types(typing.Tuple[int, Matcher])
def is_committed_counter(value):
  return MetricResultMatcher(committed_value=value)


@with_input_types(typing.Tuple[int, Matcher])
def is_attempted_counter(value):
  return MetricResultMatcher(attempted_value=value)


def verify_all(all_metrics, matchers):
  errors = []
  matched_metrics = []
  for matcher in matchers:
    matched_metrics = [mr for mr in all_metrics if matcher.matches(mr)]
  if not matched_metrics:
    errors.append('Unable to match metrics for matcher %s' % (
        string_description.tostring(matcher)))
  if errors:
    errors.append('\nActual MetricResults:\n' +
        '\n'.join([str(mr) for mr in all_metrics]))
  return ''.join(errors)
