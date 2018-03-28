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

import unittest

from apache_beam.transforms.cy_combiners import DistributionAccumulator
from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterFactory
from apache_beam.utils.counters import CounterName


class DataflowElementExecutionTrackerTest(unittest.TestCase):
  class _FakeScopedState(object):
    def __init__(self, sampler, name, state_index, counter=None):
      self.sampler = sampler
      self.name = name
      self.state_index = state_index
      self.counter = counter

  def setUp(self):
    self.counter_factory = CounterFactory()
    try:
      from apache_beam.runners.worker.dataflow_element_execution_tracker \
        import DataflowElementExecutionTracker
      self.tracker = DataflowElementExecutionTracker()
    except ImportError:
      self.tracker = None

  def _create_state(self, operation_name):
    return self._FakeScopedState(None,
                                 CounterName(operation_name,
                                             step_name=operation_name),
                                 0)

  def _get_counter_value(self, op_name):
    counter_name = CounterName('per-element-processing-time', step_name=op_name)
    return self.counter_factory.get_counter(counter_name,
                                            Counter.DISTRIBUTION).value()

  def test_typical_usage(self):
    """Typical usage scenario
    Format info: execution_journal[] | partial timings not yet reported{}
    """
    if self.tracker is None:
      return
    state_a = 'A'
    state_b = 'B'
    state_c = 'C'
    state_d = 'D'
    self.tracker.enter_for_test(state_a)  # IDLE A1 | {}
    self.tracker.enter_for_test(state_b)  # IDLE A1 B1 | {}
    self.tracker.exit_for_test()  # IDLE A1 B1 A1 | {}
    self.tracker.take_sample_for_test(40*1000000)
    self.tracker.report_counter(self.counter_factory)  # A1 | {A1:2}
    self._assert_distribution_equals(self._get_counter_value(state_b),
                                     self._get_expected_distribution([10]))

    self.tracker.enter_for_test(state_b)  # A1 B2 | {A1:2}
    self.tracker.exit_for_test()  # A1 B2 A1 | {A1:2}
    self.tracker.enter_for_test(state_c)  # A1 B2 A1 C1 | {A1:2}
    self.tracker.enter_for_test(state_d)  # A1 B2 A1 C1 D1 | {A1:2}
    self.tracker.take_sample_for_test(50*1000000)
    self.tracker.report_counter(self.counter_factory) # D1 | {A1:4 C1:1 D1:1}
    self._assert_distribution_equals(self._get_counter_value(state_b),
                                     self._get_expected_distribution([10, 10]))

    self.tracker.exit_for_test()  # D1 C1 | {A1:4 C1:1 D1:1}
    self.tracker.exit_for_test()  # D1 C1 A1 | {A1:4 C1:1 D1:1}
    self.tracker.enter_for_test(state_c)  # D1 C1 A1 C2 | {A1:4 C1:1 D1:1}
    self.tracker.take_sample_for_test(40*1000000)
    self.tracker.report_counter(self.counter_factory)  # C2 | {A1:5 C2:1}
    self._assert_distribution_equals(self._get_counter_value(state_c),
                                     self._get_expected_distribution([20]))
    self._assert_distribution_equals(self._get_counter_value(state_d),
                                     self._get_expected_distribution([20]))

    self.tracker.exit_for_test()  # C2 A1 | {A1:5 C2:1
    self.tracker.exit_for_test()  # C2 A1 IDLE | {A1:5 C2:1}

    self.tracker.take_sample_for_test(30*1000000)
    self.tracker.report_counter(self.counter_factory)  # All reported
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([60]))
    self._assert_distribution_equals(self._get_counter_value(state_b),
                                     self._get_expected_distribution([10, 10]))
    self._assert_distribution_equals(self._get_counter_value(state_c),
                                     self._get_expected_distribution([20, 20]))
    self._assert_distribution_equals(self._get_counter_value(state_d),
                                     self._get_expected_distribution([20]))

  def test_counter_reported_on_close(self):
    if self.tracker is None:
      return
    state_a = 'A'
    self.tracker.enter_for_test(state_a)
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([]))

    self.tracker.exit_for_test()
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([10]))

  def test_distributed_sampled_time(self):
    if self.tracker is None:
      return
    state_a = 'A'
    state_b = 'B'
    self.tracker.enter_for_test(state_a)
    self.tracker.exit_for_test()
    self.tracker.enter_for_test(state_b)
    self.tracker.exit_for_test()
    self.tracker.take_sample_for_test(50*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([10]))
    self._assert_distribution_equals(self._get_counter_value(state_b),
                                     self._get_expected_distribution([10]))

  def test_element_tracked_individually_for_state(self):
    if self.tracker is None:
      return
    state_a = 'A'
    state_b = 'B'
    self.tracker.enter_for_test(state_a)
    self.tracker.enter_for_test(state_b)
    self.tracker.exit_for_test()
    self.tracker.enter_for_test(state_b)
    self.tracker.exit_for_test()
    self.tracker.exit_for_test()
    self.tracker.take_sample_for_test(70*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([30]))
    self._assert_distribution_equals(self._get_counter_value(state_b),
                                     self._get_expected_distribution([10, 10]))

  def test_current_operation_counted_in_next_sample(self):
    if self.tracker is None:
      return
    state_a = 'A'
    self.tracker.enter_for_test(state_a)
    self.tracker.take_sample_for_test(20*1000000)
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.exit_for_test()
    self.tracker.take_sample_for_test(20*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([30]))

  def test_no_execution_since_last_sample(self):
    if self.tracker is None:
      return
    self.tracker.take_sample_for_test(10*1000000)
    state_a = 'A'
    self.tracker.enter_for_test(state_a)
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.exit_for_test()
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([20]))
    self.tracker.take_sample_for_test(10*1000000)
    self.tracker.report_counter(self.counter_factory)
    self._assert_distribution_equals(self._get_counter_value(state_a),
                                     self._get_expected_distribution([20]))

  def _get_expected_distribution(self, values):
    distribution = DistributionAccumulator()
    for value in values:
      distribution.add_input(value)
    return distribution

  def _assert_distribution_equals(self, counter, expected_distribution):
    self.assertEquals(counter.min, expected_distribution.min)
    self.assertEquals(counter.max, expected_distribution.max)
    self.assertEquals(counter.count, expected_distribution.count)
    self.assertEquals(counter.sum, expected_distribution.sum)
    self.assertEquals(counter.buckets, expected_distribution.buckets)
    self.assertEquals(counter.first_bucket_offset,
                      expected_distribution.first_bucket_offset)


if __name__ == '__main__':
  unittest.main()
