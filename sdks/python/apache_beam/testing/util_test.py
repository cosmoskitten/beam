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

"""Unit tests for testing utilities."""

import unittest

from apache_beam import Create
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import WindowedValueMatcher
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import WindowInto
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import TimestampedValue


class UtilTest(unittest.TestCase):

  def test_assert_that_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))

  def test_assert_that_fails(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 10, 100]), equal_to([1, 2, 3]))

  def test_assert_that_fails_on_empty_input(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([]), equal_to([1, 2, 3]))

  def test_assert_that_fails_on_empty_expected(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 2, 3]), is_empty())

  def test_windowed_value_matcher_pass(self):
    class AddTimestampDoFn(DoFn):
      def process(self, element):
        yield TimestampedValue(element, element[1])

    with TestPipeline() as p:
      data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
      expected_window_values = [
          ((1, 1), 1.0, IntervalWindow(1.0, 3.0)),
          ((2, 1), 1.0, IntervalWindow(1.0, 3.0)),
          ((3, 1), 1.0, IntervalWindow(1.0, 3.0)),
          ((1, 2), 2.0, IntervalWindow(2.0, 4.0)),
          ((2, 2), 2.0, IntervalWindow(2.0, 4.0)),
          ((1, 4), 4.0, IntervalWindow(4.0, 6.0))]
      _ = (p
           | 'start' >> Create(data)
           | 'add_timestamps' >> ParDo(AddTimestampDoFn())
           | 'window' >> WindowInto(Sessions(gap_size=2))
           | 'assert_windowed_values' >> WindowedValueMatcher(
               expected_window_values))

  def test_windowed_value_matcher_unexpected_element(self):
    class AddTimestampDoFn(DoFn):
      def process(self, element):
        yield TimestampedValue(element, element[1])

    with self.assertRaisesRegexp(BeamAssertException, r'Unexpected.*(1, 4)'):
      with TestPipeline() as p:
        data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 4)]
        # This list is missing element (1, 4).
        expected_window_values = [
            ((1, 1), 1.0, IntervalWindow(1.0, 3.0)),
            ((2, 1), 1.0, IntervalWindow(1.0, 3.0)),
            ((3, 1), 1.0, IntervalWindow(1.0, 3.0)),
            ((1, 2), 2.0, IntervalWindow(2.0, 4.0)),
            ((2, 2), 2.0, IntervalWindow(2.0, 4.0))]
        _ = (p
             | 'start' >> Create(data)
             | 'add_timestamps' >> ParDo(AddTimestampDoFn())
             | 'window' >> WindowInto(Sessions(gap_size=2))
             | 'assert_windowed_values' >> WindowedValueMatcher(
                 expected_window_values))

  def test_windowed_value_matcher_extra_element(self):
    class AddTimestampDoFn(DoFn):
      def process(self, element):
        yield TimestampedValue(element, element[1])

    with self.assertRaisesRegexp(BeamAssertException,
                                 r'Failed assert.*\[6\] == \[5\]'):
      with TestPipeline() as p:
        # This list is missing element (1, 4).
        data = [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2)]
        expected_window_values = [
            ((1, 1), 1.0, IntervalWindow(1.0, 3.0)),
            ((2, 1), 1.0, IntervalWindow(1.0, 3.0)),
            ((3, 1), 1.0, IntervalWindow(1.0, 3.0)),
            ((1, 2), 2.0, IntervalWindow(2.0, 4.0)),
            ((2, 2), 2.0, IntervalWindow(2.0, 4.0)),
            ((1, 4), 4.0, IntervalWindow(4.0, 6.0))]
        _ = (p
             | 'start' >> Create(data)
             | 'add_timestamps' >> ParDo(AddTimestampDoFn())
             | 'window' >> WindowInto(Sessions(gap_size=2))
             | 'assert_windowed_values' >> WindowedValueMatcher(
                 expected_window_values))


if __name__ == '__main__':
  unittest.main()
