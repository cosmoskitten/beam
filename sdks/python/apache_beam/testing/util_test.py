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
from apache_beam.testing.util import EqualToWindowedValue
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import WindowedValueMatcher
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import WindowInto
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import WindowedValue
from apache_beam.utils.timestamp import MIN_TIMESTAMP


class UtilTest(unittest.TestCase):

  def test_assert_that_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))

  def test_assert_that_passes_empty_equal_to(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), equal_to([]))

  def test_assert_that_passes_empty_is_empty(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), is_empty())

  def test_windowed_value_passes(self):
    expected = [TestWindowedValue(v, MIN_TIMESTAMP, [GlobalWindow()])
                for v in [1, 2, 3]]
    with TestPipeline() as p:
      assert_that(p | Create([2, 3, 1]), EqualToWindowedValue(expected))

  def test_assert_that_fails(self):
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([1, 10, 100]), equal_to([1, 2, 3]))

  def test_windowed_value_assert_fail_bad_value(self):
    expected = [TestWindowedValue(v + 1, MIN_TIMESTAMP, [GlobalWindow()])
                for v in [1, 2, 3]]
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([2, 3, 1]), EqualToWindowedValue(expected))

  def test_windowed_value_assert_fail_bad_timestamp(self):
    expected = [TestWindowedValue(v, 1, [GlobalWindow()])
                for v in [1, 2, 3]]
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([2, 3, 1]), EqualToWindowedValue(expected))

  def test_windowed_value_assert_fail_bad_window(self):
    expected = [TestWindowedValue(v, MIN_TIMESTAMP, [IntervalWindow(0, 1)])
                for v in [1, 2, 3]]
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([2, 3, 1]), EqualToWindowedValue(expected))

  def test_assert_that_fails_on_empty_input(self):
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([]), equal_to([1, 2, 3]))

  def test_assert_that_fails_on_empty_expected(self):
    with self.assertRaises(BeamAssertException):
      with TestPipeline() as p:
        assert_that(p | Create([1, 2, 3]), is_empty())

  def test_custom_matcher_plain_value_success(self):
    expected = [1, 2, 3]
    def matcher(actual):
      if actual != expected:
        raise BeamAssertException('custom matcher found mismatch: %r != %r' %
                                  (expected, actual))

    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), matcher)

  def test_custom_matcher_windowed_value_success(self):
    expected = [
        TestWindowedValue(v, MIN_TIMESTAMP,
                          [GlobalWindow()]) for v in [1, 2, 3]]

    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), CustomMatcher(expected))


class CustomMatcher(WindowedValueMatcher):
  def __init__(self, expected):
    self._expected = expected

  def match(self, actual):
    if actual != self._expected:
      raise BeamAssertException('custom matcher found mismatch: %r != %r' %
                                (self._expected, actual))


if __name__ == '__main__':
  unittest.main()
