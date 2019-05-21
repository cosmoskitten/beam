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

"""Unit tests for the SDFRestrictionProvider module."""

from __future__ import absolute_import

import unittest

from apache_beam.io.concat_source_test import RangeSource
from apache_beam.io.restriction_trackers import SDFBoundedSourceRestrictionTracker
from apache_beam.io.sdf_restriction_provider import SDFBoundedSourceRestrictionProvider


class SDFBoundedSourceRestrictionProviderTest(unittest.TestCase):
  def setUp(self):
    self.initial_start = 0
    self.initial_stop = 4
    self.initial_range_source = RangeSource(self.initial_start,
                                            self.initial_stop)
    self.sdf_restriction_provider = (
        SDFBoundedSourceRestrictionProvider(self.initial_range_source,
                                            desired_chunk_size=2))

  def test_initial_restriction(self):
    unused_element = None
    actual_start, actual_stop = (
        self.sdf_restriction_provider.initial_restriction(unused_element))
    self.assertEqual(self.initial_start, actual_start)
    self.assertEqual(self.initial_stop, actual_stop)

  def test_create_tracker(self):
    expected_start = 1
    expected_stop = 3
    restriction_tracker = self.sdf_restriction_provider.create_tracker(
        (expected_start, expected_stop))
    self.assertTrue(isinstance(restriction_tracker,
                               SDFBoundedSourceRestrictionTracker))
    self.assertEqual(expected_start, restriction_tracker.start_pos())
    self.assertEqual(expected_stop, restriction_tracker.stop_pos())

  def test_split(self):
    unused_element = None
    unused_restriction = None
    expect_splits = [(0, 2), (2, 4)]
    splits = self.sdf_restriction_provider.split(unused_element,
                                                 unused_restriction)
    self.assertEqual(expect_splits, list(splits))

  def test_restriction_size(self):
    unused_element = None
    unused_restriction = None
    split_1, split_2 = self.sdf_restriction_provider.split(unused_element,
                                                           unused_restriction)
    split_1_size = self.sdf_restriction_provider.restriction_size(
        unused_element, split_1)
    split_2_size = self.sdf_restriction_provider.restriction_size(
        unused_element, split_2)
    self.assertEqual(2, split_1_size)
    self.assertEqual(2, split_2_size)


if __name__ == '__main__':
  unittest.main()
