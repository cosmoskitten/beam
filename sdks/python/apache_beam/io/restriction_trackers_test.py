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

"""Unit tests for the range_trackers module."""
import unittest

from apache_beam.io.restriction_trackers import OffsetRange


class OffsetRangeTest(unittest.TestCase):

  def test_create(self):
    OffsetRange(0, 10)
    OffsetRange(10, 100)

    with self.assertRaises(ValueError):
      OffsetRange(10, 9)

  def test_split_respects_desired_num_splits(self):
    range = OffsetRange(10, 100)
    splits = list(range.split(desired_num_offsets_per_split=25))
    self.assertEqual(4, len(splits))
    self.assertIn(OffsetRange(10, 35), splits)
    self.assertIn(OffsetRange(35, 60), splits)
    self.assertIn(OffsetRange(60, 85), splits)
    self.assertIn(OffsetRange(85, 100), splits)

  def test_split_respects_min_num_splits(self):
    range = OffsetRange(10, 100)
    splits = list(range.split(desired_num_offsets_per_split=5,
                              min_num_offsets_per_split=25))
    self.assertEqual(3, len(splits))
    self.assertIn(OffsetRange(10, 35), splits)
    self.assertIn(OffsetRange(35, 60), splits)
    self.assertIn(OffsetRange(60, 100), splits)

  def test_split_no_small_split_at_end(self):
    range = OffsetRange(10, 90)
    splits = list(range.split(desired_num_offsets_per_split=25))
    self.assertEqual(3, len(splits))
    self.assertIn(OffsetRange(10, 35), splits)
    self.assertIn(OffsetRange(35, 60), splits)
    self.assertIn(OffsetRange(60, 90), splits)
