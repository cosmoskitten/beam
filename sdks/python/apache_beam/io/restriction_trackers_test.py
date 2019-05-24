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

from __future__ import absolute_import

import logging
import unittest

from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.restriction_trackers import SDFBoundedSourceRestrictionTracker


class OffsetRangeTest(unittest.TestCase):

  def test_create(self):
    OffsetRange(0, 10)
    OffsetRange(10, 10)
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


class OffsetRestrictionTrackerTest(unittest.TestCase):

  def test_try_claim(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertEqual((100, 200), tracker.current_restriction())
    self.assertTrue(tracker.try_claim(100))
    self.assertTrue(tracker.try_claim(150))
    self.assertTrue(tracker.try_claim(199))
    self.assertFalse(tracker.try_claim(200))

  def test_checkpoint_unstarted(self):
    tracker = OffsetRestrictionTracker(100, 200)
    checkpoint = tracker.checkpoint()
    self.assertEqual((100, 100), tracker.current_restriction())
    self.assertEqual((100, 200), checkpoint)

  def test_checkpoint_just_started(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(100))
    checkpoint = tracker.checkpoint()
    self.assertEqual((100, 101), tracker.current_restriction())
    self.assertEqual((101, 200), checkpoint)

  def test_checkpoint_regular(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(105))
    self.assertTrue(tracker.try_claim(110))
    checkpoint = tracker.checkpoint()
    self.assertEqual((100, 111), tracker.current_restriction())
    self.assertEqual((111, 200), checkpoint)

  def test_checkpoint_claimed_last(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(105))
    self.assertTrue(tracker.try_claim(110))
    self.assertTrue(tracker.try_claim(199))
    checkpoint = tracker.checkpoint()
    self.assertEqual((100, 200), tracker.current_restriction())
    self.assertEqual((200, 200), checkpoint)

  def test_checkpoint_after_failed_claim(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(105))
    self.assertTrue(tracker.try_claim(110))
    self.assertTrue(tracker.try_claim(160))
    self.assertFalse(tracker.try_claim(240))

    checkpoint = tracker.checkpoint()
    self.assertTrue((100, 161), tracker.current_restriction())
    self.assertTrue((161, 200), checkpoint)

  def test_non_monotonic_claim(self):
    with self.assertRaises(ValueError):
      tracker = OffsetRestrictionTracker(100, 200)
      self.assertTrue(tracker.try_claim(105))
      self.assertTrue(tracker.try_claim(110))
      self.assertTrue(tracker.try_claim(103))

  def test_claim_before_starting_range(self):
    with self.assertRaises(ValueError):
      tracker = OffsetRestrictionTracker(100, 200)
      tracker.try_claim(90)

  def test_check_done_after_try_claim_past_end_of_range(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(150))
    self.assertTrue(tracker.try_claim(175))
    self.assertFalse(tracker.try_claim(220))
    tracker.check_done()

  def test_check_done_after_try_claim_past_end_of_range(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(150))
    self.assertTrue(tracker.try_claim(175))
    self.assertFalse(tracker.try_claim(200))
    tracker.check_done()

  def test_check_done_after_try_claim_right_before_end_of_range(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(150))
    self.assertTrue(tracker.try_claim(175))
    self.assertTrue(tracker.try_claim(199))
    tracker.check_done()

  def test_check_done_when_not_done(self):
    tracker = OffsetRestrictionTracker(100, 200)
    self.assertTrue(tracker.try_claim(150))
    self.assertTrue(tracker.try_claim(175))

    with self.assertRaises(ValueError):
      tracker.check_done()

class SDFBoundedSourceRestrictionTrackerTest(unittest.TestCase):
  def setUp(self):
    self.initial_start_pos = 0
    self.initial_stop_pos = 4
    self.range_tracker = OffsetRangeTracker(self.initial_start_pos,
                                            self.initial_stop_pos)
    self.sdf_restriction_tracker = SDFBoundedSourceRestrictionTracker(self.range_tracker)

  def test_split_at_remainder(self):
    fraction_of_remainder = 0.5
    expected_primary = (0, 3)
    expected_residual = (3, 4)
    self.sdf_restriction_tracker._delegate_range_tracker.try_claim(1)
    actual_primary, actual_residual = self.sdf_restriction_tracker.try_split(fraction_of_remainder)
    self.assertEqual(expected_primary, actual_primary)
    self.assertEqual(expected_residual, actual_residual)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
