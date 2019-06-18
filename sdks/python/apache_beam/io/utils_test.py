import unittest

import mock as mock

from apache_beam.io import OffsetRangeTracker
from apache_beam.io import source_test_utils
from apache_beam.io.utils import CountingSource


class CountingSourceTest(unittest.TestCase):
  def setUp(self):
    self.source = CountingSource(10)

  def test_estimate_size(self):
    self.assertEqual(10, self.source.estimate_size())

  @mock.patch('apache_beam.io.utils.OffsetRangeTracker')
  def test_get_range_tracker(self, mock):
    _ = self.source.get_range_tracker(None, None)
    mock.called_with_args(0, 10)
    _ = self.source.get_range_tracker(3, 7)
    mock.called_with_args(3, 7)

  def test_read(self):
    tracker = OffsetRangeTracker(3, 6)
    res = self.source.read(tracker)
    self.assertItemsEqual([3, 4, 5], res)

  def test_split(self):
    for size in [1, 3, 10]:
      splits = list(self.source.split(desired_bundle_size=size))

      reference_info = (self.source, None, None)
      sources_info = ([(split.source, split.start_position, split.stop_position)
                       for split in splits])
      source_test_utils.assert_sources_equal_reference_source(
          reference_info, sources_info)

  def test_dynamic_work_rebalancing(self):
    splits = list(self.source.split(desired_bundle_size=20))
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
        splits[0].source, splits[0].start_position, splits[0].stop_position)


if __name__ == '__main__':
  unittest.main()
