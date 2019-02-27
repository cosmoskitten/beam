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

"""Unittest for GCP Bigtable Split testing."""
from __future__ import absolute_import, division

import datetime
import unittest
import logging
import uuid

from apache_beam.io.gcp.bigtableio import _BigTableSource as BigTableSource

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.bigtable import row
  from google.cloud.bigtable import column_family
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.row_set import RowRange
except ImportError:
  Client = None


def write_data(project_id, instance_id, table_id, num_of_rows,
               cluster_id, location_id, client_obj=None):
  from google.cloud.bigtable import enums
  STORAGE_TYPE = enums.StorageType.HDD
  INSTANCE_TYPE = enums.Instance.Type.DEVELOPMENT

  if client_obj is None:
    client = Client(project=project_id, admin=True)
  else:
    client = client_obj

  instance = client.instance(instance_id,
                             instance_type=INSTANCE_TYPE)

  if not instance.exists():
    cluster = instance.cluster(cluster_id,
                               location_id,
                               default_storage_type=STORAGE_TYPE)
    instance.create(clusters=[cluster])
  table = instance.table(table_id)

  if not table.exists():
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'cf1'
    column_families = {column_family_id: max_versions_rule}
    table.create(column_families=column_families)

    mutation_batcher = table.mutations_batcher()
    for i in range(num_of_rows):
      key = "beam_key%s" % i
      row_element = row.DirectRow(row_key=key)
      row_element.set_cell(
          'cf1',
          ('field%s' % i).encode('utf-8'),
          'abc',
          datetime.datetime.now())

      mutation_batcher.mutate(row_element)
    mutation_batcher.flush()


@unittest.skipIf(Client is None, 'GCP dependencies are not installed')
class BigtableSourceTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    DEFAULT_TABLE_PREFIX = "pythonreadtest"

    cls.project_id = 'project_id'
    cls.instance_id = 'instance_id'
    cls.table_id = DEFAULT_TABLE_PREFIX + str(uuid.uuid4())[:8]

    cls.client = Client(project=cls.project_id, admin=True)
    cls.instance = cls.client.instance(cls.instance_id)
    cls.table = cls.instance.table(cls.table_id)

    cluster_id = 'cluster_id'
    location_id = 'us-central1-a'

    num_of_rows = 10000000
    write_data(cls.project_id, cls.instance_id, cls.table_id,
               num_of_rows, cluster_id, location_id, cls.client)

  def setUp(self):
    if not hasattr(self, 'bigtable'):
      self.bigtable = BigTableSource(BigtableSourceTest.project_id,
                                     BigtableSourceTest.instance_id,
                                     BigtableSourceTest.table_id)

  def _bigtable(self):
    return BigtableSourceTest.bigtable

  def test_estimate_size(self):
    get_size = [k.offset_bytes for k in
                BigtableSourceTest.table.sample_row_keys()][-1]
    size = self.bigtable.estimate_size()

    assert get_size == size

  # Split Range Size
  def _split_range_size(self, desired_bundle_size, range_):
    sample_row_keys = self.bigtable.get_sample_row_keys()
    split_range_ = self.bigtable.split_range_size(desired_bundle_size,
                                                  sample_row_keys,
                                                  range_)
    return len(list(split_range_))

  def test_one_bundle_split_range_size(self):
    desired_bundle_size = 805306368
    range_ = RowRange(b'beam_key0672496', b'beam_key1057014')

    assert self._split_range_size(desired_bundle_size, range_) == 1

  def test_split_range_size_two(self):
    desired_bundle_size = 402653184
    range_ = RowRange(b'beam_key0672496', b'beam_key1057014')

    assert self._split_range_size(desired_bundle_size, range_) == 2

  def test_split_range_size_four(self):
    desired_bundle_size = 201326592
    range_ = RowRange(b'beam_key0672496', b'beam_key1057014')

    assert self._split_range_size(desired_bundle_size, range_) == 4

  # Range Split Fraction
  def test_range_split_fraction(self):
    current_size = 805306368
    desired_bundle_size = 402653184
    start_key = b'beam_key0672496'
    end_key = b'beam_key1057014'
    count_all = len(list(self.bigtable.range_split_fraction(current_size,
                                                            desired_bundle_size,
                                                            start_key,
                                                            end_key)))
    desired_bundle_count = current_size/desired_bundle_size

    assert count_all == desired_bundle_count

  # Split Range Sized Subranges
  def test_split_rage_sized_subranges(self):
    sample_size_bytes = 805306368
    desired_bundle_size = 402653184
    start_key = b'beam_key0672496'
    end_key = b'beam_key1057014'
    ranges = self.bigtable.get_range_tracker(start_key, end_key)
    range_subranges = self.bigtable.split_range_sized_subranges(
        sample_size_bytes,
        desired_bundle_size,
        ranges)
    count_all = len(list(range_subranges))
    desired_bundle_count = sample_size_bytes/desired_bundle_size

    assert count_all == desired_bundle_count

  # Range Tracker
  def test_get_range_tracker(self):
    start_position = b'beam_key0672496'
    stop_position = b'beam_key1057014'
    range_tracker = self.bigtable.get_range_tracker(
        start_position,
        stop_position)

    assert range_tracker.start_position() == start_position
    assert range_tracker.stop_position() == stop_position

  # Split
  def test_split_four_hundred_bytes(self):
    desired_bundle_size = 402653184
    count = len(list(self.bigtable.split(desired_bundle_size)))
    size = int(self.bigtable.estimate_size())
    count_size = size/desired_bundle_size
    assert count == count_size

  def test_split_two_hundred_bytes(self):
    desired_bundle_size = 201326592
    count = len(list(self.bigtable.split(desired_bundle_size)))
    size = int(self.bigtable.estimate_size())
    count_size = size/desired_bundle_size
    assert count == count_size

  def test_split_eight_hundred_bytes(self):
    desired_bundle_size = 805306368
    count = len(list(self.bigtable.split(desired_bundle_size)))
    size = int(self.bigtable.estimate_size())
    count_size = size/desired_bundle_size
    assert count == count_size

  def test_read(self):
    start_position = b'beam_key0000000'
    stop_position = b'beam_key0672496'
    range_tracker = self.bigtable.get_range_tracker(
        start_position,
        stop_position)
    count = len([row.row_key for row in self.bigtable.read(range_tracker)])

    assert count == 672496

  def test_full_read(self):
    start_position = b''
    stop_position = b''
    range_tracker = self.bigtable.get_range_tracker(
        start_position,
        stop_position)
    count = len([row.row_key for row in self.bigtable.read(range_tracker)])

    assert count == 10000000


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
