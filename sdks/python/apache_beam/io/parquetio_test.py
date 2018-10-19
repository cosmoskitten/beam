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
from __future__ import absolute_import

import sys
import os
import tempfile
import unittest

from apache_beam.io import filebasedsource
from apache_beam.io import source_test_utils
from apache_beam.io.parquetio import _create_parquet_source

import pyarrow as pa
import pyarrow.parquet as pq

class TestParquet(unittest.TestCase):
  _temp_files = []

  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    # Reducing the size of thread pools. Without this test execution may fail in
    # environments with limited amount of resources.
    filebasedsource.MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 2

  def tearDown(self):
    for path in self._temp_files:
      if os.path.exists(path):
        os.remove(path)
    self._temp_files = []

  RECORDS = [{'name': 'Thomas',
              'favorite_number': 1,
              'favorite_color': 'blue'}, {'name': 'Henry',
                                          'favorite_number': 3,
                                          'favorite_color': 'green'},
             {'name': 'Toby',
              'favorite_number': 7,
              'favorite_color': 'brown'}, {'name': 'Gordon',
                                           'favorite_number': 4,
                                           'favorite_color': 'blue'},
             {'name': 'Emily',
              'favorite_number': -1,
              'favorite_color': 'Red'}, {'name': 'Percy',
                                         'favorite_number': 6,
                                         'favorite_color': 'Green'}]

  SCHEMA = pa.schema([
      ('name', pa.string()),
      ('favorite_number', pa.int32()),
      ('favorite_color', pa.string())
  ])

  def _record_to_columns(self, records, schema):
    col_list = []
    for n in schema.names:
      column = []
      for r in self.RECORDS:
        column.append(r[n])
      col_list.append(column)
    return col_list

  def _write_data(self,
                  directory=None,
                  prefix=tempfile.template,
                  codec='none',
                  count=len(RECORDS)):

    with tempfile.NamedTemporaryFile(
        delete=False, dir=directory, prefix=prefix) as f:
      len_records = len(self.RECORDS)
      data = []
      for i in range(count):
        data.append(self.RECORDS[i % len_records])
      col_data = self._record_to_columns(data, self.SCHEMA)
      col_array = map(pa.array, col_data)
      table = pa.Table.from_arrays(col_array, self.SCHEMA.names)
      pq.write_table(table, f, compression=codec)

      self._temp_files.append(f.name)
      return f.name

  def _write_pattern(self, num_files):
    assert num_files > 0
    temp_dir = tempfile.mkdtemp()

    file_name = None
    for _ in range(num_files):
      file_name = self._write_data(directory=temp_dir, prefix='mytemp')

    assert file_name
    file_name_prefix = file_name[:file_name.rfind(os.path.sep)]
    return file_name_prefix + os.path.sep + 'mytemp*'

  def _run_parquet_test(self, pattern, expected_result):
    source = _create_parquet_source(pattern)
    read_records = source_test_utils.read_from_source(source, None, None)
    self.assertCountEqual(expected_result, read_records)

  def test_read(self):
    file_name = self._write_data()
    expected_result = self.RECORDS
    self._run_parquet_test(file_name, expected_result)
