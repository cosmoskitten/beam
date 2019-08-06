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

import logging
import pickle
import sys
import tempfile
import unittest

import dill
import numpy as np
from parameterized import parameterized

from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.interactive.caching import datatype_inference
from apache_beam.runners.interactive.caching import filebasedcache
from apache_beam.runners.interactive.caching import filebasedcache_test
from apache_beam.runners.interactive.caching import pcollectioncache_it_test
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that


def validate_directly(cache, expected):
  test = TestCase("__init__")
  actual = list(cache.read())
  test.assertArrayCountEqual(actual, expected)


def validate_through_pipeline(cache, expected):

  def equal_to_expected(actual):
    test = TestCase("__init__")
    test.assertArrayCountEqual(actual, expected)

  p = TestPipeline()
  pcoll = p | "Read" >> cache.reader()
  assert_that(pcoll, equal_to_expected)
  p.run()


class TestCase(ExtraAssertionsMixin, unittest.TestCase):
  pass


class SerializationTestBase(pcollectioncache_it_test.SerializationTestBase):

  # Attributes to be set by child classes.
  cache_class = None

  def setUp(self):
    self._temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self._temp_dir, self.cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self._temp_dir])

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  def test_serde_empty(self, _, serializer):
    self.check_serde_empty(filebasedcache_test.write_directly,
                           filebasedcache_test.read_directly, serializer)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  def test_serde_filled(self, _, serializer):
    self.check_serde_filled(filebasedcache_test.write_directly,
                            filebasedcache_test.read_directly, serializer)


class TextBasedCacheSerializationTest(SerializationTestBase, TestCase):

  cache_class = filebasedcache.TextBasedCache


class SafeTextBasedCacheSerializationTest(SerializationTestBase, TestCase):

  cache_class = filebasedcache.SafeTextBasedCache


class TFRecordBasedCacheSerializationTest(SerializationTestBase, TestCase):

  cache_class = filebasedcache.TFRecordBasedCache


class AvroBasedCacheSerializationTest(SerializationTestBase, TestCase):

  cache_class = filebasedcache.AvroBasedCache

  def get_writer_kwargs(self, data=None):
    if sys.version_info > (3,):
      self.skipTest("Only fastavro is supported on Python 3.")
    use_fastavro = False
    schema = datatype_inference.infer_avro_schema(
        data, use_fastavro=use_fastavro)
    return dict(schema=schema, use_fastavro=use_fastavro)


class FastAvroBasedCacheSerializationTest(SerializationTestBase, TestCase):

  cache_class = filebasedcache.AvroBasedCache

  def get_writer_kwargs(self, data=None):
    use_fastavro = True
    schema = datatype_inference.infer_avro_schema(
        data, use_fastavro=use_fastavro)
    return dict(schema=schema, use_fastavro=use_fastavro)


class ParquetBasedCacheSerializationTest(SerializationTestBase, TestCase):

  cache_class = filebasedcache.ParquetBasedCache

  def get_writer_kwargs(self, data=None):
    schema = datatype_inference.infer_pyarrow_schema(data)
    return dict(schema=schema)


class RoundtripTestBase(pcollectioncache_it_test.RoundtripTestBase):

  # Attributes to be set by child classes.
  cache_class = None
  dataset = None

  def get_writer_kwargs(self, data=None):
    return {}

  def setUp(self):
    self._temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self._temp_dir, self.cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self._temp_dir])

  @parameterized.expand([("{}-{}".format(
      write_fn.__name__,
      validate_fn.__name__,
  ), write_fn, validate_fn) for write_fn in [
      filebasedcache_test.write_directly,
      filebasedcache_test.write_through_pipeline
  ] for validate_fn in [validate_directly, validate_through_pipeline]])
  def test_roundtrip(self, _, write_fn, validate_fn):
    return self.check_roundtrip(write_fn, validate_fn, dataset=self.dataset)


class TextBasedCacheRoundtripTest(RoundtripTestBase, TestCase):

  cache_class = filebasedcache.TextBasedCache
  dataset = [
      data for data in pcollectioncache_it_test.GENERIC_TEST_DATA
      # One particular case where TextBasedCache crashes
      if data != [{"a": 10}]
      # Numpy arrays are not supported
      and not any(isinstance(e, np.ndarray) for e in data)
  ]


class SafeTextBasedCacheRoundtripTest(RoundtripTestBase, TestCase):

  cache_class = filebasedcache.SafeTextBasedCache
  dataset = pcollectioncache_it_test.GENERIC_TEST_DATA


class TFRecordBasedCacheRoundtripTest(RoundtripTestBase, TestCase):

  cache_class = filebasedcache.TFRecordBasedCache
  dataset = pcollectioncache_it_test.GENERIC_TEST_DATA


class AvroBasedCacheRoundtripBase(RoundtripTestBase):

  cache_class = filebasedcache.AvroBasedCache
  dataset = [
      data for data in pcollectioncache_it_test.DATAFRAME_TEST_DATA
      # Empty PCollections are not supported.
      if data
      # Rows with missing columns are not supported.
      and len({tuple(sorted(d.keys())) for d in data}) == 1
      # Array data are not supported.
      and not any((isinstance(v, np.ndarray) for v in data[0].values()))
  ]

  def get_writer_kwargs(self, data=None):
    if sys.version_info > (3,) and not self.use_fastavro:
      self.skipTest("Only fastavro is supported on Python 3.")
    writer_kwargs = {"use_fastavro": self.use_fastavro}
    if data is not None:
      writer_kwargs["schema"] = datatype_inference.infer_avro_schema(
          data, use_fastavro=self.use_fastavro)
    return writer_kwargs


class AvroBasedCacheRoundtripTest(AvroBasedCacheRoundtripBase, TestCase):

  use_fastavro = False


class FastAvroBasedCacheRoundtripTest(AvroBasedCacheRoundtripBase, TestCase):

  use_fastavro = True


class ParquetBasedCacheRoundtripTest(RoundtripTestBase, TestCase):

  cache_class = filebasedcache.ParquetBasedCache
  dataset = [
      data for data in pcollectioncache_it_test.DATAFRAME_TEST_DATA
      # Empty PCollections are not supported.
      if data
      # PCollections with no columns are not supported.
      and data[0]
      # Rows with missing columns are not supported.
      and len({tuple(sorted(d.keys())) for d in data}) == 1
  ]

  def get_writer_kwargs(self, data=None):
    writer_kwargs = {}
    if data is not None:
      writer_kwargs["schema"] = datatype_inference.infer_pyarrow_schema(data)
    return writer_kwargs


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
