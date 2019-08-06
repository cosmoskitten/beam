# -*- coding: utf-8 -*-
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
import unittest

import numpy as np

from apache_beam import coders
from apache_beam.runners.interactive.caching import datatype_inference

GENERIC_TEST_DATA = [
    [],
    [None],
    [None, None, None],
    ["ABC", "DeF"],
    [u"ABC", u"±♠Ωℑ"],
    [b"abc123", b"aAaAa"],
    [1.5],
    [100, -123.456, 78.9],
    ["ABC", 1.2, 100, 0, -10, None, b"abc123"],
    [()],
    [("a", "b", "c")],
    [("a", 1, 1.2), ("b", 2, 5.5)],
    [("a", 1, 1.2), (2.5, "c", None)],
    [{}],
    [{"col1": "a", "col2": 1, "col3": 1.5}],
    [{"col1": "a", "col2": 1, "col3": 1.5},
     {"col1": "b", "col2": 2, "col3": 4.5}],
    [{"col1": "a", "col2": 1, "col3": u"±♠Ω"}, {4: 1, 5: 3.4, (6, 7): "a"}],
    [("a", "b", "c"), ["d", 1], {"col1": 1, 202: 1.234}, None, "abc", b"def",
     100, (1, 2, 3, "b")],
    [np.zeros((3, 6)), np.ones((10, 22))],
]

DATAFRAME_TEST_DATA = [
    [],
    [{}],
    [{}, {}, {}],
    [{"col1": "abc", "col2": "def"}, {"col1": "hello"}],
    [{"col1": "abc", "col2": "def"}, {"col1": "hello", "col2": "good bye"}],
    [{"col1": b"abc", "col2": "def"}, {"col1": b"hello", "col2": "good bye"}],
    [{"col1": u"abc", "col2": u"±♠Ω"}, {"col1": u"hello", "col2": u"Ωℑ"}],
    [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}],
    [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}],
    [{"x": np.array([1, 2])}, {"x": np.array([3, 4, 5])}],
]


class CoderTestBase(object):

  # Attributes to be set by child classes.
  cache_class = None
  location = None

  #: The default coder used by the cache. If None, the coder is inferred.
  default_coder = None

  def get_writer_kwargs(self, data=None):
    """Additional arguments to pass through to the writer."""
    return {}

  def check_coder(self, write_fn, data):
    inferred_coder = self.default_coder or coders.registry.get_coder(
        datatype_inference.infer_element_type(data))
    cache = self.cache_class(self.location, **self.get_writer_kwargs(data))
    cache.requires_coder = True
    self.assertEqual(cache.coder, self.default_coder)
    write_fn(cache, data)
    self.assertEqual(cache.coder, inferred_coder)
    cache.truncate()
    self.assertEqual(cache.coder, self.default_coder)


class SerializationTestBase(object):

  # Attributes to be set by child classes.
  cache_class = None
  location = None

  def get_writer_kwargs(self, data=None):
    return {}

  test_data = [{"a": 11, "b": "XXX"}, {"a": 20, "b": "YYY"}]

  def check_serde_empty(self, write_fn, read_fn, serializer):
    cache = self.cache_class(self.location,
                             **self.get_writer_kwargs(self.test_data))
    cache_out = serializer.loads(serializer.dumps(cache))
    write_fn(cache_out, self.test_data)
    data_out = list(read_fn(cache_out, limit=len(self.test_data)))
    self.assertEqual(data_out, self.test_data)

  def check_serde_filled(self, write_fn, read_fn, serializer):
    cache = self.cache_class(self.location,
                             **self.get_writer_kwargs(self.test_data))
    write_fn(cache, self.test_data)
    cache_out = serializer.loads(serializer.dumps(cache))
    data_out = list(read_fn(cache_out, limit=len(self.test_data)))
    self.assertEqual(data_out, self.test_data)


class RoundtripTestBase(object):

  # Attributes to be set by child classes.
  cache_class = None
  location = None

  def get_writer_kwargs(self, data=None):
    return {}

  def check_roundtrip(self, write_fn, validate_fn, dataset):
    """Make sure that data can be correctly written using the write_fn function
    and read using the validate_fn function.
    """
    cache = self.cache_class(self.location, **self.get_writer_kwargs(None))
    for data in dataset:
      cache._writer_kwargs.update(self.get_writer_kwargs(data))
      write_fn(cache, data)
      validate_fn(cache, data)
      write_fn(cache, data)
      validate_fn(cache, data * 2)
      cache.truncate()
      validate_fn(cache, [])
    cache.remove()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
