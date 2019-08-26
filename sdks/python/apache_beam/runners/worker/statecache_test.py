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

"""Tests for state caching.

TODO mxm add more tests"""
import logging
import unittest

from apache_beam.runners.worker.statecache import StateCache


class StateCacheTest(unittest.TestCase):

  def test_cache_get_put_clear(self):
    cache = StateCache(5)
    self.assertEqual(cache.get("key", ['cache_token']), None)
    self.assertEqual(cache.get("key", []), None)
    self.assertEqual(cache.get("key", None), None)
    cache.put("key", "cache_token", "value")
    self.assertEqual(len(cache), 1)
    self.assertEqual(cache.get("key", ["cache_token"]), "value")
    self.assertEqual(cache.get("key", []), None)
    self.assertEqual(cache.get("key", None), None)
    cache.put("key", "cache_token2", "value2")
    self.assertEqual(len(cache), 1)
    self.assertEqual(cache.get("key", ["cache_token2"]), "value2")
    cache.put("key2", "cache_token", "value3")
    self.assertEqual(len(cache), 2)
    self.assertEqual(cache.get("key2", ["cache_token"]), "value3")
    cache.put("key3", "cache_token", "value4")
    cache.put("key4", "cache_token2", "value5")
    cache.put("key5", "cache_token", "value6")
    self.assertEqual(len(cache), 5)
    cache.put("key6", "cache_token2", "value7")
    self.assertEqual(len(cache), 5)
    cache.clear("key6")
    self.assertEqual(len(cache), 4)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
