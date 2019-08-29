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

"""TODO mxm document this"""
from __future__ import absolute_import

import collections
from threading import Lock


class StateCache(object):
  """ Cache for Beam state, scoped by state key.

  For a given state_key, caches a (cache_token, value) tuple and allows to
  read the cache if a valid cache token is supplied.
  """

  def __init__(self, max_entries):
    self._cache = self.LRUCache(max_entries)
    self._lock = Lock()

  def get(self, state_key, cache_token):
    with self._lock:
      cache_entry = self._cache.get(state_key)
    if cache_entry:
      token, value = cache_entry
      return value if token == cache_token else None
    else:
      return None

  def put(self, state_key, cache_token, value):
    with self._lock:
      return self._cache.put(state_key, (cache_token, value))

  def clear(self, state_key):
    with self._lock:
      self._cache.clear(state_key)

  def clear_all(self):
    with self._lock:
      self._cache.clear_all()

  def __len__(self):
    return len(self._cache)

  class LRUCache(object):

    def __init__(self, max_entries):
      self._maxEntries = max_entries
      self._cache = collections.OrderedDict()

    def get(self, key):
      value = self._cache.pop(key, None)
      if value:
        self._cache[key] = value
      return value

    def put(self, key, value):
      self._cache[key] = value
      while len(self._cache) > self._maxEntries:
        self._cache.popitem(last=False)

    def clear(self, key):
      self._cache.pop(key, None)

    def clear_all(self):
      self._cache.clear()

    def __len__(self):
      return len(self._cache)
